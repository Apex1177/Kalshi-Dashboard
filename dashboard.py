import csv
import os
import time
import json
import base64
import asyncio
import logging
import math
from logging.handlers import RotatingFileHandler
from collections import deque
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from kalshi_python_async import Configuration, KalshiClient

# ─────────────────────────────────────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────────────────────────────────────
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, "logs")
cache_dir = os.path.join(script_dir, "cache")
os.makedirs(log_dir, exist_ok=True)
os.makedirs(cache_dir, exist_ok=True)

logger = logging.getLogger("dashboard")
logger.setLevel(logging.DEBUG)

# File handler - rotates at 5MB, keeps 3 backups
file_handler = RotatingFileHandler(
    os.path.join(log_dir, "dashboard.log"),
    maxBytes=5*1024*1024,
    backupCount=3
)
file_handler.setLevel(logging.WARNING)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s'
))
logger.addHandler(file_handler)

# Usage log - rate-limit and API usage (INFO), separate file
usage_logger = logging.getLogger("dashboard.usage")
usage_logger.setLevel(logging.INFO)
usage_handler = RotatingFileHandler(
    os.path.join(log_dir, "dashboard_usage.log"),
    maxBytes=2*1024*1024,
    backupCount=2
)
usage_handler.setLevel(logging.INFO)
usage_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
usage_logger.addHandler(usage_handler)
usage_logger.propagate = False

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
MAX_RETRIES = 5            # Connection failures before full reconnect
BASE_RETRY_DELAY = 2       # Initial retry delay in seconds (doubles each retry)
REFRESH_INTERVAL = 2       # Min seconds between fetch cycles (reduces CPU when caches warm)
READS_PER_SECOND_DEFAULT = 2.5   # Default max API reads per second (user-adjustable via GUI)
_read_interval = 1.0 / READS_PER_SECOND_DEFAULT  # Min seconds between API reads; use get/set_reads_per_second
BALANCE_REFRESH_INTERVAL = 10   # Balance API fetched at most this often (seconds)
MARKET_REFRESH_INTERVAL = 10    # Market data (24h volume) fetched at most this often
ORDERBOOK_CACHE_TTL = 10   # Orderbook cached per ticker for this many seconds
USAGE_LOG_INTERVAL = 60    # Log API usage (requests in last 60s) at most this often (seconds)
LIMITS_REFRESH_INTERVAL = 60   # Fetch /account/limits at most this often (seconds)

READS_PER_SECOND_MIN = 1.0   # App uses read-only API; min 1 read/s
READS_PER_SECOND_MAX = 20.0


def get_reads_per_second() -> float:
    """Return current max API reads per second (user-adjustable)."""
    return 1.0 / _read_interval


def set_reads_per_second(rps: float) -> bool:
    """Set max API reads per second. Returns True if valid and updated."""
    global _read_interval
    try:
        r = float(rps)
    except (TypeError, ValueError):
        return False
    if not (READS_PER_SECOND_MIN <= r <= READS_PER_SECOND_MAX):
        return False
    _read_interval = 1.0 / r
    return True


def clear_request_timestamps() -> None:
    """Clear request timestamps so usage logging reflects only post-clear traffic (e.g. after Apply)."""
    _request_timestamps.clear()


BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
API_PATH_PREFIX = "/trade-api/v2"
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

# Volume tracking periods (seconds)
VOLUME_PERIODS = {
    '1m': 60,
    '10m': 600,
    '1h': 3600,
    '6h': 21600,
    '12h': 43200,
}

# Volume display cache (1m–12h only; 24h always from API)
VOLUME_DISPLAY_CACHE_PATH = os.path.join(cache_dir, "volume_display_cache.json")
VOLUME_DISPLAY_CACHE_MAX_AGE = 12 * 3600  # seconds; reject cache older than this
VOLUME_DISPLAY_CACHE_SAVE_INTERVAL = 60   # seconds between periodic saves
_volume_display_cache = {"tickers": {}}
_volume_display_cache_loaded = False
_volume_cache_last_save_ts = 0.0

# ─────────────────────────────────────────────────────────────────────────────
# Initialization
# ─────────────────────────────────────────────────────────────────────────────
parent_dir = os.path.dirname(script_dir)

# Load environment variables from script directory
load_dotenv(os.path.join(script_dir, ".env"))
api_key_id = os.environ.get("KALSHI_READONLY_KEY")

# Load private key from parent directory
key_path = os.path.join(parent_dir, "Read_Only_Kalshi_Key.txt")
if not os.path.exists(key_path):
    raise FileNotFoundError("Private key file 'Read_Only_Kalshi_Key.txt' not found in parent directory")

with open(key_path, "r") as f:
    private_key_pem = f.read()

# Initialize SDK client
def _kalshi_config():
    c = Configuration(host=BASE_URL)
    c.api_key_id = api_key_id
    c.private_key_pem = private_key_pem
    return c


client = KalshiClient(_kalshi_config())

# Load private key object for direct API calls
private_key_obj = serialization.load_pem_private_key(private_key_pem.encode(), password=None)

# Cache for less-frequent API data (balance, market 24h volume)
_balance_cache = {"balance": 0, "ts": 0.0}
_market_info_cache = {}
_market_info_ts = 0.0
# Orderbook cache: ticker -> (raw orderbook result, timestamp)
_orderbook_cache = {}

# Rate-limit usage tracking
_request_timestamps = deque(maxlen=2000)
_last_usage_log_ts = 0.0
_limits_ts = 0.0


def _log_rate_limit_usage(method: str, path: str, resp: aiohttp.ClientResponse) -> None:
    """Log at DEBUG and rate-limit headers at INFO when present. Request is recorded in _read_then_wait."""
    logger.debug("API request %s %s", method, path)
    # Collect rate-limit–related headers (case-insensitive)
    rl = {}
    for k, v in resp.headers.items():
        if "rate" in k.lower() and "limit" in k.lower():
            rl[k] = v
    for name in (
        "X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset",
        "RateLimit-Limit", "RateLimit-Remaining", "RateLimit-Reset",
    ):
        v = resp.headers.get(name)
        if v is not None and name not in rl:
            rl[name] = v
    if rl:
        usage_logger.info(
            "rate_limit path=%s %s",
            path,
            " ".join(f"{k}={v}" for k, v in sorted(rl.items())),
        )


async def _tracked_sdk_request(name: str, coro):
    """Run SDK coroutine. Request is recorded in _read_then_wait."""
    logger.debug("API request SDK %s", name)
    return await coro


# ─────────────────────────────────────────────────────────────────────────────
# Volume Tracker
# ─────────────────────────────────────────────────────────────────────────────
class VolumeTracker:
    """Track trading volume per market over sliding time windows."""
    
    def __init__(self, max_age: int = 43200):
        self.max_age = max_age  # Keep trades for up to 12 hours
        self.trades = {}  # ticker -> deque of (timestamp, count)
        self.ws_connected = False
        self.subscribed_tickers = set()
    
    def add_trade(self, ticker: str, ts: int, count: int):
        """Record a trade for a market."""
        if ticker not in self.trades:
            self.trades[ticker] = deque()
        self.trades[ticker].append((ts, count))
        self._purge_old(ticker, ts)
    
    def _purge_old(self, ticker: str, current_ts: int):
        """Remove trades older than max_age."""
        if ticker not in self.trades:
            return
        cutoff = current_ts - self.max_age
        while self.trades[ticker] and self.trades[ticker][0][0] < cutoff:
            self.trades[ticker].popleft()
    
    def get_volume(self, ticker: str, seconds: int) -> int:
        """Get total contracts traded in last N seconds."""
        if ticker not in self.trades:
            return 0
        now = int(time.time())
        cutoff = now - seconds
        return sum(count for ts, count in self.trades[ticker] if ts >= cutoff)
    
    def get_volumes(self, ticker: str) -> dict:
        """Get volumes for all tracked periods."""
        return {
            period: self.get_volume(ticker, seconds)
            for period, seconds in VOLUME_PERIODS.items()
        }


# Global volume tracker instance
volume_tracker = VolumeTracker()


# ─────────────────────────────────────────────────────────────────────────────
# Volume Recorder (CSV Export)
# ─────────────────────────────────────────────────────────────────────────────
class VolumeRecorder:
    """Record volume data to CSV for tickers in volume_config (volume_yes, volume_no)."""
    
    INTERVAL_SECONDS = {
        '1s': 1,
        '1m': 60,
        '10m': 600,
        '1h': 3600,
    }
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.join(script_dir, "volume_config.json")
        self.output_dir = os.path.join(script_dir, "volume_data")
        self.ticker_intervals = {}
        self.current_periods = {}
        self._last_config_load = 0.0
        self._last_flush_ended = 0.0
        self._load_config()
    
    def _load_config(self):
        """Load volume_config.json: tickers with 'volume' intervals."""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                tickers_config = config.get('tickers', {})
                self.ticker_intervals = {}
                for ticker, raw in tickers_config.items():
                    if not isinstance(raw, dict):
                        continue
                    intervals = raw.get('volume', []) or []
                    valid = [i for i in intervals if i in self.INTERVAL_SECONDS]
                    if valid:
                        self.ticker_intervals[ticker] = valid
                        if ticker not in self.current_periods:
                            self.current_periods[ticker] = {}
                        for iv in valid:
                            if iv not in self.current_periods[ticker]:
                                self.current_periods[ticker][iv] = {}
                self._last_config_load = time.time()
        except (json.JSONDecodeError, IOError) as e:
            logger.warning("Failed to load volume config: %s", e)
    
    def get_config_tickers(self) -> list:
        """Tickers with volume recording enabled (for WebSocket subscription)."""
        if time.time() - self._last_config_load > 60:
            self._load_config()
        return list(self.ticker_intervals.keys())
    
    def _get_period_start(self, ts: int, interval: str) -> int:
        """Get the start timestamp for the period containing ts."""
        seconds = self.INTERVAL_SECONDS.get(interval, 1)
        return (ts // seconds) * seconds
    
    def _get_ticker_file(self, ticker: str) -> str:
        """Get the CSV file path for a ticker."""
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        return os.path.join(self.output_dir, f"{ticker}_volume_data.csv")
    
    def _write_csv_row(
        self,
        timestamp: int,
        ticker: str,
        interval: str,
        volume_yes: int,
        volume_no: int,
    ):
        """Write a single row to the ticker's CSV file. Timestamps are UTC."""
        if volume_yes == 0 and volume_no == 0:
            return
        output_file = self._get_ticker_file(ticker)
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        day_of_week = dt.strftime("%A")
        timestamp_str = dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
        file_exists = os.path.exists(output_file)
        try:
            with open(output_file, "a", newline="") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(
                        ["ticker", "timestamp", "day_of_week", "interval", "volume_yes", "volume_no"]
                    )
                w.writerow([ticker, timestamp_str, day_of_week, interval, volume_yes, volume_no])
                f.flush()
        except IOError as e:
            logger.warning("Failed to write CSV row for %s: %s", ticker, e)

    def record_trade(self, ticker: str, ts: int, count: int, side: str):
        """Record a trade only for tickers in volume_config. side is 'yes' or 'no'."""
        if not ticker or count <= 0:
            return
        if time.time() - self._last_config_load > 60:
            self._load_config()
        if ticker not in self.ticker_intervals:
            return
        side = (side or "").strip().lower()
        if side not in ("yes", "no"):
            return
        if ticker not in self.current_periods:
            self.current_periods[ticker] = {}
        current_time = int(time.time())
        for interval in self.ticker_intervals[ticker]:
            if interval not in self.current_periods[ticker]:
                self.current_periods[ticker][interval] = {}
            period_start = self._get_period_start(ts, interval)
            periods = self.current_periods[ticker][interval]
            old_periods = [p for p in periods if p < period_start]
            for old_period in old_periods:
                v = periods[old_period]
                self._write_csv_row(old_period, ticker, interval, v["yes"], v["no"])
                del periods[old_period]
            if period_start not in periods:
                periods[period_start] = {"yes": 0, "no": 0}
            periods[period_start][side] += count
            sec = self.INTERVAL_SECONDS.get(interval, 1)
            if sec >= 60 and (current_time - period_start) >= sec:
                if period_start in periods:
                    v = periods[period_start]
                    if v["yes"] > 0 or v["no"] > 0:
                        self._write_csv_row(period_start, ticker, interval, v["yes"], v["no"])
                    del periods[period_start]
    
    def flush_ended_periods(self):
        """Write any ended periods (call periodically so we record even without new trades)."""
        now = time.time()
        if now - self._last_flush_ended < 60:
            return
        self._last_flush_ended = now
        if now - self._last_config_load > 60:
            self._load_config()
        current_time = int(now)
        for ticker in list(self.ticker_intervals):
            if ticker not in self.current_periods:
                continue
            for interval in self.ticker_intervals[ticker]:
                if interval not in self.current_periods[ticker]:
                    continue
                periods = self.current_periods[ticker][interval]
                sec = self.INTERVAL_SECONDS.get(interval, 1)
                ended = [
                    p for p in periods
                    if (current_time - p) >= sec and (periods[p]["yes"] > 0 or periods[p]["no"] > 0)
                ]
                for p in ended:
                    v = periods[p]
                    self._write_csv_row(p, ticker, interval, v["yes"], v["no"])
                    del periods[p]

    def flush(self):
        """Write any remaining data (call on shutdown)."""
        for ticker in self.current_periods:
            for interval in self.current_periods[ticker]:
                for period_start, v in self.current_periods[ticker][interval].items():
                    if v["yes"] > 0 or v["no"] > 0:
                        self._write_csv_row(period_start, ticker, interval, v["yes"], v["no"])
        self.current_periods = {}


# Global volume recorder instance
volume_recorder = VolumeRecorder()


# ─────────────────────────────────────────────────────────────────────────────
# Orderbook Recorder (CSV Export)
# ─────────────────────────────────────────────────────────────────────────────
class OrderbookRecorder:
    """Record orderbook snapshots to CSV for analysis."""
    
    INTERVAL_SECONDS = {
        '1s': 1,
        '1m': 60,
        '10m': 600,
        '1h': 3600,
    }
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.join(script_dir, "volume_config.json")
        self.output_dir = os.path.join(script_dir, "orderbook_data")
        self.ticker_intervals = {}
        self.last_recorded = {}
        self.last_config_load = 0
        self.last_snapshot = {}
        self._load_config()

    def _load_config(self):
        """Load or reload configuration from JSON file.
        Format: {"tickers": {"TICKER": {"volume": ["1m", "10m"], "orderbook": ["10m"]}}}.
        """
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                tickers_config = config.get('tickers', {})
                self.ticker_intervals = {}
                for ticker, raw in tickers_config.items():
                    if not isinstance(raw, dict):
                        continue
                    orderbook_intervals = raw.get('orderbook', []) or []
                    valid = [i for i in orderbook_intervals if i in self.INTERVAL_SECONDS]
                    if valid:
                        self.ticker_intervals[ticker] = valid
                        if ticker not in self.last_recorded:
                            self.last_recorded[ticker] = {}
                        for interval in valid:
                            if interval not in self.last_recorded[ticker]:
                                self.last_recorded[ticker][interval] = 0
                self.last_config_load = time.time()
                kept = {}
                for t, ivs in self.ticker_intervals.items():
                    for i in ivs:
                        prev = self.last_snapshot.get(t, {}).get(i)
                        if prev is not None:
                            kept.setdefault(t, {})[i] = prev
                self.last_snapshot = kept
        except (json.JSONDecodeError, IOError) as e:
            logger.warning("Failed to load orderbook config: %s", e)
    
    def _get_ticker_file(self, ticker: str) -> str:
        """Get the CSV file path for a ticker."""
        os.makedirs(self.output_dir, exist_ok=True)
        return os.path.join(self.output_dir, f"{ticker}_orderbook_data.csv")

    def _build_snapshot(self, orderbook_json: dict, bbo: dict):
        """Build comparable snapshot (yes_bid, yes_ask, no_bid, spread, yes_depth, no_depth, yes_levels_str, no_levels_str)."""
        orderbook = orderbook_json.get("orderbook", {})
        yes_levels = orderbook.get("yes_dollars", [])
        no_levels = orderbook.get("no_dollars", [])
        yes_depth = sum(int(level[1]) for level in yes_levels if level and len(level) >= 2) if yes_levels else 0
        no_depth = sum(int(level[1]) for level in no_levels if level and len(level) >= 2) if no_levels else 0
        yes_bid = bbo.get("yes_bid", 0) if bbo else 0
        yes_ask = bbo.get("yes_ask", 0) if bbo else 0
        no_bid = bbo.get("no_bid", 0) if bbo else 0
        spread = bbo.get("spread", 0) if bbo else 0
        yes_levels_str = json.dumps(yes_levels[:5]) if yes_levels else "[]"
        no_levels_str = json.dumps(no_levels[:5]) if no_levels else "[]"
        return (yes_bid, yes_ask, no_bid, spread, yes_depth, no_depth, yes_levels_str, no_levels_str)

    def _write_orderbook_row(self, timestamp: int, ticker: str, snapshot: tuple):
        """Write orderbook snapshot to CSV. Timestamps are UTC. Uses csv.writer for safe quoting of JSON columns."""
        output_file = self._get_ticker_file(ticker)
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        day_of_week = dt.strftime("%A")
        timestamp_str = dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
        yes_bid, yes_ask, no_bid, spread, yes_depth, no_depth, yes_levels_str, no_levels_str = snapshot
        file_exists = os.path.exists(output_file)
        try:
            with open(output_file, "a", newline="") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(
                        [
                            "ticker",
                            "timestamp",
                            "day_of_week",
                            "yes_bid",
                            "yes_ask",
                            "no_bid",
                            "spread",
                            "yes_depth",
                            "no_depth",
                            "yes_levels",
                            "no_levels",
                        ]
                    )
                w.writerow(
                    [
                        ticker,
                        timestamp_str,
                        day_of_week,
                        yes_bid,
                        yes_ask,
                        no_bid,
                        spread,
                        yes_depth,
                        no_depth,
                        yes_levels_str,
                        no_levels_str,
                    ]
                )
                f.flush()
        except IOError as e:
            logger.warning("Failed to write orderbook CSV row for %s: %s", ticker, e)

    def should_record(self, ticker: str, interval: str, current_time: int) -> bool:
        """Check if we should record orderbook for this ticker/interval."""
        if ticker not in self.ticker_intervals:
            return False
        if interval not in self.ticker_intervals[ticker]:
            return False
        
        interval_seconds = self.INTERVAL_SECONDS.get(interval, 1)
        last_ts = self.last_recorded.get(ticker, {}).get(interval, 0)
        
        return (current_time - last_ts) >= interval_seconds
    
    def record_orderbook(self, ticker: str, orderbook_json: dict, bbo: dict):
        """Record orderbook snapshot only when interval has elapsed and the book has changed."""
        if time.time() - self.last_config_load > 60:
            self._load_config()
        if ticker not in self.ticker_intervals:
            return
        current_time = int(time.time())
        snapshot = self._build_snapshot(orderbook_json, bbo)
        for interval in self.ticker_intervals[ticker]:
            if not self.should_record(ticker, interval, current_time):
                continue
            last = self.last_snapshot.get(ticker, {}).get(interval)
            if last is not None and last == snapshot:
                continue
            self._write_orderbook_row(current_time, ticker, snapshot)
            if ticker not in self.last_recorded:
                self.last_recorded[ticker] = {}
            self.last_recorded[ticker][interval] = current_time
            if ticker not in self.last_snapshot:
                self.last_snapshot[ticker] = {}
            self.last_snapshot[ticker][interval] = snapshot


# Global orderbook recorder instance
orderbook_recorder = OrderbookRecorder()


# ─────────────────────────────────────────────────────────────────────────────
# API Helpers
# ─────────────────────────────────────────────────────────────────────────────
def sign_request(method: str, path: str) -> dict:
    """Generate authentication headers for direct API calls."""
    timestamp = str(int(time.time() * 1000))
    path_parts = path.split('?')
    full_path = API_PATH_PREFIX + path_parts[0]
    msg = timestamp + method + full_path
    
    signature = private_key_obj.sign(
        msg.encode('utf-8'),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    
    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode('utf-8'),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }


async def fetch_orderbook_direct(session: aiohttp.ClientSession, ticker: str) -> dict:
    """Fetch orderbook directly via HTTP."""
    path = f"/markets/{ticker}/orderbook?depth=5"
    headers = sign_request("GET", path)
    try:
        async with session.get(BASE_URL + path, headers=headers) as resp:
            _log_rate_limit_usage("GET", path, resp)
            if resp.status == 200:
                return await resp.json()
            return None
    except Exception as e:
        logger.warning("Failed to fetch orderbook for %s: %s", ticker, e)
        return None


async def fetch_queue_positions_direct(session: aiohttp.ClientSession, market_tickers: list) -> dict:
    """
    Fetch queue positions for all resting orders.
    Returns dict mapping order_id -> queue_position (contracts at same price ahead).
    """
    if not market_tickers:
        return {}
    tickers_param = ",".join(market_tickers)
    path = f"/portfolio/orders/queue_positions?market_tickers={tickers_param}"
    headers = sign_request("GET", path)
    try:
        async with session.get(BASE_URL + path, headers=headers) as resp:
            _log_rate_limit_usage("GET", path, resp)
            if resp.status == 200:
                data = await resp.json()
                queue_lookup = {}
                for qp in data.get("queue_positions", []):
                    order_id = qp.get("order_id")
                    queue_pos = qp.get("queue_position", 0)
                    if order_id:
                        queue_lookup[order_id] = queue_pos
                return queue_lookup
            return {}
    except Exception as e:
        logger.warning("Failed to fetch queue positions: %s", e)
        return {}


async def fetch_market_direct(session: aiohttp.ClientSession, ticker: str) -> dict:
    """Fetch market data directly via HTTP (includes volume_24h, last_price)."""
    path = f"/markets/{ticker}"
    headers = sign_request("GET", path)
    try:
        async with session.get(BASE_URL + path, headers=headers) as resp:
            _log_rate_limit_usage("GET", path, resp)
            if resp.status == 200:
                data = await resp.json()
                return data.get("market", {})
            return {}
    except Exception as e:
        logger.warning("Failed to fetch market data for %s: %s", ticker, e)
        return {}


async def fetch_resting_orders_direct(session: aiohttp.ClientSession) -> list:
    """Fetch resting orders directly via HTTP."""
    path = "/portfolio/orders?status=resting"
    headers = sign_request("GET", path)
    try:
        async with session.get(BASE_URL + path, headers=headers) as resp:
            _log_rate_limit_usage("GET", path, resp)
            if resp.status == 200:
                data = await resp.json()
                return data.get("orders", [])
            return []
    except Exception as e:
        logger.warning("Failed to fetch resting orders: %s", e)
        return []


async def fetch_account_limits_direct(session: aiohttp.ClientSession) -> dict:
    """Fetch account API tier limits (GET /account/limits). Returns {usage_tier, read_limit, write_limit} or {}."""
    path = "/account/limits"
    headers = sign_request("GET", path)
    try:
        async with session.get(BASE_URL + path, headers=headers) as resp:
            _log_rate_limit_usage("GET", path, resp)
            if resp.status != 200:
                return {}
            data = await resp.json()
            out = {
                "usage_tier": data.get("usage_tier", ""),
                "read_limit": data.get("read_limit"),
                "write_limit": data.get("write_limit"),
            }
            if out["usage_tier"] or out["read_limit"] is not None or out["write_limit"] is not None:
                usage_logger.info(
                    "API limits tier=%s read_limit=%s write_limit=%s",
                    out["usage_tier"], out["read_limit"], out["write_limit"],
                )
            return out
    except Exception as e:
        logger.warning("Failed to fetch account limits: %s", e)
        return {}


def _log_api_usage_summary(now: float) -> None:
    """Log request counts in last 1s and last 60s if interval elapsed."""
    global _last_usage_log_ts
    if now - _last_usage_log_ts < USAGE_LOG_INTERVAL:
        return
    _last_usage_log_ts = now
    cutoff_1s = now - 1.0
    cutoff_60s = now - 60.0
    n_1s = sum(1 for t in _request_timestamps if t >= cutoff_1s)
    n_60s = sum(1 for t in _request_timestamps if t >= cutoff_60s)
    usage_logger.info("API usage last_1s=%d last_60s=%d", n_1s, n_60s)


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket
# ─────────────────────────────────────────────────────────────────────────────
async def websocket_trade_listener(tickers: list):
    """
    Connect to Kalshi WebSocket and listen for trades on specified tickers only.
    Subscribes to market_tickers (positions + orders + volume_config tickers).
    Populates volume_tracker and volume_recorder. Retries with backoff on disconnect.
    """
    if not tickers:
        return
    retry_delay = BASE_RETRY_DELAY
    subscribe_msg = {
        "id": 1,
        "cmd": "subscribe",
        "params": {"channels": ["trade"], "market_tickers": tickers},
    }

    while True:
        try:
            timestamp = str(int(time.time() * 1000))
            msg_to_sign = timestamp + "GET" + "/trade-api/ws/v2"
            signature = private_key_obj.sign(
                msg_to_sign.encode('utf-8'),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH
                ),
                hashes.SHA256()
            )
            headers = {
                "KALSHI-ACCESS-KEY": api_key_id,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode('utf-8'),
                "KALSHI-ACCESS-TIMESTAMP": timestamp,
            }

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, headers=headers) as ws:
                    volume_tracker.ws_connected = True
                    volume_tracker.subscribed_tickers = set(tickers)
                    await ws.send_json(subscribe_msg)
                    retry_delay = BASE_RETRY_DELAY

                    while True:
                        msg = await ws.receive()

                        if msg.type == aiohttp.WSMsgType.PING:
                            await ws.pong()
                            continue
                        if msg.type == aiohttp.WSMsgType.PONG:
                            continue
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                msg_type = data.get('type')
                                if msg_type == 'trade':
                                    trade_msg = data.get('msg', {})
                                    ticker = trade_msg.get('market_ticker')
                                    count = trade_msg.get('count', 0)
                                    ts = trade_msg.get('ts', int(time.time()))
                                    side = (trade_msg.get('taker_side') or '').strip().lower()
                                    if side not in ('yes', 'no'):
                                        side = 'no'
                                    if ticker and count > 0:
                                        volume_tracker.add_trade(ticker, ts, count)
                                        volume_recorder.record_trade(ticker, ts, count, side)
                                elif msg_type == 'error':
                                    err = data.get('msg') or {}
                                    logger.warning(
                                        "WebSocket error from server: code=%s msg=%s",
                                        err.get('code'),
                                        err.get('msg', ''),
                                    )
                                elif msg_type not in ('subscribed', 'unsubscribed', 'ok', 'list_subscriptions'):
                                    logger.debug("WebSocket message type=%s", msg_type)
                            except json.JSONDecodeError as e:
                                logger.warning("WebSocket JSON decode error: %s", e)

                    logger.warning("WebSocket closed or error, reconnecting in %ss", retry_delay)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("WebSocket connection error: %s", e)
        finally:
            volume_tracker.ws_connected = False

        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 60)


async def update_ws_subscriptions(ws_task, tickers: list):
    """Update WebSocket subscriptions when tickers change (positions + orders + volume_config)."""
    config_tickers = volume_recorder.get_config_tickers()
    all_tickers = list(set(tickers + config_tickers))
    current = set(all_tickers)
    if current == volume_tracker.subscribed_tickers and volume_tracker.ws_connected:
        return ws_task
    if ws_task and not ws_task.done():
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
    if not all_tickers:
        return None
    return asyncio.create_task(websocket_trade_listener(all_tickers))


# ─────────────────────────────────────────────────────────────────────────────
# Data Processing
# ─────────────────────────────────────────────────────────────────────────────
def get_positions_list(positions_resp):
    """Extract positions list from response object."""
    return getattr(positions_resp, 'market_positions', None) or \
           getattr(positions_resp, 'positions', None) or []


def get_attr(obj, key, default=None):
    """Get attribute from dict or object. Handles None but preserves falsy values like 0."""
    if isinstance(obj, dict):
        val = obj.get(key, default)
    else:
        val = getattr(obj, key, default)
    return default if val is None else val


def get_contracts_ahead(orderbook_json: dict, side: str, action: str, my_price: int, bbo: dict) -> int:
    """
    Calculate contracts ahead of your order in a binary market.
    
    Binary market logic:
    - YES BUY at price X: competes with YES bids at prices > X
    - YES SELL at price X: equivalent to NO BUY at (100-X), competes with NO bids > (100-X)
    - NO BUY at price X: competes with NO bids at prices > X
    - NO SELL at price X: equivalent to YES BUY at (100-X), competes with YES bids > (100-X)
    """
    if not orderbook_json or not bbo:
        return 0
    
    orderbook = orderbook_json.get('orderbook', {})
    if not orderbook:
        return 0
    
    # Determine which side of the orderbook to look at
    if action == 'buy':
        book_side = side
        effective_price = my_price
        best_bid = bbo.get(f'{side}_bid', 0)
    else:
        # SELL = opposite side BUY (YES SELL = NO BUY)
        book_side = 'no' if side == 'yes' else 'yes'
        effective_price = 100 - my_price
        best_bid = bbo.get(f'{book_side}_bid', 0)
    
    if effective_price >= best_bid:
        return 0
    
    total_contracts = 0
    price_levels = orderbook.get(f'{book_side}_dollars') or []
    
    for level in price_levels:
        if not level or len(level) < 2:
            continue
        try:
            level_price_cents = int(float(level[0]) * 100)
            level_count = int(level[1])
            
            if effective_price < level_price_cents <= best_bid:
                total_contracts += level_count
        except (ValueError, TypeError, IndexError) as e:
            logger.debug(f"Orderbook level parse error: {e}")
            continue
    
    return total_contracts


def calculate_exposure(positions_resp, orders_list, queue_lookup, orderbooks=None, raw_orderbooks=None):
    """Calculate one-sided exposure per market including inventory, resting orders, and realized P&L."""
    positions = get_positions_list(positions_resp)
    orders = orders_list or []
    orderbooks = orderbooks or {}
    raw_orderbooks = raw_orderbooks or {}
    queue_lookup = queue_lookup or {}
    
    market_data = {}
    
    def init_market():
        return {
            'yes_inventory_dollars': 0,
            'no_inventory_dollars': 0,
            'yes_inventory_contracts': 0,
            'no_inventory_contracts': 0,
            'yes_resting_buy_dollars': 0,
            'no_resting_buy_dollars': 0,
            'yes_resting_buy_contracts': 0,
            'no_resting_buy_contracts': 0,
            'realized_pnl': 0,
            'yes_queue_positions': [],
            'no_queue_positions': [],
            'bbo': None
        }
    
    # Process positions
    for pos in positions:
        ticker = getattr(pos, 'ticker', None)
        if not ticker:
            continue
        position = getattr(pos, 'position', 0) or 0
        market_exposure = getattr(pos, 'market_exposure', 0) or 0
        realized_pnl = getattr(pos, 'realized_pnl', 0) or 0
        
        if ticker not in market_data:
            market_data[ticker] = init_market()
        
        market_data[ticker]['realized_pnl'] = realized_pnl
        
        if position > 0:
            market_data[ticker]['yes_inventory_dollars'] = market_exposure
            market_data[ticker]['yes_inventory_contracts'] = position
        elif position < 0:
            market_data[ticker]['no_inventory_dollars'] = market_exposure
            market_data[ticker]['no_inventory_contracts'] = abs(position)
    
    # Process resting orders
    for order in orders:
        status = get_attr(order, 'status')
        ticker = get_attr(order, 'ticker')
        side = get_attr(order, 'side')
        action = get_attr(order, 'action')
        remaining = get_attr(order, 'remaining_count', 0)
        order_id = get_attr(order, 'order_id')
        yes_price = get_attr(order, 'yes_price', 0)
        no_price = get_attr(order, 'no_price', 0)
        
        if status != 'resting' or not ticker or remaining == 0:
            continue
        
        if ticker not in market_data:
            market_data[ticker] = init_market()
        
        price = yes_price if side == 'yes' else no_price
        order_dollars = remaining * price
        
        # Calculate contracts ahead
        raw_ob = raw_orderbooks.get(ticker)
        bbo = orderbooks.get(ticker)
        # API returns 1-indexed position (1 = first), convert to 0-indexed contracts ahead
        api_queue_pos = queue_lookup.get(order_id, 0)
        queue_pos = max(0, api_queue_pos - 1) if api_queue_pos > 0 else 0
        contracts_at_better = get_contracts_ahead(raw_ob, side, action, price, bbo) if raw_ob and bbo else 0
        total_ahead = contracts_at_better + queue_pos
        
        # Track queue by EFFECTIVE side (YES SELL → NO Q, NO SELL → YES Q)
        effective_side = side if action == 'buy' else ('no' if side == 'yes' else 'yes')
        
        if effective_side == 'yes':
            market_data[ticker]['yes_queue_positions'].append(total_ahead)
        else:
            market_data[ticker]['no_queue_positions'].append(total_ahead)
        
        # Track buy exposure (buys create exposure - will fill as market moves through)
        if action == 'buy':
            if side == 'yes':
                market_data[ticker]['yes_resting_buy_dollars'] += order_dollars
                market_data[ticker]['yes_resting_buy_contracts'] += remaining
            else:
                market_data[ticker]['no_resting_buy_dollars'] += order_dollars
                market_data[ticker]['no_resting_buy_contracts'] += remaining
    
    # Add BBO data
    for ticker in market_data:
        if ticker in orderbooks:
            market_data[ticker]['bbo'] = orderbooks[ticker]
    
    return market_data


def calculate_bbo(orderbook_json: dict):
    """Calculate Best Bid/Offer from raw orderbook JSON response."""
    orderbook = (orderbook_json or {}).get('orderbook', {})
    if not orderbook:
        return None
    
    def best_bid(side_data):
        if not side_data:
            return 0
        try:
            prices = [float(level[0]) * 100 for level in side_data if level and len(level) >= 2]
            return int(max(prices)) if prices else 0
        except (ValueError, TypeError, IndexError) as e:
            logger.debug(f"BBO price parse error: {e}")
            return 0
    
    yes_bid = best_bid(orderbook.get('yes_dollars'))
    no_bid = best_bid(orderbook.get('no_dollars'))
    
    if yes_bid == 0 and no_bid == 0:
        return None
    
    yes_ask = 100 - no_bid if no_bid > 0 else 100
    return {
        'yes_bid': yes_bid,
        'yes_ask': yes_ask,
        'no_bid': no_bid,
        'spread': yes_ask - yes_bid if yes_bid > 0 and no_bid > 0 else 0
    }


def format_queue(queue_list):
    """Format queue positions as a compact string."""
    if not queue_list:
        return "-"
    return ", ".join(str(q) for q in sorted(queue_list)) if len(queue_list) > 1 else str(queue_list[0])


# ─────────────────────────────────────────────────────────────────────────────
# Data Building (for GUI)
# ─────────────────────────────────────────────────────────────────────────────
def calculate_net_risk(data: dict) -> int:
    """
    Calculate WORST-CASE net risk:
    1. Resting buy orders ADD to exposure (will fill as market crashes through your bid)
    2. Resting sell orders do NOT reduce risk (won't fill when market crashes against you)
    3. YES and NO contracts offset each other 1:1 (hedged)
    4. Unhedged contracts = net directional risk at cost basis
    """
    # Inventory
    yes_inv_contracts = data['yes_inventory_contracts']
    no_inv_contracts = data['no_inventory_contracts']
    yes_inv_cost = data['yes_inventory_dollars']
    no_inv_cost = data['no_inventory_dollars']
    
    # Resting buys ADD to exposure (will fill as market drops through your bid)
    yes_buy_contracts = data['yes_resting_buy_contracts']
    no_buy_contracts = data['no_resting_buy_contracts']
    yes_buy_cost = data['yes_resting_buy_dollars']
    no_buy_cost = data['no_resting_buy_dollars']
    
    # Add fees to resting orders (fees not included in inventory cost)
    # Fee formula: 0.0175 × C × P × (1-P) where P is price in dollars, C is contracts
    # Fees are rounded up
    if yes_buy_contracts > 0:
        # Average price per contract in dollars (cost is in cents)
        avg_yes_price_dollars = (yes_buy_cost / yes_buy_contracts) / 100
        # Fee = 0.0175 × contracts × price × (1 - price), rounded up
        yes_fee_cents = math.ceil(0.0175 * yes_buy_contracts * avg_yes_price_dollars * (1 - avg_yes_price_dollars) * 100)
        yes_buy_cost_with_fees = yes_buy_cost + yes_fee_cents
    else:
        yes_buy_cost_with_fees = yes_buy_cost
    
    if no_buy_contracts > 0:
        # Average price per contract in dollars (cost is in cents)
        avg_no_price_dollars = (no_buy_cost / no_buy_contracts) / 100
        # Fee = 0.0175 × contracts × price × (1 - price), rounded up
        no_fee_cents = math.ceil(0.0175 * no_buy_contracts * avg_no_price_dollars * (1 - avg_no_price_dollars) * 100)
        no_buy_cost_with_fees = no_buy_cost + no_fee_cents
    else:
        no_buy_cost_with_fees = no_buy_cost
    
    # Net Risk Rule: ONLY INVENTORY can provide offset, resting orders CANNOT
    # - YES inventory can offset NO exposure (inventory + resting)
    # - NO inventory can offset YES exposure (inventory + resting)
    # - Resting orders don't provide offset (they're not guaranteed to fill)
    
    # Total exposure on each side (inventory + resting buys with fees)
    yes_total_contracts = yes_inv_contracts + yes_buy_contracts
    yes_total_cost = yes_inv_cost + yes_buy_cost_with_fees
    no_total_contracts = no_inv_contracts + no_buy_contracts
    no_total_cost = no_inv_cost + no_buy_cost_with_fees
    
    # Offset provided by opposite INVENTORY only (not resting)
    yes_hedged_contracts = min(yes_total_contracts, no_inv_contracts)
    no_hedged_contracts = min(no_total_contracts, yes_inv_contracts)
    
    # Unhedged exposure
    yes_unhedged_contracts = yes_total_contracts - yes_hedged_contracts
    no_unhedged_contracts = no_total_contracts - no_hedged_contracts
    
    # Calculate unhedged cost (proportional)
    if yes_total_contracts > 0:
        yes_unhedged_cost = math.ceil((yes_unhedged_contracts * yes_total_cost) / yes_total_contracts)
    else:
        yes_unhedged_cost = 0
        
    if no_total_contracts > 0:
        no_unhedged_cost = math.ceil((no_unhedged_contracts * no_total_cost) / no_total_contracts)
    else:
        no_unhedged_cost = 0
    
    # Net risk = maximum of unhedged exposure on either side
    return max(yes_unhedged_cost, no_unhedged_cost)


def build_exposure_data(market_data) -> dict:
    """Build exposure data structure for GUI."""
    rows = []
    total_net_risk = 0
    total_pnl = 0

    for ticker, data in sorted(market_data.items()):
        yes_exp = data['yes_inventory_dollars'] + data['yes_resting_buy_dollars']
        no_exp = data['no_inventory_dollars'] + data['no_resting_buy_dollars']

        if yes_exp == 0 and no_exp == 0:
            continue

        net_risk = calculate_net_risk(data)
        bbo = data.get('bbo')

        rows.append({
            'ticker': ticker[:36],
            'yes_exp': yes_exp,
            'no_exp': no_exp,
            'max_risk': net_risk,
            'yes_q': format_queue(data.get('yes_queue_positions', []))[:15],
            'no_q': format_queue(data.get('no_queue_positions', []))[:15],
            'bid': bbo['yes_bid'] if bbo else 0,
            'ask': bbo['yes_ask'] if bbo else 0,
            'spread': bbo['spread'] if bbo else 0,
            'pnl': data['realized_pnl']
        })

        total_net_risk += net_risk
        total_pnl += data['realized_pnl']

    return {
        'rows': rows,
        'totals': {'pnl': total_pnl, 'max_risk': total_net_risk}
    }


def _load_volume_display_cache() -> dict:
    """Load volume display cache (1m–12h per ticker + updated_ts). Use timestamps for validity."""
    global _volume_display_cache, _volume_display_cache_loaded
    if _volume_display_cache_loaded:
        return _volume_display_cache
    _volume_display_cache_loaded = True
    try:
        if os.path.exists(VOLUME_DISPLAY_CACHE_PATH):
            with open(VOLUME_DISPLAY_CACHE_PATH, "r") as f:
                data = json.load(f)
            _volume_display_cache = data if isinstance(data, dict) else {"tickers": {}}
            if "tickers" not in _volume_display_cache:
                _volume_display_cache["tickers"] = {}
    except (json.JSONDecodeError, IOError) as e:
        logger.debug("Volume display cache load failed: %s", e)
        _volume_display_cache = {"tickers": {}}
    return _volume_display_cache


def _save_volume_display_cache(rows: list) -> None:
    """Persist 1m–12h per ticker (no 24h). Used for GUI startup restore."""
    global _volume_display_cache, _volume_cache_last_save_ts
    now = time.time()
    tickers = {}
    for r in rows:
        t = r.get("ticker")
        if not t:
            continue
        tickers[t] = {
            "1m": int(r.get("vol_1m", 0) or 0),
            "10m": int(r.get("vol_10m", 0) or 0),
            "1h": int(r.get("vol_1h", 0) or 0),
            "6h": int(r.get("vol_6h", 0) or 0),
            "12h": int(r.get("vol_12h", 0) or 0),
            "updated_ts": int(now),
        }
    _volume_display_cache["tickers"] = tickers
    _volume_cache_last_save_ts = now
    try:
        with open(VOLUME_DISPLAY_CACHE_PATH, "w") as f:
            json.dump({"tickers": tickers}, f, indent=0)
    except IOError as e:
        logger.debug("Volume display cache save failed: %s", e)


def save_volume_display_cache(rows: list) -> None:
    """Public save for volume display cache (e.g. GUI shutdown). Persists 1m–12h only; never 24h."""
    if rows:
        _save_volume_display_cache(rows)


def build_volume_data(tickers: list, market_data: dict = None) -> dict:
    """Build volume data structure for GUI.
    Uses live VolumeTracker when available; falls back to cached 1m–12h on startup (timestamps).
    24h volume always from API; never from cache.
    """
    global _volume_cache_last_save_ts
    market_data = market_data or {}
    cache = _load_volume_display_cache()
    ticker_cache = cache.get("tickers") or {}
    now = time.time()
    rows = []

    for ticker in sorted(tickers):
        volumes = volume_tracker.get_volumes(ticker)
        live_1m = volumes.get("1m", 0) or 0
        live_10m = volumes.get("10m", 0) or 0
        live_1h = volumes.get("1h", 0) or 0
        live_6h = volumes.get("6h", 0) or 0
        live_12h = volumes.get("12h", 0) or 0
        use_cache = (
            (live_1m == 0 and live_10m == 0 and live_1h == 0 and live_6h == 0 and live_12h == 0)
            and ticker in ticker_cache
        )
        ent = ticker_cache.get(ticker) or {}
        updated = int(ent.get("updated_ts") or 0)
        if use_cache and (now - updated) <= VOLUME_DISPLAY_CACHE_MAX_AGE:
            vol_1m = int(ent.get("1m") or 0)
            vol_10m = int(ent.get("10m") or 0)
            vol_1h = int(ent.get("1h") or 0)
            vol_6h = int(ent.get("6h") or 0)
            vol_12h = int(ent.get("12h") or 0)
        else:
            vol_1m, vol_10m, vol_1h, vol_6h, vol_12h = live_1m, live_10m, live_1h, live_6h, live_12h

        mkt = market_data.get(ticker, {})
        volume_24h = mkt.get("volume_24h", 0) or 0
        last_price = mkt.get("last_price", 50) or 50
        volume_24h_dollars = int(volume_24h * last_price)

        rows.append({
            "ticker": ticker[:36],
            "vol_1m": vol_1m,
            "vol_10m": vol_10m,
            "vol_1h": vol_1h,
            "vol_6h": vol_6h,
            "vol_12h": vol_12h,
            "vol_24h": volume_24h,
            "vol_24h_dollars": volume_24h_dollars,
        })

    if (now - _volume_cache_last_save_ts) >= VOLUME_DISPLAY_CACHE_SAVE_INTERVAL and rows:
        _save_volume_display_cache(rows)

    return {
        "rows": rows,
        "ws_connected": volume_tracker.ws_connected,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main Data Fetch
# ─────────────────────────────────────────────────────────────────────────────
async def _read_then_wait(coro):
    """Execute one API read, then sleep _read_interval to limit request rate.
    Records timestamp once immediately before the request (single append point).
    """
    _request_timestamps.append(time.time())
    result = await coro
    await asyncio.sleep(_read_interval)
    logger.debug("throttle sleep %.2fs", _read_interval)
    return result


async def fetch_dashboard_data(http_session: aiohttp.ClientSession) -> dict:
    """Fetch all data and return as structured dict for GUI.
    API reads are staggered (_read_interval between calls). Balance, market data,
    and orderbooks are cached to reduce redundant calls.
    """
    global _market_info_ts, _limits_ts, _orderbook_cache
    now = time.time()
    fetch_balance = (now - _balance_cache["ts"]) >= BALANCE_REFRESH_INTERVAL
    fetch_markets = (now - _market_info_ts) >= MARKET_REFRESH_INTERVAL
    fetch_limits = (now - _limits_ts) >= LIMITS_REFRESH_INTERVAL

    if fetch_limits:
        await _read_then_wait(fetch_account_limits_direct(http_session))
        _limits_ts = now

    if fetch_balance:
        balance_resp = await _read_then_wait(
            _tracked_sdk_request("get_balance", client._portfolio_api.get_balance())
        )
        balance = getattr(balance_resp, "balance", 0) or 0
        _balance_cache["balance"] = balance
        _balance_cache["ts"] = now
    else:
        balance = _balance_cache["balance"]

    positions_resp = await _read_then_wait(
        _tracked_sdk_request("get_positions", client._portfolio_api.get_positions())
    )
    orders_list = await _read_then_wait(fetch_resting_orders_direct(http_session))

    positions = get_positions_list(positions_resp)
    tickers_with_positions = [
        getattr(p, "ticker", None)
        for p in positions
        if getattr(p, "ticker", None) and (getattr(p, "position", 0) or 0) != 0
    ]
    open_count = len(tickers_with_positions)
    order_tickers = list(set(o.get("ticker") for o in orders_list if o.get("ticker")))
    all_tickers = list(set(tickers_with_positions + order_tickers))

    # Evict orderbook cache for tickers we no longer track
    for k in list(_orderbook_cache.keys()):
        if k not in all_tickers:
            del _orderbook_cache[k]

    queue_lookup = {}
    if order_tickers:
        queue_lookup = await _read_then_wait(
            fetch_queue_positions_direct(http_session, order_tickers)
        )

    orderbook_results = []
    for t in all_tickers:
        ent = _orderbook_cache.get(t)
        if ent is not None and (time.time() - ent[1]) < ORDERBOOK_CACHE_TTL:
            ob = ent[0]
        else:
            ob = await _read_then_wait(fetch_orderbook_direct(http_session, t))
            _orderbook_cache[t] = (ob, time.time())
        orderbook_results.append(ob)

    market_info = {}
    if fetch_markets:
        for t in all_tickers:
            mkt = await _read_then_wait(fetch_market_direct(http_session, t))
            if mkt:
                market_info[t] = mkt
        _market_info_cache.clear()
        _market_info_cache.update(market_info)
        _market_info_ts = now
    else:
        market_info = {t: _market_info_cache.get(t) for t in all_tickers}
        market_info = {t: m for t, m in market_info.items() if m is not None}

    orderbooks = {}
    raw_orderbooks = {}
    for ticker, result in zip(all_tickers, orderbook_results):
        if result:
            raw_orderbooks[ticker] = result
            bbo = calculate_bbo(result)
            if bbo:
                orderbooks[ticker] = bbo
                orderbook_recorder.record_orderbook(ticker, result, bbo)

    exposure_calc = calculate_exposure(
        positions_resp, orders_list, queue_lookup, orderbooks, raw_orderbooks
    )
    exposure_data = build_exposure_data(exposure_calc)
    volume_data = build_volume_data(all_tickers, market_info)

    _log_api_usage_summary(now)
    volume_recorder.flush_ended_periods()

    return {
        "balance": balance,
        "active_markets": open_count,
        "exposure": exposure_data,
        "volume": volume_data,
        "tickers": all_tickers,
        "timestamp": datetime.now(),
    }


if __name__ == "__main__":
    from gui import run_gui
    run_gui()
