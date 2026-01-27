# Kalshi Portfolio Dashboard

A real-time portfolio monitoring tool for Kalshi that displays market exposure, order queue positions, BBO spreads, and trailing trading volume. The dashboard fetches data via the Kalshi REST API and streams live trade data through WebSocket for volume tracking.

## Features

- **Portfolio Exposure** - View capital deployed and net risk per market (with fee calculations)
- **Order Queue Position** - See how many contracts are ahead of your orders
- **BBO Spread** - Monitor best bid/offer spreads in real-time
- **Trailing Volume** - Track 1-minute, 10-minute, 1-hour, 6-hour, 12-hour, and 24-hour trading volume
- **Volume Recording** - Export trade volume data to CSV for analysis
- **Orderbook Recording** - Export orderbook snapshots to CSV for market depth analysis

## Setup

**Python 3.9+** required.

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

On Linux, you also need tkinter:
```bash
sudo apt-get install python3-tk
```

### 2. Configure API Access

Create a `.env` file in the project directory:
```
KALSHI_READONLY_KEY=your_api_key_id
```

Place your RSA private key file `Read_Only_Kalshi_Key.txt` in the **parent directory** (one level above this project folder). The app loads it from there.
```
your-project/                  # This repo (clone root)
├── .env                       # KALSHI_READONLY_KEY=your_key_id
├── dashboard.py
├── gui.py
├── ...
└── README.md

../Read_Only_Kalshi_Key.txt    # Your RSA private key (PEM), one level up
```

### 3. Configure Volume / Orderbook Recording (Optional)

Create `volume_config.json` and list tickers with `volume` and/or `orderbook` intervals:

```bash
cp volume_config.json.example volume_config.json
```

- **`volume`** – intervals to record trade volume (CSV in `volume_data/`). Only these tickers are recorded.
- **`orderbook`** – intervals to record orderbook snapshots (CSV in `orderbook_data/`). Use `[]` to skip.

Example:

```json
{
  "tickers": {
    "EXAMPLE-EVENT": {
      "volume": ["1m", "10m"],
      "orderbook": ["1m"]
    }
  }
}
```

Available intervals: `1s`, `1m`, `10m`, `1h`.

## Usage

```bash
python dashboard.py
```

The GUI updates when each fetch cycle completes. Balance, market data, and orderbooks are cached to cut down API calls; cycles run at most ~2.5 reads/s by default, with a short pause between cycles. You can change the max **Read/s** in the status bar (1–20) and click **Apply** to take effect immediately.

## Data Recording

One file per `(ticker, interval)`.

### Volume Recording

Volume is recorded only for tickers in `volume_config.json` with a `volume` array. Files: `volume_data/{ticker}_{interval}_volume.csv` (e.g. `EXAMPLE-EVENT_1m_volume.csv`). A row is written only when there is volume in the period (zero-volume periods are skipped).

**Format:** CSV, UTF-8. Header row, then one data row per record.

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_utc` | string | Period start (UTC, ISO 8601: `YYYY-MM-DDTHH:MM:SSZ`) |
| `volume_yes` | int | Contracts traded on the YES side (taker) in the period |
| `volume_no` | int | Contracts traded on the NO side (taker) in the period |

Example:
```csv
timestamp_utc,volume_yes,volume_no
2026-01-27T22:30:00Z,150,42
2026-01-27T22:35:00Z,0,200
```

### Orderbook Recording

Orderbook snapshots: `orderbook_data/{ticker}_{interval}_orderbook.csv` (e.g. `EXAMPLE-EVENT_1m_orderbook.csv`). A row is written only when the orderbook has changed since the last write.

**Format:** CSV, UTF-8. Header row, then one data row per record. `yes_levels` and `no_levels` are JSON strings; use a CSV-aware reader (e.g. `csv` module or `pd.read_csv`) so quoted commas inside the JSON are handled correctly.

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_utc` | string | Snapshot time (UTC, ISO 8601: `YYYY-MM-DDTHH:MM:SSZ`) |
| `yes_bid` | int | Best YES bid (cents) |
| `yes_ask` | int | Best YES ask (cents) |
| `no_bid` | int | Best NO bid (cents) |
| `spread` | int | Bid-ask spread (cents) |
| `yes_depth` | int | Total contracts on YES side (top 5 levels) |
| `no_depth` | int | Total contracts on NO side (top 5 levels) |
| `yes_levels` | string | JSON array `[[price, contracts], ...]` for top 5 YES levels (price in dollars) |
| `no_levels` | string | JSON array `[[price, contracts], ...]` for top 5 NO levels (price in dollars) |

Example:
```csv
timestamp_utc,yes_bid,yes_ask,no_bid,spread,yes_depth,no_depth,yes_levels,no_levels
2026-01-27T22:37:00Z,32,33,67,1,2167,2809,"[[\"0.27\", 25], [\"0.29\", 2000]]","[[\"0.64\", 2000], [\"0.67\", 300]]"
```

## File Structure

```
.
├── .env                       # KALSHI_READONLY_KEY=your_key_id (user-created)
├── dashboard.py               # Main dashboard logic
├── gui.py                     # CustomTkinter GUI
├── volume_config.json.example # Example; copy for volume/orderbook recording (optional)
├── volume_config.json         # Volume and orderbook recording config (optional, user-created)
├── volume_data/               # Volume CSVs: e.g. EXAMPLE-EVENT_1m_volume.csv (auto-created)
├── orderbook_data/            # Orderbook CSVs: e.g. EXAMPLE-EVENT_1m_orderbook.csv (auto-created)
├── logs/                      # Log files (auto-created)
├── cache/                     # Volume display cache (auto-created)
├── requirements.txt
└── README.md
```

## Logs

Error logs are written to `logs/dashboard.log`. The log file rotates when it reaches 5MB, keeping up to 3 backup files. Logs are gitignored and auto-created when the dashboard runs.

## Notes

- All dollar amounts in the codebase are stored in **cents** (divide by 100 for display)
- Net risk calculation includes fees on resting orders
- Only inventory positions can hedge each other; resting orders cannot provide hedging offset
