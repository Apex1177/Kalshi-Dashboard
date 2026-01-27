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
    "YOUR-TICKER": {
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

### Volume Recording

Volume is recorded only for tickers listed in `volume_config.json` with a `volume` array. Data is written to `volume_data/{TICKER}_volume_data.csv` with columns:
- `ticker` - Market ticker
- `timestamp` - Period start time (UTC, `YYYY-MM-DD HH:MM:SS+00:00`)
- `day_of_week` - Day name (for time-of-day analysis)
- `interval` - Recording interval (1m, 10m, 1h)
- `volume_yes` - Contracts traded on the YES side (taker side) in the period
- `volume_no` - Contracts traded on the NO side (taker side) in the period

### Orderbook Recording

Orderbook snapshots are written to `orderbook_data/{TICKER}_orderbook_data.csv`. A row is written only when the orderbook has changed since the last write, to reduce file size. Columns:
- `ticker` - Market ticker (enables merging multiple files for analysis)
- `timestamp` - Snapshot time (UTC, `YYYY-MM-DD HH:MM:SS+00:00`)
- `day_of_week` - Day name
- `yes_bid` - Best YES bid (cents)
- `yes_ask` - Best YES ask (cents)
- `no_bid` - Best NO bid (cents)
- `spread` - Bid-ask spread (cents)
- `yes_depth` - Total contracts on YES side (top 5 levels)
- `no_depth` - Total contracts on NO side (top 5 levels)
- `yes_levels` - JSON array of top 5 YES price levels `[[price, contracts], ...]` (use `csv` reader for correct parsing)
- `no_levels` - JSON array of top 5 NO price levels `[[price, contracts], ...]`

## File Structure

```
.
├── .env                       # KALSHI_READONLY_KEY=your_key_id (user-created)
├── dashboard.py               # Main dashboard logic
├── gui.py                     # CustomTkinter GUI
├── volume_config.json.example # Example; copy for volume/orderbook recording (optional)
├── volume_config.json         # Volume and orderbook recording config (optional, user-created)
├── volume_data/               # CSV output (auto-created)
├── orderbook_data/            # Orderbook CSV output (auto-created)
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
