# StockNotify

Live US equity scanner — ORB retest, MACD cross, and VWAP × PDH/PDL signals, delivered via Telegram with PNG charts.

## Quick Start

### 1. Prerequisites
- Python 3.9+
- Databento account with **Standard** plan + **US Equities** add-on (for EQUS.MINI)
- Telegram bot token (optional — alerts still log locally without Telegram)

### 2. Setup (Windows)
```bat
setup.bat
```
This creates a virtual environment and installs all dependencies.

### 3. Configure
Edit `.env` (created from `.env.example` during setup):
```env
SN_DB_KEY=db-your_databento_api_key
SN_TG_TOKEN=your_telegram_bot_token
SN_TG_CHATS={"123456789": "my_channel", "987654321": "admin"}
```

### 4. Run
```bat
start.bat
```
Then open http://localhost:8436 in your browser.

---

## Signals

| Signal | Timeframe | Description |
|--------|-----------|-------------|
| ORB30 Retest ↑ | 30m | Price retests 30-min ORB high after breakout (buy zone) |
| ORB30 Retest ↓ | 30m | Price retests 30-min ORB low after breakdown (short zone) |
| ORB60 Retest ↑ | 1h | Price retests 1-hr ORB high after breakout (buy zone) |
| ORB60 Retest ↓ | 1h | Price retests 1-hr ORB low after breakdown (short zone) |
| MACD Bull Cross | Weekly | Weekly MACD histogram flips positive |
| MACD Bear Cross | Weekly | Weekly MACD histogram flips negative |
| VWAP × PDH | 1m | Session VWAP crosses prior day's high |
| VWAP × PDL | 1m | Session VWAP crosses prior day's low |

---

## Default Tickers
QQQ, SPY, NVDA, TSLA, AAPL, MSFT, AMZN, META, GOOGL, AMD, SMCI

Edit `stocknotify/tickers.json` to add/remove tickers. Changes take effect on the next stream reconnect.

---

## Dashboard Features
- **White theme** — clean light mode UI
- **Live candlestick chart** — TradingView LightweightCharts with VWAP, W.VWAP, ORB levels
- **Mini ticker cards** — click any card to open full-size chart modal
- **Indicator controls** — enable/disable individual signals
- **History panel** — pull and store historical 1-min bars for each ticker
- **WebSocket live updates** — new bars and signals appear instantly
- **Telegram test** — send a test alert from the dashboard

---

## Structure
```
StockNotify/
├── main.py                     — entry point (python main.py)
├── requirements.txt
├── .env.example                — copy to .env and fill in keys
├── setup.bat                   — one-time setup (Windows)
├── start.bat                   — launch with live stream
├── start_no_stream.bat         — launch dashboard only (no Databento)
├── build_exe.bat               — build StockNotify.exe
├── StockNotify.spec            — PyInstaller spec
├── data/bars/                  — historical parquet files (auto-created)
├── logs/                       — watchdog logs (auto-created)
└── stocknotify/
    ├── config.py               — all configuration
    ├── runner.py               — main orchestrator
    ├── streamer.py             — Databento EQUS.MINI live stream
    ├── analysis.py             — signal detection engine
    ├── alerts.py               — Telegram delivery
    ├── chartgen.py             — PNG chart generator
    ├── history.py              — historical bar puller
    ├── dashboard.py            — FastAPI web dashboard
    ├── chart.py                — desktop PySide6 chart window
    ├── watchdog.py             — 24/7 health monitor + auto-restart
    ├── tickers.json            — active ticker list
    └── matrix_tickers.json     — matrix panel tickers
```

---

## Command Line Options
```
python main.py [--port PORT] [--no-stream]

  --port PORT     Dashboard port (default: 8436)
  --no-stream     Skip Databento connection (dashboard-only mode)
```

---

## EXE Build
```bat
build_exe.bat
```
Produces `dist/StockNotify.exe` — a fully self-contained executable. The user still needs a `.env` file with their API keys in the same directory.

---

## Watchdog (optional 24/7 auto-restart)
```bat
start watchdog.bat
```
Or: `python -m stocknotify.watchdog`

Monitors the dashboard health endpoint every 60s and restarts the service automatically if it goes down.
