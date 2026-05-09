"""
StockNotify — Configuration (standalone edition)

Edit this file OR set environment variables (override order: env > .env file > defaults).

ENV overrides:
    SN_DB_KEY       — Databento API key
    SN_TG_TOKEN     — Telegram bot token
    SN_TG_CHATS     — JSON dict {"chat_id": "label", ...}
    SN_PORT         — Dashboard port (default 8436)
    SN_DATA_ROOT    — Storage root for historical bars (default ./data/bars)
"""

import os
import json as _json
import pathlib as _pl

# ── Load .env file if present (simple KEY=VALUE parser) ───────────────────────
def _load_dotenv():
    env_path = _pl.Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v

_load_dotenv()

# ── Load optional local Telegram config (telegram_config.json) ────────────────
_tg_cfg: dict = {}
_tg_cfg_path = _pl.Path(__file__).parent.parent / "telegram_config.json"
if _tg_cfg_path.exists():
    try:
        _tg_cfg = _json.loads(_tg_cfg_path.read_text())
    except Exception:
        pass

# ── Databento ─────────────────────────────────────────────────────────────────
DATABENTO_API_KEY: str = (
    os.environ.get("SN_DB_KEY")
    or os.environ.get("DATABENTO_API_KEY")
    or "YOUR_DATABENTO_API_KEY"
)
DATASET: str = "EQUS.MINI"   # US Equities Mini — requires Standard + US Equities plan

# ── Tickers ───────────────────────────────────────────────────────────────────
TICKERS: list = [
    "QQQ",   # Nasdaq 100 ETF
    "SPY",   # S&P 500 ETF
    "NVDA",  # Nvidia
    "TSLA",  # Tesla
    "AAPL",  # Apple
    "MSFT",  # Microsoft
    "AMZN",  # Amazon
    "META",  # Meta
    "GOOGL", # Alphabet
    "AMD",   # AMD
    "SMCI",  # Super Micro Computer
]

# ── Opening Range Breakout ────────────────────────────────────────────────────
ORB_30_MINUTES: int = 30   # 30-min ORB (9:30–10:00 ET)
ORB_60_MINUTES: int = 60   # 1-hr ORB  (9:30–10:30 ET)

# ORB retest zone: within this % of ORB high/low counts as a retest
ORB_RETEST_TOL_PCT: float = 0.3   # 0.3% tolerance band around ORB level

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = (
    os.environ.get("SN_TG_TOKEN")
    or _tg_cfg.get("bot_token", "")
    or "YOUR_TELEGRAM_BOT_TOKEN"
)

# Configure your Telegram chat IDs here (or use SN_TG_CHATS env var as JSON).
# Example: {"123456789": "my_channel", "987654321": "my_dm"}
_raw_chats = os.environ.get("SN_TG_CHATS", "")
if _raw_chats:
    try:
        TELEGRAM_CHATS: dict = _json.loads(_raw_chats)
    except Exception:
        TELEGRAM_CHATS = {}
else:
    TELEGRAM_CHATS: dict = {
        str(_tg_cfg.get("channel_id", "")): "alerts_channel",
        str(_tg_cfg.get("admin_chat_id", "")): "admin",
    } if _tg_cfg else {}

# Remove empty keys
TELEGRAM_CHATS = {k: v for k, v in TELEGRAM_CHATS.items() if k and k != "None" and k != ""}

# ── Dashboard auth ────────────────────────────────────────────────────────────
# Bearer tokens for dashboard login.
# Localhost connections bypass auth automatically.
AUTH_TOKENS: dict = {
    "admin-change-this-token": "admin",
    "bart-change-this-token":  "bart",
}

# ── Dashboard server ──────────────────────────────────────────────────────────
PORT: int = int(os.environ.get("SN_PORT", "8436"))

# ── Storage limits ────────────────────────────────────────────────────────────
MAX_BARS_STORED: int   = 200   # per ticker, in-memory ring buffer
MAX_ALERTS_STORED: int = 500   # total alert log entries

# ── Historical data storage ────────────────────────────────────────────────────
# Default: ./data/bars relative to the StockNotify project folder.
# Override: set SN_DATA_ROOT env var or edit below.
HIST_START_DATE: str  = "2023-03-28"   # EQUS.MINI available from this date
_default_root = str(_pl.Path(__file__).parent.parent / "data" / "bars")
HIST_STORAGE_ROOT: str = os.environ.get("SN_DATA_ROOT", _default_root)

# ── Alert Indicator Registry ───────────────────────────────────────────────────
ALERT_INDICATORS: dict = {
    "ORB_30_RETEST_HIGH": {"enabled": True, "timeframe": "30m", "label": "ORB30 Retest ↑", "description": "Price retests 30-min ORB high after breakout (buy zone) → Telegram",  "color": "#fbbf24"},
    "ORB_30_RETEST_LOW":  {"enabled": True, "timeframe": "30m", "label": "ORB30 Retest ↓", "description": "Price retests 30-min ORB low after breakdown (short zone) → Telegram", "color": "#fb923c"},
    "ORB_60_RETEST_HIGH": {"enabled": True, "timeframe": "1h",  "label": "ORB60 Retest ↑", "description": "Price retests 1-hr ORB high after breakout (buy zone) → Telegram",    "color": "#f59e0b"},
    "ORB_60_RETEST_LOW":  {"enabled": True, "timeframe": "1h",  "label": "ORB60 Retest ↓", "description": "Price retests 1-hr ORB low after breakdown (short zone) → Telegram",  "color": "#ea580c"},
    "MACD_CROSS_BULL":    {"enabled": True, "timeframe": "1w",  "label": "MACD Bull Cross", "description": "Weekly MACD histogram turns positive — EMA12 crosses above signal line", "color": "#22d3ee"},
    "MACD_CROSS_BEAR":    {"enabled": True, "timeframe": "1w",  "label": "MACD Bear Cross", "description": "Weekly MACD histogram turns negative — EMA12 crosses below signal line", "color": "#f43f5e"},
    "VWAP_CROSS_PDH":     {"enabled": True, "timeframe": "1m",  "label": "VWAP × PDH",      "description": "Session VWAP crosses the prior day's high — key intraday level",         "color": "#facc15"},
    "VWAP_CROSS_PDL":     {"enabled": True, "timeframe": "1m",  "label": "VWAP × PDL",      "description": "Session VWAP crosses the prior day's low — key intraday level",          "color": "#e879f9"},
}
