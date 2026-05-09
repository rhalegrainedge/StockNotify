import pathlib as _sn_pl
_SN_DATA_ROOT = str(_sn_pl.Path(__file__).parent.parent / 'data' / 'bars')
"""
StockNotify — FastAPI web dashboard (port 8436)

Features:
  - Full-size candlestick main chart with symbol switcher
  - Daily VWAP + Weekly VWAP + SMA 9/20/50/200 (DMAs) overlay
  - ORB high/low price lines, signal markers
  - Control panel: pull history, reload stream, stream status
  - Natural language command area with help/explainer
  - Mini card grid for all tickers (click to expand modal)
  - WebSocket live updates (new bar + signals every minute)
  - Ticker management: add/remove from header UI
  - Localhost connections bypass auth (CENTRAL tab works without token)
  - External access requires ?token=YOUR_TOKEN
"""

import asyncio
import json
import logging
import os
import queue as sync_queue
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import pytz
from fastapi import FastAPI, Request, HTTPException, Depends, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

log = logging.getLogger("sn_dashboard")
ET  = pytz.timezone("America/New_York")

# ── Injected by sn_runner.py ──────────────────────────────────────────────────
_config   = None
_engine   = None    # AnalysisEngine
_alerter  = None    # TelegramAlerter
_streamer = None    # StockStreamer
_history  = None    # HistoryPuller
_indicator_state: dict = {}   # runtime enabled/disabled per signal type (mutable copy from config)
_ticker_alert_enabled: dict = {}  # {ticker: bool} — per-ticker alert suppression
_matrix_tickers: list = []        # tickers shown in symbol×indicator matrix (persisted to file)

_MATRIX_TICKERS_FILE = os.path.join(os.path.dirname(__file__), "matrix_tickers.json")


def _load_matrix_tickers() -> list:
    global _matrix_tickers
    if _matrix_tickers:
        return list(_matrix_tickers)
    try:
        if os.path.exists(_MATRIX_TICKERS_FILE):
            _matrix_tickers = json.load(open(_MATRIX_TICKERS_FILE)).get("tickers", ["QQQ", "TSLA"])
        else:
            _matrix_tickers = ["QQQ", "TSLA"]
    except Exception:
        _matrix_tickers = ["QQQ", "TSLA"]
    return list(_matrix_tickers)


def _save_matrix_tickers():
    try:
        with open(_MATRIX_TICKERS_FILE, "w") as f:
            json.dump({"tickers": _matrix_tickers}, f, indent=2)
    except Exception:
        pass


# ── WebSocket broadcast ───────────────────────────────────────────────────────
_ws_clients: set        = set()
_event_queue: sync_queue.SimpleQueue = sync_queue.SimpleQueue()
_async_loop: Optional[asyncio.AbstractEventLoop] = None


def broadcast(msg: dict):
    """Thread-safe: called from sync threads (streamer/analysis) to push to all WS clients."""
    _event_queue.put(msg)


async def _ws_broadcast_loop():
    while True:
        try:
            while not _event_queue.empty():
                msg = _event_queue.get_nowait()
                dead = set()
                for ws in list(_ws_clients):
                    try:
                        await ws.send_json(msg)
                    except Exception:
                        dead.add(ws)
                _ws_clients.difference_update(dead)
        except Exception as exc:
            log.debug(f"WS broadcast loop error: {exc}")
        await asyncio.sleep(0.2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _async_loop
    _async_loop = asyncio.get_running_loop()
    task = asyncio.create_task(_ws_broadcast_loop())
    yield
    task.cancel()


def init(config, engine, alerter, streamer=None):
    global _config, _engine, _alerter, _streamer, _history, _indicator_state, _ticker_alert_enabled
    _config   = config
    _engine   = engine
    _alerter  = alerter
    _streamer = streamer
    # Build runtime indicator state from config defaults
    if hasattr(config, "ALERT_INDICATORS"):
        _indicator_state = {k: dict(v) for k, v in config.ALERT_INDICATORS.items()}
    # Seed per-ticker alert enabled state (all ON by default)
    for tk in (getattr(config, "TICKERS", []) or []):
        if tk not in _ticker_alert_enabled:
            _ticker_alert_enabled[tk] = True
    try:
        from stocknotify.history import HistoryPuller
        _history = HistoryPuller(config)
    except Exception as exc:
        log.warning(f"HistoryPuller not available: {exc}")


_coverage_cache: dict = {}      # ticker → (timestamp, stats_dict)
_COVERAGE_TTL:   int  = 300     # seconds before re-reading parquet


def _get_parquet_stats(ticker: str) -> dict:
    """Read historical parquet ts column and return bar count, date range, % coverage. Cached 5 min."""
    import time as _time
    now = _time.time()
    cached = _coverage_cache.get(ticker)
    if cached and (now - cached[0]) < _COVERAGE_TTL:
        return cached[1]
    try:
        import pandas as pd
        root = getattr(_config, "HIST_STORAGE_ROOT", _SN_DATA_ROOT) if _config else _SN_DATA_ROOT
        path = os.path.join(root, ticker, f"{ticker}_1m.parquet")
        if not os.path.exists(path):
            r = {"bars": 0, "first_date": None, "last_date": None, "pct": 0.0, "expected_days": 0}
            _coverage_cache[ticker] = (now, r); return r
        df = pd.read_parquet(path, columns=["ts", "close"])
        total = len(df)
        if total == 0:
            r = {"bars": 0, "first_date": None, "last_date": None, "pct": 0.0,
                 "expected_days": 0, "last_close": None, "last_ts": None}
            _coverage_cache[ticker] = (now, r); return r
        first_ts   = int(df["ts"].min())
        last_ts_v  = int(df["ts"].max())
        last_close = round(float(df["close"].iloc[-1]), 2)
        first_date = datetime.fromtimestamp(first_ts).strftime("%Y-%m-%d")
        last_date  = datetime.fromtimestamp(last_ts_v).strftime("%Y-%m-%d")
        start_str  = getattr(_config, "HIST_START_DATE", "2023-03-28") if _config else "2023-03-28"
        hist_start = pd.Timestamp(start_str)
        today_ts   = pd.Timestamp.now().normalize()
        bdays      = len(pd.bdate_range(hist_start, today_ts))
        expected   = bdays * 390          # 390 one-minute bars per full session (9:30–16:00 ET)
        pct        = min(round(total / expected * 100, 1), 100.0) if expected else 0.0
        today_str  = datetime.now().strftime("%Y-%m-%d")
        needs_pull = last_date < today_str   # parquet behind today
        r = {"bars": total, "first_date": first_date, "last_date": last_date,
             "pct": pct, "expected_days": bdays, "expected_bars": expected,
             "last_close": last_close, "last_ts": last_ts_v, "needs_pull": needs_pull}
        _coverage_cache[ticker] = (now, r)
        return r
    except Exception as exc:
        log.debug(f"parquet stats {ticker}: {exc}")
        r = {"bars": 0, "first_date": None, "last_date": None, "pct": 0.0, "expected_days": 0}
        _coverage_cache[ticker] = (now, r)
        return r


def is_indicator_enabled(signal_type: str) -> bool:
    """Check if a signal type is currently enabled (for filtering Telegram + WS broadcasts)."""
    if not _indicator_state:
        return True   # default: allow all if registry not loaded
    entry = _indicator_state.get(signal_type)
    return entry["enabled"] if entry else True


def is_ticker_alert_enabled(ticker: str) -> bool:
    """Check if alerts for a specific ticker are currently enabled."""
    return _ticker_alert_enabled.get(ticker, True)


app = FastAPI(title="StockNotify", docs_url=None, redoc_url=None, lifespan=lifespan)


# ── Auth ──────────────────────────────────────────────────────────────────────

def auth(request: Request, token: Optional[str] = Query(None)) -> str:
    host = request.client.host if request.client else ""
    if host in ("127.0.0.1", "::1", "localhost"):
        return "local"
    hdr = request.headers.get("Authorization", "")
    tok = hdr[7:] if hdr.startswith("Bearer ") else token
    if not tok:
        raise HTTPException(status_code=401, detail="Add ?token=YOUR_TOKEN to the URL")
    user = (_config.AUTH_TOKENS.get(tok) if _config else None)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")
    return user


def _load_tickers_file() -> list:
    f = os.path.join(os.path.dirname(__file__), "tickers.json")
    if os.path.exists(f):
        try:
            return json.load(open(f)).get("tickers", [])
        except Exception:
            pass
    return (_config.TICKERS if _config else [])


def _save_tickers_file(tickers: list):
    f = os.path.join(os.path.dirname(__file__), "tickers.json")
    with open(f, "w") as fp:
        json.dump({"tickers": tickers}, fp, indent=2)


# ── Dashboard HTML ────────────────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>StockNotify</title>
<script src="https://unpkg.com/lightweight-charts@4/dist/lightweight-charts.standalone.production.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#f8fafc;color:#1e293b;font-family:'Segoe UI',system-ui,sans-serif;font-size:13px;overflow-x:hidden}

/* ── Header ── */
header{background:#f1f5f9;border-bottom:1px solid #cbd5e1;padding:7px 14px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;position:sticky;top:0;z-index:200}
header h1{font-size:13px;font-weight:800;letter-spacing:.5px;white-space:nowrap;color:#10b981}
.sdot{width:8px;height:8px;border-radius:50%;background:#374151;flex-shrink:0}
.sdot.live{background:#22c55e;box-shadow:0 0 6px #22c55e88}.sdot.dead{background:#ef4444}
.pill-row{display:flex;flex-wrap:wrap;gap:4px;flex:1;min-width:0}
.tp{display:inline-flex;align-items:center;gap:3px;background:#e2e8f0;border:1px solid #cbd5e1;border-radius:10px;padding:2px 7px 2px 8px;font-size:11px;font-weight:700;cursor:default}
.tp .rm{cursor:pointer;color:#64748b;font-size:11px;margin-left:1px}.tp .rm:hover{color:#ef4444}
.add-row{display:flex;gap:5px;align-items:center;flex-shrink:0}
#new-tk{background:#e2e8f0;border:1px solid #cbd5e1;border-radius:5px;color:#1e293b;padding:3px 7px;font-size:11px;width:82px;outline:none}
#new-tk:focus{border-color:#10b981}
.hdr-btn{background:#e2e8f0;color:#334155;border:1px solid #cbd5e1;border-radius:5px;padding:3px 9px;font-size:11px;font-weight:700;cursor:pointer}
.hdr-btn:hover{background:#334155;color:#1e293b}
.hdr-btn.green{border-color:#065f46;color:#10b981}.hdr-btn.green:hover{background:#065f46}
.hdr-btn.blue{border-color:#1d4ed8;color:#60a5fa}.hdr-btn.blue:hover{background:#1d4ed8;color:#fff}
#lupd{color:#64748b;font-size:10px;white-space:nowrap}

/* ── Signal matrix ── */
.mx-panel{background:#f1f5f9;border-bottom:2px solid #cbd5e1;padding:10px 14px}
.mx-title{font-size:9px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:1.2px;margin-bottom:8px;display:flex;align-items:center;gap:8px}
.mx-tk-row{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:8px;align-items:center}
.mx-tk-pill{display:inline-flex;align-items:center;gap:3px;background:#e2e8f0;border:1px solid #cbd5e1;border-radius:8px;padding:2px 8px;font-size:10px;font-weight:700;color:#334155}
.mx-tk-pill .rm{cursor:pointer;color:#64748b;margin-left:2px;font-size:10px}.mx-tk-pill .rm:hover{color:#ef4444}
.mx-add{display:flex;gap:4px;align-items:center;margin-left:4px}
#mx-new-tk{background:#f0f4f8;border:1px solid #cbd5e1;border-radius:4px;color:#1e293b;padding:2px 6px;font-size:10px;width:64px;outline:none}
#mx-new-tk:focus{border-color:#10b981}
.mx-add-btn{background:#e2e8f0;color:#334155;border:1px solid #cbd5e1;border-radius:4px;padding:2px 8px;font-size:10px;font-weight:700;cursor:pointer}
.mx-add-btn:hover{background:#334155;color:#1e293b}
.mx-wrap{overflow-x:auto}
.mx-table{border-collapse:collapse;font-size:10px;white-space:nowrap;min-width:100%}
.mx-table th{padding:5px 10px;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.7px;color:#64748b;border-bottom:1px solid #cbd5e1;background:#f8fafc;text-align:center}
.mx-table th.ind-col{text-align:left;width:140px;position:sticky;left:0;background:#f8fafc;z-index:2}
.mx-table th.tf-col{width:40px;color:#94a3b8}
.mx-table td{padding:4px 10px;border-bottom:1px solid #0f172a;vertical-align:middle}
.mx-table td.ind-col{font-weight:700;font-size:10px;color:#334155;position:sticky;left:0;background:#f1f5f9;z-index:1;white-space:nowrap}
.mx-table td.tf-col{text-align:center;font-size:9px;color:#94a3b8}
.mx-table tr:hover td{background:#f0f4f8}.mx-table tr:hover td.ind-col{background:#f0f4f8}
.mx-hit{display:inline-flex;flex-direction:column;align-items:center;gap:1px;padding:3px 8px;border-radius:5px;min-width:70px}
.mx-hit-time{font-size:10px;font-weight:700;font-variant-numeric:tabular-nums}
.mx-hit-cnt{font-size:8px;opacity:.7}
.mx-miss{color:#1e293b;font-size:12px;text-align:center}

/* ── Coverage table ── */
.cov-panel{background:#f1f5f9;border-bottom:2px solid #cbd5e1;padding:10px 14px}
.cov-title{font-size:9px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:1.2px;margin-bottom:8px;display:flex;align-items:center;gap:10px}
.cov-title .cov-upd{font-size:9px;font-weight:400;color:#94a3b8;margin-left:auto}
.cov-table{width:100%;border-collapse:collapse;font-size:11px;font-variant-numeric:tabular-nums}
.cov-table th{text-align:left;padding:4px 8px;color:#64748b;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.7px;border-bottom:1px solid #cbd5e1;white-space:nowrap;background:#f8fafc}
.cov-table th.r{text-align:right}
.cov-table td{padding:5px 8px;border-bottom:1px solid #0f172a;vertical-align:middle;white-space:nowrap}
.cov-table tr:last-child td{border-bottom:none}
.cov-table tr:hover td{background:#f0f4f8}
.cov-sym{font-size:13px;font-weight:800;letter-spacing:.5px;color:#1e293b}
.cov-dot{width:8px;height:8px;border-radius:50%;display:inline-block;flex-shrink:0}
.cov-dot.live{background:#22c55e;box-shadow:0 0 6px #22c55e88}
.cov-dot.dead{background:#ef4444}
.cov-lat{font-size:10px}.cov-lat.ok{color:#22c55e}.cov-lat.warn{color:#f59e0b}.cov-lat.dead{color:#ef4444}
.cov-px{font-size:12px;font-weight:700}
.cov-px.up{color:#22c55e}.cov-px.dn{color:#ef4444}.cov-px.nc{color:#64748b}
.cov-vwap-pct{font-size:10px;font-weight:700}.cov-vwap-pct.up{color:#22c55e}.cov-vwap-pct.dn{color:#ef4444}
.cov-today{font-size:10px;color:#334155}
.cov-hist{font-size:10px;color:#334155;text-align:right}
.cov-bar-wrap{width:90px;background:#f8fafc;border-radius:3px;height:8px;overflow:hidden;display:inline-block;vertical-align:middle}
.cov-bar-fill{height:100%;border-radius:3px;transition:width .4s}
.cov-pct-lbl{font-size:10px;font-weight:700;margin-left:6px;display:inline-block;min-width:36px}
.cov-date{font-size:9px;color:#64748b}

/* ── Indicator table ── */
.ind-panel{background:#f1f5f9;border-bottom:2px solid #cbd5e1;padding:10px 14px}
.ind-title{font-size:9px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:1.2px;margin-bottom:8px;display:flex;align-items:center;gap:8px}
.ind-table{width:100%;border-collapse:collapse;font-size:11px}
.ind-table th{text-align:left;padding:4px 10px;color:#64748b;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;border-bottom:1px solid #cbd5e1;white-space:nowrap}
.ind-table td{padding:5px 10px;border-bottom:1px solid #0f172a;vertical-align:middle}
.ind-table tr:last-child td{border-bottom:none}
.ind-table tr:hover td{background:#f0f4f8}
.ind-row.off td{opacity:.35}
.ind-toggle{display:inline-flex;align-items:center;gap:6px;cursor:pointer;user-select:none}
.ind-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;display:inline-block}
.ind-label{font-weight:700}
.ind-off-label{color:#64748b;font-size:9px;font-style:italic}
.ind-tf-badge{display:inline-block;font-size:9px;font-weight:700;background:#f8fafc;border:1px solid #cbd5e1;border-radius:3px;padding:1px 5px;color:#64748b;letter-spacing:.3px}
.ind-ch-badge{display:inline-flex;align-items:center;gap:3px;font-size:9px;font-weight:700;padding:2px 6px;border-radius:3px;border:1px solid #cbd5e1;white-space:nowrap}
.ind-ch-badge.tg{border-color:#0369a1;color:#38bdf8;background:#082f49}
.ind-ch-badge.ws{border-color:#cbd5e1;color:#64748b;background:#f8fafc}
.ind-desc{color:#64748b;font-size:10px;line-height:1.4}
.ind-calc{color:#64748b;font-size:9px;font-style:italic;line-height:1.3}

/* ── Ticker alert toggles ── */
.tk-alert-row{display:flex;flex-wrap:wrap;align-items:center;gap:5px;margin-bottom:10px;padding:7px 10px;background:#f8fafc;border-radius:6px;border:1px solid #cbd5e1}
.tk-alert-lbl{font-size:9px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:1px;white-space:nowrap;margin-right:4px}
.tk-alert-pill{display:inline-flex;align-items:center;gap:4px;border-radius:8px;padding:3px 10px;font-size:11px;font-weight:700;cursor:pointer;border:1px solid;transition:all .15s;user-select:none}
.tk-alert-pill.on{background:#052e16;border-color:#14532d;color:#4ade80}.tk-alert-pill.on:hover{background:#064e3b;border-color:#065f46}
.tk-alert-pill.off{background:#f0f4f8;border-color:#94a3b8;color:#64748b}.tk-alert-pill.off:hover{background:#e2e8f0;border-color:#4b5563}
.tk-alert-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.tk-alert-pill.on .tk-alert-dot{background:#22c55e}.tk-alert-pill.off .tk-alert-dot{background:#374151}

/* ── Recent triggers in indicator table ── */
.ind-recent-cell{font-size:9px;line-height:1.7;min-width:150px}
.ind-recent-entry{display:flex;align-items:center;gap:5px;padding:1px 0;white-space:nowrap}
.ind-recent-tk{font-weight:800;font-size:10px;min-width:36px;color:#1e293b}
.ind-recent-px{color:#64748b;font-variant-numeric:tabular-nums}
.ind-recent-time{color:#94a3b8;font-size:9px}
.ind-recent-tg{font-size:9px}

/* ── Desktop chart launcher ── */
.launcher-wrap{background:#e8edf3;border-bottom:2px solid #cbd5e1;padding:14px}
.launcher-btn{display:block;width:100%;padding:16px 24px;background:linear-gradient(135deg,#1e3a5f 0%,#0f2744 100%);border:2px solid #3b82f6;border-radius:10px;color:#93c5fd;font-size:16px;font-weight:800;cursor:pointer;text-align:center;letter-spacing:.5px;transition:all .2s;margin-bottom:10px}
.launcher-btn:hover{background:linear-gradient(135deg,#2563eb 0%,#1d4ed8 100%);border-color:#60a5fa;color:#fff;box-shadow:0 0 24px #3b82f644}
.launcher-tk-row{display:flex;gap:5px;flex-wrap:wrap;justify-content:center}
.launch-tk{background:#e2e8f0;border:1px solid #cbd5e1;border-radius:6px;color:#334155;font-size:12px;font-weight:800;padding:5px 12px;cursor:pointer;transition:all .15s;letter-spacing:.3px}
.launch-tk:hover{background:#1e3a5f;color:#93c5fd;border-color:#3b82f6}
.launch-tk.sel{background:#1e3a5f;color:#3b82f6;border-color:#3b82f6;box-shadow:0 0 8px #3b82f644}

/* ── Latency pills in header ── */
.tp{display:inline-flex;align-items:center;gap:4px;background:#e2e8f0;border:1px solid #cbd5e1;border-radius:10px;padding:2px 7px 2px 6px;font-size:10px;font-weight:700;cursor:default;white-space:nowrap}
.tp .lat{font-size:9px;color:#64748b;font-weight:400}.tp .lat.ok{color:#22c55e}.tp .lat.warn{color:#f59e0b}.tp .lat.dead{color:#ef4444}
.tp .rm{cursor:pointer;color:#64748b;font-size:11px;margin-left:2px}.tp .rm:hover{color:#ef4444}
.tp .ldot{width:6px;height:6px;border-radius:50%;background:#374151;flex-shrink:0}
.tp .ldot.live{background:#22c55e;box-shadow:0 0 4px #22c55e88}.tp .ldot.dead{background:#ef4444}

/* ── Control panel ── */
.ctrl-panel{display:flex;align-items:center;gap:8px;padding:8px 14px;background:#f0f4f8;border-bottom:1px solid #cbd5e1;flex-wrap:wrap}
.ctrl-label{font-size:10px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.8px;white-space:nowrap}
.ctrl-btn{background:#e2e8f0;color:#334155;border:1px solid #cbd5e1;border-radius:6px;padding:5px 12px;font-size:11px;font-weight:700;cursor:pointer}
.ctrl-btn:hover{background:#334155;color:#1e293b}
.ctrl-btn.green{border-color:#065f46;color:#10b981}.ctrl-btn.green:hover{background:#065f46}
.ctrl-btn.amber{border-color:#78350f;color:#fbbf24}.ctrl-btn.amber:hover{background:#78350f}
.stream-chips{display:flex;gap:4px;flex-wrap:wrap;margin-left:auto}
.s-chip{background:#e2e8f0;border-radius:4px;padding:2px 6px;font-size:9px;font-weight:700;color:#64748b}
.s-chip.ok{color:#22c55e;background:#052e16}.s-chip.no{color:#6b7280;background:#f0f4f8}
.hist-summary{font-size:10px;color:#64748b;margin-left:4px}

/* ── Command area ── */
.cmd-wrap{padding:10px 14px;background:#f1f5f9;border-bottom:1px solid #cbd5e1}
.cmd-row{display:flex;gap:6px;align-items:center;margin-bottom:6px}
#cmd-input{flex:1;background:#f0f4f8;border:1px solid #cbd5e1;border-radius:6px;color:#1e293b;padding:7px 12px;font-size:12px;outline:none;font-family:inherit}
#cmd-input:focus{border-color:#10b981}
.cmd-send{background:#10b981;color:#fff;border:none;border-radius:6px;padding:7px 14px;font-size:12px;font-weight:700;cursor:pointer}
.cmd-send:hover{background:#059669}
.cmd-quick{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:6px}
.qbtn{background:#e2e8f0;border:1px solid #cbd5e1;border-radius:5px;color:#334155;font-size:10px;padding:3px 8px;cursor:pointer}
.qbtn:hover{background:#334155;color:#1e293b}
#cmd-log{min-height:22px;font-size:11px;color:#64748b;font-family:monospace;padding:2px 4px;max-height:60px;overflow-y:auto}
.help-toggle{font-size:10px;color:#3b82f6;cursor:pointer;text-decoration:underline;margin-left:4px}
.help-panel{display:none;background:#f0f4f8;border:1px solid #cbd5e1;border-radius:6px;padding:12px;margin-top:6px;font-size:11px;line-height:1.7;color:#334155}
.help-panel.open{display:block}
.help-panel h4{color:#60a5fa;font-size:11px;margin-top:8px;margin-bottom:2px}
.help-panel h4:first-child{margin-top:0}
.help-panel code{background:#f8fafc;border-radius:3px;padding:1px 4px;color:#34d399;font-family:monospace}

/* ── Chart grid (mini cards) ── */
.chart-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px;padding:10px 12px}
.chart-card{background:#f0f4f8;border:1px solid #cbd5e1;border-radius:7px;overflow:hidden;cursor:pointer;transition:border-color .15s}
.chart-card:hover{border-color:#cbd5e1}
.cc-header{display:flex;justify-content:space-between;align-items:center;padding:7px 10px 4px}
.cc-ticker{font-size:13px;font-weight:800;letter-spacing:.5px}
.cc-price{font-size:13px;font-weight:700}
.cc-price.up{color:#22c55e}.cc-price.dn{color:#ef4444}.cc-price.nc{color:#64748b}
.cc-chart{height:100px}
.cc-meta{display:flex;gap:0;border-top:1px solid #0f172a}
.cc-stat{flex:1;padding:4px 8px;border-right:1px solid #0f172a;font-size:9px;color:#64748b}
.cc-stat:last-child{border-right:none}
.cc-stat span{display:block;color:#1e293b;font-weight:700;font-size:10px;font-variant-numeric:tabular-nums}
.cc-actions{display:flex;gap:4px;padding:4px 8px;border-top:1px solid #0f172a}
.cc-act{flex:1;background:#e2e8f0;border:1px solid #cbd5e1;border-radius:4px;color:#334155;font-size:9px;font-weight:700;padding:2px 4px;cursor:pointer}
.cc-act:hover{background:#334155}.cc-act.blue{border-color:#1d4ed8;color:#60a5fa}.cc-act.blue:hover{background:#1d4ed8;color:#fff}
.cc-act.green{border-color:#065f46;color:#34d399}.cc-act.green:hover{background:#065f46}
.hist-status{font-size:9px;color:#64748b;padding:1px 8px 3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

/* ── Signal log ── */
.sig-panel{margin:0 12px 12px;background:#f0f4f8;border:1px solid #cbd5e1;border-radius:7px;padding:10px}
.sig-title{font-size:10px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}
.sig-row{display:flex;align-items:center;gap:7px;padding:4px 0;border-bottom:1px solid #0f172a;font-size:11px}
.sig-row:last-child{border-bottom:none}
.sig-tk{font-weight:800;min-width:48px}.sig-badge{font-size:9px;font-weight:700;padding:2px 5px;border-radius:3px;white-space:nowrap}
.ORB_30_RETEST_HIGH,.ORB_60_RETEST_HIGH{background:#451a03;color:#fbbf24}
.ORB_30_RETEST_LOW,.ORB_60_RETEST_LOW{background:#431407;color:#fb923c}
.MACD_CROSS_BULL{background:#0c4a6e;color:#22d3ee}
.MACD_CROSS_BEAR{background:#4c0519;color:#f43f5e}
.VWAP_CROSS_PDH{background:#422006;color:#facc15}
.VWAP_CROSS_PDL{background:#3b0764;color:#e879f9}
.sig-px{color:#334155;font-variant-numeric:tabular-nums}.sig-ts{color:#64748b;font-size:9px;margin-left:auto;white-space:nowrap}
.no-sig{color:#64748b;font-style:italic;font-size:11px;padding:4px 0}

/* ── Market Status Banner ── */
.mkt-banner{display:flex;align-items:center;gap:10px;padding:7px 16px;border-bottom:2px solid #cbd5e1;font-size:11px;transition:background .5s,border-color .5s}
.mkt-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;transition:background .5s,box-shadow .5s}
.mkt-status{font-weight:800;letter-spacing:1px;text-transform:uppercase;font-size:12px}
.mkt-session{color:#64748b;font-size:10px;font-weight:500;margin-left:2px}
.mkt-countdown{color:#64748b;margin-left:8px;font-size:10px}
.mkt-et{margin-left:auto;color:#94a3b8;font-variant-numeric:tabular-nums;font-size:10px;font-weight:600}
.mkt-stale{font-size:9px;color:#f59e0b;margin-left:10px;background:#78350f22;border:1px solid #78350f55;border-radius:4px;padding:1px 6px}

/* ── Modal ── */
.modal-bg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:500;align-items:center;justify-content:center}
.modal-bg.open{display:flex}
.modal{background:#f0f4f8;border:1px solid #cbd5e1;border-radius:10px;width:92vw;max-width:1200px;overflow:hidden}
.modal-head{display:flex;align-items:center;padding:9px 14px;border-bottom:1px solid #cbd5e1;gap:10px}
.modal-tk{font-size:17px;font-weight:800}.modal-px{font-size:15px;font-weight:700}
.modal-close{margin-left:auto;cursor:pointer;color:#64748b;font-size:19px;line-height:1}.modal-close:hover{color:#1e293b}
.modal-stats{display:flex;gap:0;border-bottom:1px solid #0f172a}
.ms{flex:1;padding:5px 12px;border-right:1px solid #0f172a;font-size:10px;color:#64748b}
.ms:last-child{border-right:none}.ms span{display:block;color:#1e293b;font-weight:700;font-size:11px}
.modal-chart{height:50vh}.modal-legend{display:flex;gap:14px;padding:6px 12px;border-top:1px solid #0f172a;flex-wrap:wrap}
.m-leg{display:flex;align-items:center;gap:4px;font-size:10px;color:#334155}
.m-leg-line{width:18px;height:2px;border-radius:1px}
</style>
</head>
<body>

<!-- ── Header ───────────────────────────────────────────────────────────── -->
<header>
  <div class="sdot" id="sdot"></div>
  <h1>&#9670; StockNotify</h1>
  <div class="pill-row" id="pill-row"></div>
  <div class="add-row">
    <input id="new-tk" type="text" placeholder="Add ticker" maxlength="6"
           onkeydown="if(event.key==='Enter')addTicker()">
    <button class="hdr-btn green" onclick="addTicker()">+Add</button>
    <button class="hdr-btn" id="restart-btn" onclick="restartServer()"
      style="border-color:#7c3aed;color:#a78bfa;white-space:nowrap"
      title="Kill and restart the StockNotify server (reconnects in ~10s)">
      &#8635; Restart
    </button>
  </div>
  <span id="lupd"></span>
</header>

<!-- ── Market Status Banner ──────────────────────────────────────────────── -->
<div id="mkt-banner" class="mkt-banner"></div>

<!-- ── Signal Matrix ─────────────────────────────────────────────────────── -->
<div class="mx-panel">
  <div class="mx-title">
    &#9670; SYMBOL &times; INDICATOR MATRIX
    <span style="color:#374151;font-weight:400;font-size:9px">— today's fired signals per symbol</span>
    <span id="mx-upd" style="margin-left:auto;color:#374151;font-size:9px;font-weight:400"></span>
  </div>
  <div class="mx-tk-row">
    <span style="font-size:9px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:.8px;white-space:nowrap">Symbols:</span>
    <div id="mx-tk-pills"></div>
    <div class="mx-add">
      <input id="mx-new-tk" type="text" placeholder="Add…" maxlength="6"
             onkeydown="if(event.key==='Enter')addMatrixTicker()">
      <button class="mx-add-btn" onclick="addMatrixTicker()">+</button>
    </div>
  </div>
  <div class="mx-wrap">
    <table class="mx-table" id="mx-table">
      <thead id="mx-thead"></thead>
      <tbody id="mx-tbody">
        <tr><td colspan="4" style="color:#374151;font-style:italic;padding:8px 10px">Loading…</td></tr>
      </tbody>
    </table>
  </div>
</div>

<!-- ── Coverage Table ────────────────────────────────────────────────────── -->
<div class="cov-panel">
  <div class="cov-title">
    &#9670; STREAMING &amp; DATA COVERAGE
    <span class="cov-upd" id="cov-upd">—</span>
  </div>
  <table class="cov-table">
    <thead>
      <tr>
        <th>Symbol</th>
        <th>Stream</th>
        <th>Latency</th>
        <th class="r">Last Price</th>
        <th class="r">vs VWAP</th>
        <th class="r">VWAP</th>
        <th class="r">W.VWAP</th>
        <th class="r">Today Bars</th>
        <th class="r">Hist Bars</th>
        <th>Date Range</th>
        <th>Coverage</th>
      </tr>
    </thead>
    <tbody id="cov-tbody">
      <tr><td colspan="11" style="color:#374151;font-style:italic;padding:10px 8px">Loading coverage data…</td></tr>
    </tbody>
  </table>
</div>

<!-- ── Indicator Panel ───────────────────────────────────────────────────── -->
<div class="ind-panel">
  <div class="ind-title">
    &#9670; ALERT INDICATORS
    <span style="color:#374151;font-weight:400;font-size:9px">— click ON/OFF to toggle Telegram + WebSocket alerts per indicator; toggle ticker pills to suppress all alerts for a symbol</span>
  </div>
  <!-- Ticker-level alert toggles -->
  <div class="tk-alert-row" id="tk-alert-row">
    <span class="tk-alert-lbl">Per-Ticker Alerts:</span>
    <span style="color:#374151;font-style:italic;font-size:10px">Loading…</span>
  </div>
  <table class="ind-table">
    <thead>
      <tr>
        <th style="width:110px">Indicator</th>
        <th style="width:60px">TF</th>
        <th style="width:80px">Channel</th>
        <th>Description &amp; Calculation</th>
        <th style="width:70px;text-align:center">Enabled</th>
        <th style="width:180px">Last 3 Triggers</th>
      </tr>
    </thead>
    <tbody id="ind-tbody">
      <tr><td colspan="6" style="color:#374151;font-style:italic;padding:8px 10px">Loading…</td></tr>
    </tbody>
  </table>
</div>

<!-- ── Desktop Chart Launcher ────────────────────────────────────────────── -->
<div class="launcher-wrap">
  <button class="launcher-btn" onclick="launchDesktopChart()">
    &#9112;&nbsp;&nbsp;LAUNCH DESKTOP CHART&nbsp;&nbsp;&#9112;
    <div style="font-size:11px;font-weight:400;color:#60a5fa;margin-top:4px">Full pyqtgraph candlestick window — VWAP · WVWAP · ORB · signals</div>
  </button>
  <div class="launcher-tk-row" id="launcher-tk-row"></div>
</div>

<!-- ── Control Panel ─────────────────────────────────────────────────────── -->
<div class="ctrl-panel">
  <span class="ctrl-label">Controls</span>
  <button class="ctrl-btn green" onclick="pullAllHistory()">&#8595; Pull All History</button>
  <button class="ctrl-btn amber" onclick="reloadStream()">&#8635; Reload Stream</button>
  <button class="ctrl-btn" id="tg-test-btn" onclick="testTelegram()" style="border-color:#0369a1;color:#38bdf8">&#9992; Test Telegram</button>
  <span id="tg-status" style="font-size:10px;color:#475569"></span>
  <div class="hist-summary" id="hist-summary"></div>
  <div class="stream-chips" id="stream-chips"></div>
</div>

<!-- ── Command Area ──────────────────────────────────────────────────────── -->
<div class="cmd-wrap">
  <div class="cmd-row">
    <input id="cmd-input" type="text" placeholder='Type a command: "add PLTR" · "pull history NVDA" · "pull all" · "reload" · "help"'
           onkeydown="if(event.key==='Enter')executeCmd()">
    <button class="cmd-send" onclick="executeCmd()">&#9656; Run</button>
    <span class="help-toggle" onclick="document.getElementById('help-panel').classList.toggle('open')">? Help</span>
  </div>
  <div class="cmd-quick">
    <button class="qbtn" onclick="setCmd('pull all')">&#8595; Pull All History</button>
    <button class="qbtn" onclick="setCmd('reload')">&#8635; Reload Stream</button>
    <button class="qbtn" onclick="setCmd('status')">&#9679; Stream Status</button>
    <button class="qbtn" onclick="setCmd('help')">? Help</button>
  </div>
  <div id="cmd-log"></div>
  <div class="help-panel" id="help-panel">
    <h4>&#9670; Ticker Management</h4>
    <code>add TICKER</code> — Add a new ticker to the live stream (e.g., <code>add PLTR</code>)<br>
    <code>remove TICKER</code> — Remove a ticker from stream and charts<br>
    <h4>&#8595; Historical Data</h4>
    <code>pull history TICKER</code> — Download 3-year 1-min bars for one ticker<br>
    <code>pull all</code> — Download history for all tracked tickers<br>
    History stored at <code>D:/CentralFolder/STOCKNOTIFY/{TICKER}/{TICKER}_1m.parquet</code><br>
    <h4>&#8635; Stream Control</h4>
    <code>reload</code> — Reconnect the live stream (picks up new tickers)<br>
    <code>status</code> — Show which tickers are actively receiving live data<br>
    <h4>&#9670; Indicators</h4>
    Click any pill in the INDICATOR PANEL to enable/disable that alert.<br>
    Disabled indicators do NOT send Telegram messages or appear in signal log.<br>
    <b>ORB 15m / 30m Break</b> — Price closes beyond opening range. Fires once/day.<br>
    <b>ORB Retest</b> — Price retests ORB level after breakout. Fires once/breakout. → Telegram<br>
    <b>VWAP Cross</b> — Price crosses daily VWAP (volume-gated). Each cross fires.<br>
    <b>VWAP Retest</b> — Price was &gt;0.5% from VWAP, approaches within 0.2%. → Telegram<br>
    <b>VWAP Bounce</b> — Confirmed bounce off VWAP (moves away 0.15%+ after retest).<br>
    <b>WVWAP Cross</b> — Price crosses weekly VWAP. Each cross fires.<br>
    All enabled signals → Telegram channels configured in sn_config.py.
  </div>
</div>

<!-- ── Mini Card Grid ────────────────────────────────────────────────────── -->
<div class="chart-grid" id="chart-grid"></div>

<!-- ── Signal Log ────────────────────────────────────────────────────────── -->
<div class="sig-panel">
  <div class="sig-title">Recent Signals</div>
  <div id="sig-log"><div class="no-sig">No signals yet today</div></div>
</div>

<!-- ── Telegram Channel Info ─────────────────────────────────────────────── -->
<div class="sig-panel" style="margin-top:0">
  <div class="sig-title" style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
    &#9992; TELEGRAM CHANNELS &amp; ROUTING
    <button onclick="sendTestAllTickers()" id="tg-mass-test-btn"
      style="margin-left:auto;background:#082f49;color:#38bdf8;border:1px solid #0369a1;border-radius:5px;padding:4px 14px;font-size:10px;font-weight:700;cursor:pointer;white-space:nowrap">
      &#9992; Test Send — All Tickers
    </button>
  </div>
  <table style="width:100%;border-collapse:collapse;font-size:11px;margin-bottom:12px">
    <thead>
      <tr style="border-bottom:1px solid #1e293b">
        <th style="text-align:left;padding:4px 10px;color:#475569;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.7px">Bot</th>
        <th style="text-align:left;padding:4px 10px;color:#475569;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.7px">Channel / Destination</th>
        <th style="text-align:left;padding:4px 10px;color:#475569;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.7px">Chat ID</th>
        <th style="text-align:left;padding:4px 10px;color:#475569;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.7px">Receives</th>
        <th style="text-align:center;padding:4px 10px;color:#475569;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.7px">Status</th>
      </tr>
    </thead>
    <tbody id="tg-channel-tbody">
      <tr><td colspan="5" style="color:#374151;font-style:italic;padding:8px 10px">Loading…</td></tr>
    </tbody>
  </table>
  <div style="font-size:9px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:1px;margin-bottom:7px">Per-Ticker Test Results</div>
  <div id="tg-ticker-results" style="display:flex;flex-wrap:wrap;gap:5px;min-height:22px">
    <span style="color:#374151;font-size:10px;font-style:italic">Click "Test Send — All Tickers" to verify delivery</span>
  </div>
</div>

<!-- ── Modal ─────────────────────────────────────────────────────────────── -->
<div class="modal-bg" id="modal" onclick="closeModal(event)">
  <div class="modal">
    <div class="modal-head">
      <span class="modal-tk" id="m-ticker"></span>
      <span class="modal-px" id="m-price"></span>
      <span class="modal-close" onclick="closeModal()">&#10005;</span>
    </div>
    <div class="modal-stats">
      <div class="ms">Daily VWAP<span id="m-vwap">—</span></div>
      <div class="ms">Weekly VWAP<span id="m-wvwap">—</span></div>
      <div class="ms">ORB High<span id="m-orb-h">—</span></div>
      <div class="ms">ORB Low<span id="m-orb-l">—</span></div>
      <div class="ms">Bars today<span id="m-bars">—</span></div>
    </div>
    <div class="modal-chart" id="modal-chart"></div>
    <div class="modal-legend">
      <div class="m-leg"><div class="m-leg-line" style="background:#60a5fa"></div>VWAP</div>
      <div class="m-leg"><div class="m-leg-line" style="background:#a78bfa"></div>WVWAP</div>
      <div class="m-leg"><div class="m-leg-line" style="background:#4fc3f7"></div>SMA9</div>
      <div class="m-leg"><div class="m-leg-line" style="background:#81c784"></div>SMA20</div>
      <div class="m-leg"><div class="m-leg-line" style="background:#ffb74d"></div>SMA50</div>
      <div class="m-leg"><div class="m-leg-line" style="background:#ef5350"></div>SMA200</div>
      <div class="m-leg"><div class="m-leg-line" style="background:#f59e0b"></div>ORB</div>
    </div>
  </div>
</div>

<script>
const TOKEN = new URLSearchParams(window.location.search).get('token') || '';
const fmt   = (v,d=2) => v != null ? (+v).toFixed(d) : '—';
const fmtT  = iso => {
  if (!iso) return '—';
  try { const d = typeof iso==='number'?new Date(iso*1000):new Date(iso);
        return d.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',hour12:true,timeZone:'America/New_York'}); }
  catch { return String(iso).slice(11,16); }
};
function secsAgo(iso) {
  if (!iso) return null;
  try { return Math.floor((Date.now() - new Date(iso).getTime()) / 1000); } catch { return null; }
}

async function api(path) {
  const sep = path.includes('?') ? '&' : '?';
  const r = await fetch(path + (TOKEN ? sep+'token='+TOKEN : ''));
  if (!r.ok) throw r.status;
  return r.json();
}
async function postApi(path, body) {
  const sep = path.includes('?') ? '&' : '?';
  const r = await fetch(path + (TOKEN ? sep+'token='+TOKEN : ''), {
    method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body||{})
  });
  if (!r.ok) throw r.status;
  return r.json();
}

// ── State ─────────────────────────────────────────────────────────────────
let allStatus  = {};
let allMarkers = {};
let miniCharts = {};
const modalChart = {};
let modalTicker  = null;
let _selectedLaunchTk = null;

// ── Chart helpers ─────────────────────────────────────────────────────────
function computeSMA(bars, n) {
  const out = [];
  for (let i = n-1; i < bars.length; i++) {
    let s = 0;
    for (let j = i-n+1; j <= i; j++) s += bars[j].c;
    out.push({time: bars[i].t, value: parseFloat((s/n).toFixed(4))});
  }
  return out;
}
const BASE_OPTS = (h, bg='#111827') => ({
  width: 0, height: h,
  layout: {background:{color:bg}, textColor:'#64748b'},
  grid: {vertLines:{color:'#111827'}, horzLines:{color:'#111827'}},
  crosshair: {mode: LightweightCharts.CrosshairMode.Normal},
  timeScale: {timeVisible:true, secondsVisible:false, borderColor:'#1e293b',
               tickMarkFormatter: t => {
                 const d = new Date(t*1000);
                 return d.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZone:'America/New_York'});
               }},
  rightPriceScale: {borderColor:'#1e293b', scaleMargins:{top:0.08,bottom:0.06}},
});
const CANDLE_OPTS = {upColor:'#26a69a',downColor:'#ef5350',borderUpColor:'#26a69a',borderDownColor:'#ef5350',wickUpColor:'#26a69a',wickDownColor:'#ef5350'};
const LINE = (c,w=1,dash=false,pv=false,lv=false,t='') => ({color:c, lineWidth:w, lineStyle: dash?LightweightCharts.LineStyle.Dashed:0, priceLineVisible:pv, lastValueVisible:lv, title:t});

function mergeBars(hist, live) {
  const map = {};
  hist.forEach(b => { map[b.t] = b; });
  live.forEach(b => { map[b.t] = {...(map[b.t]||{}), ...b}; });
  return Object.values(map).sort((a,b)=>a.t-b.t);
}

// ── Indicator table ───────────────────────────────────────────────────────

// Extended metadata: calculation details + channel info per signal type
const IND_META = {
  ORB_30_RETEST_HIGH: {calc:'After 30-min ORB (9:30–10:00 ET) breakout: price returns within 0.3% of ORB high. Once/breakout.', channels:['tg','ws']},
  ORB_30_RETEST_LOW:  {calc:'After 30-min ORB (9:30–10:00 ET) breakdown: price returns within 0.3% of ORB low. Once/breakdown.', channels:['tg','ws']},
  ORB_60_RETEST_HIGH: {calc:'After 1-hr ORB (9:30–10:30 ET) breakout: price returns within 0.3% of ORB high. Once/breakout.', channels:['tg','ws']},
  ORB_60_RETEST_LOW:  {calc:'After 1-hr ORB (9:30–10:30 ET) breakdown: price returns within 0.3% of ORB low. Once/breakdown.', channels:['tg','ws']},
  MACD_CROSS_BULL:    {calc:'EMA12(weekly) crosses above EMA26(weekly). Histogram flips positive. Fires once per new week.', channels:['tg','ws']},
  MACD_CROSS_BEAR:    {calc:'EMA12(weekly) crosses below EMA26(weekly). Histogram flips negative. Fires once per new week.', channels:['tg','ws']},
  VWAP_CROSS_PDH:     {calc:'Session VWAP crosses prior day\'s high from below or above. Each cross fires.', channels:['tg','ws']},
  VWAP_CROSS_PDL:     {calc:'Session VWAP crosses prior day\'s low from below or above. Each cross fires.',  channels:['tg','ws']},
};

async function loadIndicators() {
  try {
    const inds = await api('/api/indicators');
    const tbody = document.getElementById('ind-tbody');
    tbody.innerHTML = inds.map(ind => {
      const on  = ind.enabled;
      const meta = IND_META[ind.name] || {calc:'—', channels:['ws']};
      const chBadges = meta.channels.map(ch =>
        ch==='tg'
          ? `<span class="ind-ch-badge tg">&#9992; Telegram</span>`
          : `<span class="ind-ch-badge ws">&#9670; WebSocket</span>`
      ).join(' ');
      return `<tr class="ind-row ${on?'on':'off'}" id="ind-row-${ind.name}">
        <td>
          <span class="ind-toggle" onclick="toggleIndicator('${ind.name}')">
            <span class="ind-dot" style="background:${on?ind.color:'#374151'}"></span>
            <span class="ind-label" style="color:${on?ind.color:'#64748b'}">${ind.label}</span>
          </span>
        </td>
        <td><span class="ind-tf-badge">${ind.timeframe}</span></td>
        <td>${chBadges}</td>
        <td>
          <div class="ind-desc">${ind.description}</div>
          <div class="ind-calc">${meta.calc}</div>
        </td>
        <td style="text-align:center">
          <button class="ind-toggle-btn" onclick="toggleIndicator('${ind.name}')"
            style="background:${on?'#065f46':'#1e293b'};color:${on?'#34d399':'#475569'};border:1px solid ${on?'#065f46':'#334155'};border-radius:4px;padding:3px 9px;font-size:10px;font-weight:700;cursor:pointer;min-width:42px">
            ${on?'ON':'OFF'}
          </button>
        </td>
        <td class="ind-recent-cell" id="ind-recent-${ind.name}">
          <span style="color:#374151;font-style:italic">—</span>
        </td>
      </tr>`;
    }).join('');
    // Fill recent triggers immediately after rendering rows
    await loadIndicatorRecent();
  } catch(e) {
    const tbody = document.getElementById('ind-tbody');
    if (tbody) tbody.innerHTML = '<tr><td colspan="6" style="color:#475569;padding:8px 10px">Could not load indicators — reload page</td></tr>';
  }
}

// ── Ticker alert toggles ──────────────────────────────────────────────────
async function loadTickerAlerts() {
  try {
    const data = await api('/api/tickers/alerts');
    const row = document.getElementById('tk-alert-row');
    if (!row) return;
    const tickers = Object.keys(data).sort();
    if (!tickers.length) { row.innerHTML = '<span class="tk-alert-lbl">Per-Ticker Alerts:</span><span style="color:#374151;font-size:10px;font-style:italic">No tickers tracked</span>'; return; }
    row.innerHTML = '<span class="tk-alert-lbl">Per-Ticker Alerts:</span>' + tickers.map(tk => {
      const on = data[tk];
      return `<span class="tk-alert-pill ${on?'on':'off'}" id="tk-alert-pill-${tk}"
        onclick="toggleTickerAlert('${tk}')" title="${on?'Click to suppress all alerts for '+tk:'Click to re-enable alerts for '+tk}">
        <span class="tk-alert-dot"></span>${tk}
      </span>`;
    }).join('');
  } catch(e) {}
}

async function toggleTickerAlert(tk) {
  try {
    const r = await postApi('/api/tickers/'+tk+'/toggle-alerts');
    const pill = document.getElementById('tk-alert-pill-'+tk);
    if (pill) {
      const on = r.enabled;
      pill.className = 'tk-alert-pill '+(on?'on':'off');
      pill.title = on ? 'Click to suppress all alerts for '+tk : 'Click to re-enable alerts for '+tk;
    }
  } catch(e) {}
}

// ── Indicator recent triggers ─────────────────────────────────────────────
async function loadIndicatorRecent() {
  try {
    const data = await api('/api/indicators/recent');
    for (const [sigType, entries] of Object.entries(data)) {
      const cell = document.getElementById('ind-recent-'+sigType);
      if (!cell) continue;
      if (!entries || !entries.length) {
        cell.innerHTML = '<span style="color:#374151;font-style:italic">—</span>';
        continue;
      }
      cell.innerHTML = entries.map(e => {
        const tgBadge = e.tg_sent
          ? '<span class="ind-recent-tg" title="Telegram sent" style="color:#38bdf8">&#9992;</span>'
          : '<span class="ind-recent-tg" title="WebSocket only" style="color:#374151">&#9670;</span>';
        const timeStr = fmtT(e.ts);
        return `<div class="ind-recent-entry">
          <span class="ind-recent-tk">${e.ticker}</span>
          <span class="ind-recent-px">$${(+e.price).toFixed(2)}</span>
          <span class="ind-recent-time">${timeStr}</span>
          ${tgBadge}
        </div>`;
      }).join('');
    }
  } catch(e) {}
}

async function toggleIndicator(name) {
  try {
    await postApi('/api/indicators/'+name+'/toggle');
    await loadIndicators();
  } catch(e){}
}

// ── Desktop chart launcher ────────────────────────────────────────────────
let _launching = false;
async function launchDesktopChart() {
  const tk = _selectedLaunchTk;
  if (!tk) { appendCmdLog('Select a ticker below first'); return; }
  if (_launching) return;
  _launching = true;
  try {
    await api('/api/chart/'+tk);
    appendCmdLog('Desktop chart launched for '+tk);
  } catch(e) { appendCmdLog('Chart launch failed: '+e); }
  finally { _launching = false; }
}

function selectLaunchTk(tk) {
  _selectedLaunchTk = tk;
  document.querySelectorAll('.launch-tk').forEach(b =>
    b.classList.toggle('sel', b.textContent.trim()===tk));
}

function renderLauncherRow(tickers) {
  if (!_selectedLaunchTk && tickers.length) _selectedLaunchTk = tickers[0];
  document.getElementById('launcher-tk-row').innerHTML =
    tickers.map(t => {
      const s = allStatus[t] || {};
      const p = s.last_price;
      const v = s.vwap;
      const cls = p&&v ? (p>v?'price-up':'price-dn') : '';
      return `<button class="launch-tk ${cls} ${t===_selectedLaunchTk?'sel':''}"
                onclick="selectLaunchTk('${t}')">${t}${p?'<br><span style="font-size:9px;font-weight:400">$'+p.toFixed(2)+'</span>':''}</button>`;
    }).join('');
}

// ── Header: latency pills ─────────────────────────────────────────────────
function renderPills(tickers, status) {
  const mktOpen = isMarketOpen();
  document.getElementById('pill-row').innerHTML = tickers.map(t => {
    const s      = status[t] || {};
    const sec    = secsAgo(s.last_bar_time);
    const dotCls = sec!=null && sec<120 ? 'live' : 'dead';
    const latCls = sec==null ? '' : sec<120 ? 'ok' : sec<300 ? 'warn' : 'dead';
    const latStr = sec==null ? '—' : sec<60 ? sec+'s' : Math.floor(sec/60)+'m '+((sec%60)+'s');
    const timeStr = s.last_bar_time ? fmtT(s.last_bar_time) : '—';
    const p = s.last_price;
    // When market is closed, fall back to last parquet close (dimmed)
    const showClose = !mktOpen && !p && s.last_close;
    const pxHtml = p
      ? `<span style="color:${s.vwap?(p>s.vwap?'#22c55e':'#ef4444'):'#94a3b8'}">$${p.toFixed(2)}</span>`
      : showClose
        ? `<span style="color:#475569;font-style:italic" title="Last close">$${s.last_close.toFixed(2)}</span>`
        : '';
    return `<span class="tp">
      <span class="ldot ${dotCls}"></span>
      <b>${t}</b>
      ${pxHtml}
      <span class="lat ${latCls}">${mktOpen ? timeStr+' · '+latStr : 'closed'}</span>
      <span class="rm" onclick="event.stopPropagation();removeTicker('${t}')">&#10005;</span>
    </span>`;
  }).join('');
}

// ── Mini chart cards ──────────────────────────────────────────────────────
function buildCard(ticker, s) {
  const price = s.last_price;
  const prev  = allStatus[ticker]?.last_price;
  const dir   = price==null?'nc':(prev==null||price===prev?'nc':price>prev?'up':'dn');
  const arrow = dir==='up'?'▲':dir==='dn'?'▼':'';
  return `
<div class="chart-card" id="card-${ticker}" onclick="openModal('${ticker}')">
  <div class="cc-header">
    <span class="cc-ticker">${ticker}</span>
    <span class="cc-price ${dir}" id="px-${ticker}">$${fmt(price)} ${arrow}</span>
  </div>
  <div class="cc-chart" id="mchart-${ticker}"></div>
  <div class="cc-meta">
    <div class="cc-stat">VWAP<span id="vwap-${ticker}">$${fmt(s.vwap)}</span></div>
    <div class="cc-stat">W.VWAP<span id="wvwap-${ticker}">$${fmt(s.wvwap)}</span></div>
    <div class="cc-stat">ORB30<span id="orb30-${ticker}">${s.orb30_set?'$'+fmt(s.orb30_low)+'–$'+fmt(s.orb30_high):'Pending'}</span></div>
    <div class="cc-stat">Bars<span>${s.bars_today}</span></div>
  </div>
  <div class="cc-actions">
    <button class="cc-act blue" onclick="event.stopPropagation();selectLaunchTk('${ticker}');launchDesktopChart()">&#9112; Launch</button>
    <button class="cc-act" onclick="event.stopPropagation();openModal('${ticker}')">&#9633; Expand</button>
    <button class="cc-act green" id="hbtn-${ticker}" onclick="event.stopPropagation();pullHistory('${ticker}')">&#8595; Hist</button>
  </div>
  <div class="hist-status" id="hist-${ticker}"></div>
</div>`;
}

async function initMiniChart(ticker) {
  if (miniCharts[ticker]) return;
  const el = document.getElementById('mchart-'+ticker);
  if (!el) return;
  const c  = LightweightCharts.createChart(el, {...BASE_OPTS(100),
    handleScroll:{mouseWheel:false,pressedMouseMove:false},
    handleScale:{mouseWheel:false,pinch:false}});
  const cs = c.addCandlestickSeries(CANDLE_OPTS);
  const vs = c.addLineSeries(LINE('#60a5fa',1,false,false,false));
  const ws = c.addLineSeries(LINE('#a78bfa',1,true, false,false));
  miniCharts[ticker] = {chart:c, candle:cs, vwapS:vs, wvwapS:ws};
  new ResizeObserver(()=>c.applyOptions({width:el.clientWidth})).observe(el);
  try {
    let bars = await api('/api/bars/'+ticker+'?n=200');
    let isHistFallback = false;
    // Fallback to historical parquet bars when market is closed / live buffer empty
    if (!bars.length) {
      try { bars = await api('/api/history/'+ticker+'/bars?n=390'); isHistFallback = true; } catch(e2){}
    }
    if (bars.length) {
      cs.setData(bars.map(b=>({time:b.t,open:b.o,high:b.h,low:b.l,close:b.c})));
      vs.setData(bars.filter(b=>b.vwap!=null).map(b=>({time:b.t,value:b.vwap})));
      ws.setData(bars.filter(b=>b.wvwap!=null).map(b=>({time:b.t,value:b.wvwap})));
      c.timeScale().fitContent();
      // When no live price is available, show last bar's close in the card header
      if (isHistFallback && !allStatus[ticker]?.last_price) {
        const lastBar = bars[bars.length - 1];
        if (lastBar) {
          const pxEl = document.getElementById('px-'+ticker);
          if (pxEl) {
            pxEl.textContent = '$'+lastBar.c.toFixed(2);
            pxEl.className   = 'cc-price nc';
            pxEl.title       = 'Last close (market closed)';
          }
        }
      }
    }
    if (allMarkers[ticker]) cs.setMarkers(allMarkers[ticker]);
  } catch(e){}
}

// ── ORB price lines on mini charts ────────────────────────────────────────
const _orbLines = {};
function applyORB(ticker, h, l, set) {
  const mc = miniCharts[ticker];
  if (!mc || !set || !h || !l) return;
  const prev = _orbLines[ticker] || {};
  if (prev.hi===h && prev.lo===l) return;
  try{if(prev._h) mc.candle.removePriceLine(prev._h);}catch(e){}
  try{if(prev._l) mc.candle.removePriceLine(prev._l);}catch(e){}
  _orbLines[ticker] = {hi:h, lo:l,
    _h: mc.candle.createPriceLine({price:h,color:'#f59e0b',lineWidth:1,lineStyle:2,axisLabelVisible:true,title:'ORB Hi'}),
    _l: mc.candle.createPriceLine({price:l,color:'#f59e0b',lineWidth:1,lineStyle:2,axisLabelVisible:true,title:'ORB Lo'}),
  };
}

// ── Signal markers ────────────────────────────────────────────────────────
const SIG_MARKER = {
  ORB_30_RETEST_HIGH: {position:'belowBar',color:'#fbbf24',shape:'arrowUp',  text:'RT30↑'},
  ORB_30_RETEST_LOW:  {position:'aboveBar',color:'#fb923c',shape:'arrowDown',text:'RT30↓'},
  ORB_60_RETEST_HIGH: {position:'belowBar',color:'#f59e0b',shape:'arrowUp',  text:'RT60↑'},
  ORB_60_RETEST_LOW:  {position:'aboveBar',color:'#ea580c',shape:'arrowDown',text:'RT60↓'},
  MACD_CROSS_BULL:    {position:'belowBar',color:'#22d3ee',shape:'arrowUp',  text:'MACD↑'},
  MACD_CROSS_BEAR:    {position:'aboveBar',color:'#f43f5e',shape:'arrowDown',text:'MACD↓'},
  VWAP_CROSS_PDH:     {position:'belowBar',color:'#facc15',shape:'circle',   text:'PDH'},
  VWAP_CROSS_PDL:     {position:'aboveBar',color:'#e879f9',shape:'circle',   text:'PDL'},
};
function addMarker(ticker, time_s, signal_type) {
  const def = SIG_MARKER[signal_type]; if(!def) return;
  const marker = {time:time_s,...def};
  if (!allMarkers[ticker]) allMarkers[ticker] = [];
  allMarkers[ticker].push(marker);
  allMarkers[ticker].sort((a,b)=>a.time-b.time);
  const mc = miniCharts[ticker];
  if (mc) mc.candle.setMarkers(allMarkers[ticker]);
  if (modalTicker===ticker && modalChart.candle) modalChart.candle.setMarkers(allMarkers[ticker]);
}

// ── Modal chart ───────────────────────────────────────────────────────────
const _morbLines = {};
async function openModal(ticker) {
  modalTicker = ticker;
  const s = allStatus[ticker] || {};
  document.getElementById('m-ticker').textContent = ticker;
  document.getElementById('m-price').textContent  = '$'+fmt(s.last_price);
  document.getElementById('m-price').className    = 'modal-px '+(s.last_price&&s.vwap?(s.last_price>s.vwap?'up':'dn'):'nc');
  document.getElementById('m-vwap').textContent   = '$'+fmt(s.vwap);
  document.getElementById('m-wvwap').textContent  = '$'+fmt(s.wvwap);
  document.getElementById('m-orb-h').textContent  = s.orb30_set?'$'+fmt(s.orb30_high):'Pending';
  document.getElementById('m-orb-l').textContent  = s.orb30_set?'$'+fmt(s.orb30_low) :'Pending';
  document.getElementById('m-bars').textContent   = s.bars_today||'0';
  document.getElementById('modal').classList.add('open');

  const el = document.getElementById('modal-chart');
  el.innerHTML = '';
  const c  = LightweightCharts.createChart(el, BASE_OPTS(el.clientHeight||400));
  const cs = c.addCandlestickSeries(CANDLE_OPTS);
  const vs = c.addLineSeries(LINE('#60a5fa',1,false,false,true,'VWAP'));
  const ws = c.addLineSeries(LINE('#a78bfa',1,true, false,true,'WVWAP'));
  const s9 = c.addLineSeries(LINE('#4fc3f7',1,false,false,true,'SMA9'));
  const s20= c.addLineSeries(LINE('#81c784',1,false,false,true,'SMA20'));
  const s50= c.addLineSeries(LINE('#ffb74d',1,false,false,true,'SMA50'));
  const s200=c.addLineSeries(LINE('#ef5350',1,true, false,true,'SMA200'));
  Object.assign(modalChart,{chart:c,candle:cs,vwapS:vs,wvwapS:ws,sma9:s9,sma20:s20,sma50:s50,sma200:s200});
  new ResizeObserver(()=>c.applyOptions({width:el.clientWidth})).observe(el);

  let hb=[],lb=[];
  try { hb = await api('/api/history/'+ticker+'/bars?n=600'); } catch(e){}
  try { lb = await api('/api/bars/'+ticker+'?n=200'); } catch(e){}
  const merged = mergeBars(hb, lb);
  if (merged.length) {
    cs.setData(merged.map(b=>({time:b.t,open:b.o,high:b.h,low:b.l,close:b.c})));
    vs.setData(lb.filter(b=>b.vwap!=null).map(b=>({time:b.t,value:b.vwap})));
    ws.setData(lb.filter(b=>b.wvwap!=null).map(b=>({time:b.t,value:b.wvwap})));
    s9.setData(computeSMA(merged,9)); s20.setData(computeSMA(merged,20));
    s50.setData(computeSMA(merged,50)); s200.setData(computeSMA(merged,200));
    c.timeScale().fitContent();
  }
  if (s.orb30_set && s.orb30_high && s.orb30_low) {
    _morbLines._h = cs.createPriceLine({price:s.orb30_high,color:'#fbbf24',lineWidth:1,lineStyle:2,axisLabelVisible:true,title:'ORB30 Hi'});
    _morbLines._l = cs.createPriceLine({price:s.orb30_low, color:'#fb923c',lineWidth:1,lineStyle:2,axisLabelVisible:true,title:'ORB30 Lo'});
  }
  if (s.orb60_set && s.orb60_high && s.orb60_low) {
    _morbLines._h60 = cs.createPriceLine({price:s.orb60_high,color:'#f59e0b',lineWidth:1,lineStyle:3,axisLabelVisible:true,title:'ORB60 Hi'});
    _morbLines._l60 = cs.createPriceLine({price:s.orb60_low, color:'#ea580c',lineWidth:1,lineStyle:3,axisLabelVisible:true,title:'ORB60 Lo'});
  }
  if (allMarkers[ticker]) cs.setMarkers(allMarkers[ticker]);
}

function closeModal(e) {
  if (e && e.target!==document.getElementById('modal')) return;
  document.getElementById('modal').classList.remove('open');
  modalTicker = null;
  if (modalChart.chart) { try{modalChart.chart.remove();}catch(e){} }
  Object.keys(modalChart).forEach(k=>delete modalChart[k]);
  Object.keys(_morbLines).forEach(k=>delete _morbLines[k]);
}
document.addEventListener('keydown', e=>{if(e.key==='Escape')closeModal();});

// ── Stream chips ──────────────────────────────────────────────────────────
async function updateStreamChips() {
  try {
    const st = await api('/api/stream/status');
    const streaming = new Set(st.streaming);
    const engine    = st.engine || [];
    document.getElementById('stream-chips').innerHTML =
      engine.map(t=>`<span class="s-chip ${streaming.has(t)?'ok':'no'}">${t}</span>`).join('');
  } catch(e){}
}

// ── Ticker management ─────────────────────────────────────────────────────
async function addTicker() {
  const inp = document.getElementById('new-tk');
  const tk  = (inp.value||'').trim().toUpperCase();
  if (!tk) return; inp.value='';
  try { await api('/api/tickers/add?ticker='+tk); await refreshStatus(); } catch(e){ alert('Could not add '+tk); }
}
async function removeTicker(tk) {
  if (!confirm('Remove '+tk+' from stream?')) return;
  try {
    await api('/api/tickers/remove?ticker='+tk);
    if(miniCharts[tk]){try{miniCharts[tk].chart.remove();}catch(e){};delete miniCharts[tk];}
    delete allMarkers[tk]; delete _orbLines[tk];
    const card = document.getElementById('card-'+tk); if(card) card.remove();
    if (_selectedLaunchTk===tk) _selectedLaunchTk = null;
    await refreshStatus();
  } catch(e){ alert('Could not remove '+tk); }
}

// ── Telegram ──────────────────────────────────────────────────────────────
async function loadTelegramStatus() {
  try {
    const s = await api('/api/telegram/status');
    const el = document.getElementById('tg-status');
    if (!el) return;
    if (s.configured && s.chat_count > 0) {
      const labels = Object.values(s.chats).join(', ');
      el.innerHTML = `<span style="color:#22c55e">&#9679;</span> Telegram OK &mdash; ${s.chat_count} chat${s.chat_count!==1?'s':''}: ${labels}`;
    } else if (s.configured) {
      el.innerHTML = `<span style="color:#f59e0b">&#9679;</span> Token OK but no chats configured`;
    } else {
      el.innerHTML = `<span style="color:#ef4444">&#9679;</span> Telegram not configured`;
    }
    // Also populate the channel routing table at the bottom
    await loadTelegramChannelInfo(s);
  } catch(e) {}
}

async function loadTelegramChannelInfo(s) {
  const tbody = document.getElementById('tg-channel-tbody');
  if (!tbody) return;
  if (!s) {
    try { s = await api('/api/telegram/status'); } catch(e) { return; }
  }
  const configured = s.configured;
  const tokenPfx   = s.token_prefix || '—';
  const chats       = s.chats || {};   // {chat_id: label}

  // Known channel descriptions
  const CHAN_DESC = {
    'Trading_Alerts': 'All stock alert signals (ORB, VWAP, MACD, PDH/PDL) — entire trading channel',
    'admin':          'Owner personal notifications — same signals as Trading_Alerts',
    'bart':           'Bart\'s personal feed',
  };
  const KNOWN_IDS = {
    '-1003773521526': 'Trading_Alerts Channel',
    '1690197800':     'Admin / Owner DM',
  };

  if (!configured) {
    tbody.innerHTML = `<tr><td colspan="5" style="color:#ef4444;padding:8px 10px">
      &#9888; Alerts bot token not configured. Check sn_config.py or SN_TG_TOKEN env var.
    </td></tr>`;
    return;
  }

  if (Object.keys(chats).length === 0) {
    tbody.innerHTML = `<tr><td colspan="5" style="color:#f59e0b;padding:8px 10px">
      &#9888; Bot token OK (${tokenPfx}) but no chat IDs configured in TELEGRAM_CHATS.
    </td></tr>`;
    return;
  }

  tbody.innerHTML = Object.entries(chats).map(([cid, label]) => {
    const knownName = KNOWN_IDS[cid] || (cid.startsWith('-') ? 'Channel' : 'Personal DM');
    const desc      = CHAN_DESC[label] || 'Receives all enabled StockNotify signals';
    const isChannel = cid.startsWith('-');
    const typeLabel = isChannel
      ? '<span style="font-size:9px;font-weight:700;background:#0c1a2e;border:1px solid #1d4ed8;border-radius:3px;padding:1px 5px;color:#60a5fa">Channel</span>'
      : '<span style="font-size:9px;font-weight:700;background:#0f2010;border:1px solid #14532d;border-radius:3px;padding:1px 5px;color:#4ade80">DM</span>';
    return `<tr style="border-bottom:1px solid #0f172a">
      <td style="padding:6px 10px">
        <div style="font-weight:700;color:#38bdf8;font-size:11px">&#9992; Alerts Bot</div>
        <div style="font-size:9px;color:#475569;font-variant-numeric:tabular-nums">${tokenPfx}</div>
      </td>
      <td style="padding:6px 10px">
        <div style="font-weight:700;color:#e2e8f0">${label}</div>
        <div style="font-size:9px;color:#64748b">${knownName}</div>
      </td>
      <td style="padding:6px 10px;font-size:10px;font-family:monospace;color:#64748b">${cid} ${typeLabel}</td>
      <td style="padding:6px 10px;font-size:10px;color:#64748b;max-width:260px">${desc}</td>
      <td style="padding:6px 10px;text-align:center">
        <span style="color:#22c55e;font-size:11px;font-weight:700">&#9679; Active</span>
      </td>
    </tr>`;
  }).join('');
}

async function sendTestAllTickers() {
  const btn = document.getElementById('tg-mass-test-btn');
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Sending…'; }
  const res = document.getElementById('tg-ticker-results');
  if (res) res.innerHTML = '<span style="color:#f59e0b;font-size:10px">Sending test messages…</span>';
  try {
    const r = await postApi('/api/telegram/test-all-tickers');
    if (res) {
      res.innerHTML = (r.results || []).map(item => {
        const ok = item.ok;
        const chOk = (item.channels || []).filter(c=>c.ok).map(c=>c.label).join(', ');
        const chFail = (item.channels || []).filter(c=>!c.ok).map(c=>c.label).join(', ');
        const detail = ok
          ? `<span style="font-size:8px;color:#22c55e">→ ${chOk || '✓'}</span>`
          : `<span style="font-size:8px;color:#ef4444">✗ ${chFail||item.error||'failed'}</span>`;
        return `<span style="display:inline-flex;flex-direction:column;align-items:center;background:${ok?'#052e16':'#2d1111'};border:1px solid ${ok?'#14532d':'#7f1d1d'};border-radius:6px;padding:4px 10px;gap:2px">
          <span style="font-weight:800;font-size:11px;color:${ok?'#4ade80':'#f87171'}">${item.ticker}</span>
          ${detail}
        </span>`;
      }).join('');
    }
    const nOk = (r.results||[]).filter(x=>x.ok).length;
    appendCmdLog('Test sent: '+nOk+'/'+(r.results||[]).length+' tickers delivered to Telegram');
  } catch(e) {
    if (res) res.innerHTML = '<span style="color:#ef4444;font-size:10px">Test failed: '+e+'</span>';
    appendCmdLog('Test-all-tickers failed: '+e);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '✈ Test Send — All Tickers'; }
  }
}

async function testTelegram() {
  const btn = document.getElementById('tg-test-btn');
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Sending…'; }
  try {
    const r = await postApi('/api/telegram/test');
    if (r.ok) {
      const sent = (r.results||[]).filter(x=>x.ok).map(x=>x.label).join(', ');
      appendCmdLog(`✅ Telegram test sent to: ${sent}`);
    } else {
      appendCmdLog(`❌ Telegram test failed: ${r.error || JSON.stringify(r.results)}`);
    }
  } catch(e) { appendCmdLog('Telegram test error: '+e); }
  finally {
    if (btn) { btn.disabled = false; btn.textContent = '✈ Test Telegram'; }
    await loadTelegramStatus();
  }
}

// ── Bulk controls ─────────────────────────────────────────────────────────
async function pullAllHistory() {
  appendCmdLog('Pulling history for all tickers…');
  const tickers = Object.keys(allStatus);
  for (const tk of tickers) {
    try { await api('/api/history/'+tk+'/pull'); _pollHistStatus(tk); } catch(e){}
  }
  appendCmdLog('Pull started for: '+tickers.join(', '));
}

async function reloadStream() {
  try {
    await api('/api/stream/reload');
    appendCmdLog('Stream reload requested');
    setTimeout(updateStreamChips, 5000);
  } catch(e){ appendCmdLog('Stream reload failed: '+e); }
}

async function restartServer() {
  const btn = document.getElementById('restart-btn');
  if (btn.disabled) return;
  btn.disabled = true;
  btn.textContent = '⏳ Restarting…';
  btn.style.color = '#f59e0b';
  btn.style.borderColor = '#78350f';
  document.getElementById('sdot').className = 'sdot dead';
  document.getElementById('lupd').textContent = 'Restarting…';

  try {
    await postApi('/api/self-restart');
  } catch(e) { /* server dies mid-response — expected */ }

  // Count down while waiting for server to come back up
  let secs = 0;
  const timer = setInterval(() => {
    secs++;
    btn.textContent = '⏳ ' + secs + 's…';
  }, 1000);

  // Poll /health until it responds (max 30s)
  const deadline = Date.now() + 30000;
  const sep = TOKEN ? '?token='+TOKEN : '';
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 1500));
    try {
      const r = await fetch('/health' + sep);
      if (r.ok) break;
    } catch(e) {}
  }

  clearInterval(timer);
  btn.disabled = false;
  btn.textContent = '↺ Restart';
  btn.style.color = '#a78bfa';
  btn.style.borderColor = '#7c3aed';
  appendCmdLog('Server restarted in ' + secs + 's — reconnecting…');
  // WebSocket will auto-reconnect via its own onclose handler
}

// ── Command executor ──────────────────────────────────────────────────────
function setCmd(v) { document.getElementById('cmd-input').value = v; document.getElementById('cmd-input').focus(); }

async function executeCmd() {
  const raw = document.getElementById('cmd-input').value.trim();
  if (!raw) return;
  document.getElementById('cmd-input').value = '';
  const cmd = raw.toLowerCase();
  appendCmdLog('> '+raw);

  if (cmd.startsWith('add ')) {
    const tk = raw.slice(4).trim().toUpperCase();
    try { await api('/api/tickers/add?ticker='+tk); await refreshStatus(); appendCmdLog('Added '+tk); }
    catch(e){ appendCmdLog('Error adding '+tk+': '+e); }

  } else if (cmd.startsWith('remove ')) {
    const tk = raw.slice(7).trim().toUpperCase();
    await removeTicker(tk);

  } else if (cmd.startsWith('pull history ')) {
    const tk = raw.slice(13).trim().toUpperCase();
    try { await api('/api/history/'+tk+'/pull'); _pollHistStatus(tk); appendCmdLog('Pulling history for '+tk); }
    catch(e){ appendCmdLog('Error: '+e); }

  } else if (cmd === 'pull all' || cmd === 'pull all history') {
    await pullAllHistory();

  } else if (cmd === 'reload' || cmd === 'reload stream') {
    await reloadStream();

  } else if (cmd === 'status') {
    const st = await api('/api/stream/status');
    appendCmdLog('Streaming ('+st.streaming.length+'): '+st.streaming.join(', ')||'none');
    appendCmdLog('Engine tickers: '+st.engine.join(', '));

  } else if (cmd === 'help') {
    document.getElementById('help-panel').classList.toggle('open');
    appendCmdLog('Help panel toggled');

  } else {
    // Try sending to Claude natural language endpoint
    try {
      const r = await postApi('/api/cmd', {cmd: raw});
      appendCmdLog(r.result || r.error || 'Done');
    } catch(e) {
      appendCmdLog('Unknown command. Type "help" for available commands.');
    }
  }
}

function appendCmdLog(msg) {
  const el = document.getElementById('cmd-log');
  const line = document.createElement('div');
  line.textContent = new Date().toLocaleTimeString()+' '+msg;
  el.insertBefore(line, el.firstChild);
  while(el.children.length > 10) el.removeChild(el.lastChild);
}

// ── History pull helpers ──────────────────────────────────────────────────
const _histPollers = {};

async function pullHistory(ticker) {
  const btn = document.getElementById('hbtn-'+ticker);
  if (btn) { btn.disabled=true; btn.textContent='⏳'; }
  try { await api('/api/history/'+ticker+'/pull'); _pollHistStatus(ticker); }
  catch(e){ if(btn){btn.disabled=false;btn.textContent='↓ Hist';} }
}

function _pollHistStatus(ticker) {
  if (_histPollers[ticker]) clearInterval(_histPollers[ticker]);
  _histPollers[ticker] = setInterval(async () => {
    try {
      const info = await api('/api/history/'+ticker+'/status');
      _applyHistStatus(ticker, info);
      if (info.status!=='running') clearInterval(_histPollers[ticker]);
    } catch(e){ clearInterval(_histPollers[ticker]); }
  }, 2000);
}

function _applyHistStatus(ticker, info) {
  const el  = document.getElementById('hist-'+ticker);
  const btn = document.getElementById('hbtn-'+ticker);
  if (!el) return;
  const st = info.status || 'idle';
  if (st==='running') {
    el.textContent='⏳ '+(info.progress||'Downloading…'); el.style.color='#f59e0b';
    if(btn){btn.disabled=true;btn.textContent='⏳';}
  } else if (st==='done') {
    el.textContent='✓ '+((info.bars||0).toLocaleString())+' bars'; el.style.color='#34d399';
    if(btn){btn.disabled=false;btn.textContent='↓ Hist';}
  } else if (st==='error') {
    el.textContent='✗ '+(info.error||'Error').slice(0,60); el.style.color='#f87171';
    if(btn){btn.disabled=false;btn.textContent='↓ Retry';}
  } else {
    el.textContent=''; if(btn){btn.disabled=false;btn.textContent='↓ Hist';}
  }
}

async function loadAllHistStatus() {
  try {
    const all = await api('/api/history/all');
    let done=0, total=0;
    for (const [tk,info] of Object.entries(all)) {
      _applyHistStatus(tk, info); total++;
      if (info.status==='done') done++;
      if (info.status==='running') _pollHistStatus(tk);
    }
    if (total) document.getElementById('hist-summary').textContent = `History: ${done}/${total} tickers`;
  } catch(e){}
}

// ── Signal log ────────────────────────────────────────────────────────────
let sigRows = [];
function renderSignalLog() {
  const el = document.getElementById('sig-log');
  if (!sigRows.length) { el.innerHTML='<div class="no-sig">No signals yet today</div>'; return; }
  el.innerHTML = sigRows.slice(0,30).map(a =>
    `<div class="sig-row">
      <span class="sig-tk" onclick="openModal('${a.ticker}')" style="cursor:pointer">${a.ticker}</span>
      <span class="sig-badge ${a.signal_type}">${a.signal_type.replace(/_/g,' ')}</span>
      <span class="sig-px">$${fmt(a.price)}</span>
      <span class="sig-ts">${fmtT(a.bar_time||a.ts)}</span>
    </div>`
  ).join('');
}

// ── WebSocket ─────────────────────────────────────────────────────────────
let ws;
function connectWS() {
  const proto = location.protocol==='https:' ? 'wss' : 'ws';
  const host  = location.host || '127.0.0.1:8436';
  const sep   = TOKEN ? '?token='+TOKEN : '';
  ws = new WebSocket(proto+'://'+host+'/ws'+sep);
  ws.onopen  = () => { document.getElementById('sdot').className='sdot live';
                       document.getElementById('lupd').textContent='Live'; };
  ws.onclose = () => { document.getElementById('sdot').className='sdot dead'; setTimeout(connectWS,3000); };
  ws.onerror = () => ws.close();
  ws.onmessage = e => {
    const msg = JSON.parse(e.data);
    if (msg.type==='bar') {
      const {ticker,t,o,h,l,c,v,vwap,wvwap} = msg;
      // Update mini chart
      const mc = miniCharts[ticker];
      if (mc) {
        mc.candle.update({time:t,open:o,high:h,low:l,close:c});
        if(vwap !=null) mc.vwapS.update({time:t,value:vwap});
        if(wvwap!=null) mc.wvwapS.update({time:t,value:wvwap});
      }
      // Update modal if open
      if (modalTicker===ticker && modalChart.candle) {
        modalChart.candle.update({time:t,open:o,high:h,low:l,close:c});
        if(vwap !=null) modalChart.vwapS.update({time:t,value:vwap});
        if(wvwap!=null) modalChart.wvwapS.update({time:t,value:wvwap});
      }
      // Update card price label
      const pxEl=document.getElementById('px-'+ticker);
      if (pxEl) {
        const prev=allStatus[ticker]?.last_price;
        const dir=prev==null||c===prev?'nc':c>prev?'up':'dn';
        pxEl.textContent='$'+c.toFixed(2)+(dir==='up'?' ▲':dir==='dn'?' ▼':'');
        pxEl.className='cc-price '+dir;
      }
      const vEl=document.getElementById('vwap-'+ticker),wEl=document.getElementById('wvwap-'+ticker);
      if(vEl&&vwap !=null) vEl.textContent='$'+vwap.toFixed(2);
      if(wEl&&wvwap!=null) wEl.textContent='$'+wvwap.toFixed(2);
      // Update state + header latency pill
      if(!allStatus[ticker]) allStatus[ticker]={};
      allStatus[ticker].last_price=c; allStatus[ticker].vwap=vwap; allStatus[ticker].wvwap=wvwap;
      allStatus[ticker].last_bar_time=new Date(t*1000).toISOString();
      // Refresh launch row price label
      const lb = document.querySelector(`.launch-tk:nth-child(${Object.keys(allStatus).sort().indexOf(ticker)+1})`);
      if (lb) lb.innerHTML=ticker+'<br><span style="font-size:9px;font-weight:400">$'+c.toFixed(2)+'</span>';
    }
    if (msg.type==='signal') {
      const {ticker,signal_type,price,bar_time} = msg;
      const ts = bar_time?new Date(bar_time).getTime()/1000:Math.floor(Date.now()/1000);
      addMarker(ticker, Math.floor(ts), signal_type);
      sigRows.unshift({ticker,signal_type,price,bar_time});
      renderSignalLog();
    }
  };
}

// ── Status polling ────────────────────────────────────────────────────────
async function refreshStatus() {
  try {
    const status = await api('/api/status');
    // When market is closed, merge last_close from coverage so pills show last known price
    if (!isMarketOpen()) {
      try {
        const cov = await api('/api/coverage');
        for (const tk of Object.keys(cov)) {
          if (status[tk] && !status[tk].last_price && cov[tk].last_close != null)
            status[tk].last_close = cov[tk].last_close;
        }
      } catch(e) {}
    }
    const tickers = Object.keys(status).sort();
    allStatus = status;
    renderPills(tickers, status);
    renderLauncherRow(tickers);

    const grid = document.getElementById('chart-grid');
    for (const tk of tickers) {
      if (!document.getElementById('card-'+tk)) {
        const div=document.createElement('div'); div.innerHTML=buildCard(tk,status[tk]);
        grid.appendChild(div.firstElementChild);
      } else {
        const s=status[tk];
        const vEl=document.getElementById('vwap-'+tk),wEl=document.getElementById('wvwap-'+tk),oEl=document.getElementById('orb30-'+tk);
        if(vEl) vEl.textContent='$'+fmt(s.vwap);
        if(wEl) wEl.textContent='$'+fmt(s.wvwap);
        if(oEl) oEl.textContent=s.orb30_set?'$'+fmt(s.orb30_low)+'–$'+fmt(s.orb30_high):'Pending';
        applyORB(tk, s.orb30_high, s.orb30_low, s.orb30_set);
      }
      await initMiniChart(tk);
    }
    // Remove stale cards
    for (const card of grid.querySelectorAll('.chart-card')) {
      const tk=card.id.replace('card-',''); if(!status[tk]) card.remove();
    }
    document.getElementById('lupd').textContent='Updated '+new Date().toLocaleTimeString();
  } catch(e) { document.getElementById('sdot').className='sdot dead'; }
}

async function refreshAlerts() {
  try {
    const alerts = await api('/api/alerts');
    if (alerts.length && !sigRows.length) {
      sigRows = alerts.map(a=>({ticker:a.ticker,signal_type:a.type,price:a.price,bar_time:a.ts}));
      renderSignalLog();
    }
  } catch(e){}
}

// ── Market hours helper ───────────────────────────────────────────────────
function isMarketOpen() {
  const etStr = new Date().toLocaleString('en-US', {timeZone: 'America/New_York'});
  const et    = new Date(etStr);
  const day   = et.getDay();
  if (day === 0 || day === 6) return false;
  const mins  = et.getHours() * 60 + et.getMinutes();
  return mins >= 570 && mins < 960;
}

function getMarketStatus() {
  const etStr = new Date().toLocaleString('en-US', {timeZone: 'America/New_York'});
  const et    = new Date(etStr);
  const day   = et.getDay();
  const mins  = et.getHours() * 60 + et.getMinutes();
  const isWeekend = day === 0 || day === 6;

  let label, session, color, bg, nextLabel, nextMins;

  if (isWeekend) {
    label = 'MARKET CLOSED'; session = 'Weekend';
    color = '#475569'; bg = '#0a0e17';
    const daysToMon = day === 6 ? 2 : 1;
    nextMins  = daysToMon * 24 * 60 + 570 - mins;
    nextLabel = 'Opens Mon in';
  } else if (mins < 240) {                     // midnight – 4:00 ET
    label = 'MARKET CLOSED'; session = 'Overnight';
    color = '#475569'; bg = '#0a0e17';
    nextMins = 240 - mins; nextLabel = 'Pre-market in';
  } else if (mins < 570) {                     // 4:00 – 9:30 ET
    label = 'PRE-MARKET'; session = 'Pre-Market';
    color = '#f59e0b'; bg = '#1a1100';
    nextMins = 570 - mins; nextLabel = 'Opens in';
  } else if (mins < 960) {                     // 9:30 – 16:00 ET
    label = 'MARKET OPEN'; session = 'Regular Session';
    color = '#22c55e'; bg = '#021008';
    nextMins = 960 - mins; nextLabel = 'Closes in';
  } else if (mins < 1200) {                    // 16:00 – 20:00 ET
    label = 'AFTER HOURS'; session = 'After Hours';
    color = '#818cf8'; bg = '#0d0e1f';
    nextMins = 1200 - mins; nextLabel = 'AH ends in';
  } else {                                     // 20:00+ ET
    label = 'MARKET CLOSED'; session = 'Overnight';
    color = '#475569'; bg = '#0a0e17';
    nextMins = (240 + 24 * 60) - mins; nextLabel = 'Pre-market in';
  }

  const h = Math.floor(Math.abs(nextMins) / 60);
  const m = Math.abs(nextMins) % 60;
  const countdown = h > 0 ? `${h}h ${m}m` : `${m}m`;
  return { label, session, color, bg, nextLabel, countdown };
}

function updateMarketBanner() {
  const banner = document.getElementById('mkt-banner');
  if (!banner) return;
  const { label, session, color, bg, nextLabel, countdown } = getMarketStatus();
  const etTime = new Date().toLocaleTimeString('en-US', {
    timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', second: '2-digit'
  });
  banner.style.background   = bg;
  banner.style.borderBottomColor = color + '55';
  banner.innerHTML = `
    <span class="mkt-dot" style="background:${color};box-shadow:0 0 7px ${color}99"></span>
    <span class="mkt-status" style="color:${color}">${label}</span>
    <span class="mkt-session">${session}</span>
    <span class="mkt-countdown">${nextLabel} <b style="color:${color}">${countdown}</b></span>
    <span class="mkt-et">ET&nbsp;${etTime}</span>
  `;
}

async function pullCovTicker(tk) {
  try {
    await api('/api/history/'+tk+'/pull');
    _pollHistStatus(tk);
    // Expire coverage cache entry so next loadCoverage shows fresh state
    appendCmdLog('Pulling today\'s data for '+tk);
  } catch(e) { appendCmdLog('Pull failed for '+tk+': '+e); }
}

// ── Coverage table ────────────────────────────────────────────────────────
async function loadCoverage() {
  try {
    const data = await api('/api/coverage');
    const tickers = Object.keys(data).sort();
    const tbody = document.getElementById('cov-tbody');
    if (!tickers.length) { tbody.innerHTML='<tr><td colspan="11" style="color:#374151;padding:8px">No tickers tracked</td></tr>'; return; }

    const mktOpen = isMarketOpen();
    tbody.innerHTML = tickers.map(tk => {
      const d = data[tk];

      // Stream status — CLOSED (after-hours) vs STALE (market open but no data) vs LIVE
      const isLive   = d.streaming && d.latency_s != null && d.latency_s < 150;
      const dotCls   = isLive ? 'live' : 'dead';
      let streamTxt, streamClr;
      if (!d.streaming)    { streamTxt='OFF';    streamClr='#475569'; }
      else if (isLive)     { streamTxt='LIVE';   streamClr='#22c55e'; }
      else if (!mktOpen)   { streamTxt='CLOSED'; streamClr='#475569'; }
      else                 { streamTxt='STALE';  streamClr='#f59e0b'; }

      // Latency
      const sec = d.latency_s;
      let latTxt = '—', latCls = '';
      if (sec != null) {
        latTxt = sec < 60 ? sec+'s ago' : Math.floor(sec/60)+'m '+(sec%60)+'s';
        latCls = sec < 120 ? 'ok' : sec < 300 ? 'warn' : 'dead';
      }

      // Price — live price when streaming, fallback to last parquet close after hours
      const px = d.last_price;
      const lastClose = d.last_close;
      const vwap = d.vwap;
      let pxTxt, pxDir, pxStyle='';
      if (px != null) {
        pxDir  = vwap ? (px > vwap ? 'up' : 'dn') : 'nc';
        pxTxt  = '$'+px.toFixed(2);
      } else if (lastClose != null) {
        pxDir  = 'nc';
        pxTxt  = '$'+lastClose.toFixed(2);
        pxStyle= 'opacity:.55';   // dimmed to signal it's yesterday's close
      } else {
        pxDir = 'nc'; pxTxt = '—';
      }
      // "vs VWAP" — only meaningful with live price
      const pvPct = (px != null) ? d.pct_vs_vwap : null;
      const pvTxt = pvPct != null ? (pvPct>0?'+':'')+pvPct.toFixed(2)+'%' : '—';
      const pvDir = pvPct != null ? (pvPct>0?'up':'dn') : '';

      // Today bars
      const todayBars = d.bars_today||0;
      const todayTxt  = todayBars ? todayBars.toLocaleString() : '—';

      // Historical
      const histBars = d.hist_bars||0;
      const histTxt  = histBars ? histBars.toLocaleString() : '—';
      const firstD   = d.hist_first_date || '—';
      const lastD    = d.hist_last_date  || '—';
      const pct      = d.hist_pct || 0;
      const pctColor = pct >= 95 ? '#22c55e' : pct >= 80 ? '#f59e0b' : '#ef4444';

      // Pull-today button when parquet is behind today
      const pullBtn = d.needs_pull
        ? `<button onclick="event.stopPropagation();pullCovTicker('${tk}')" title="Pull today's bars" style="margin-left:5px;font-size:8px;background:#1e293b;color:#f59e0b;border:1px solid #78350f;border-radius:3px;padding:1px 5px;cursor:pointer;vertical-align:middle">&#8595; Pull</button>`
        : '';

      // "close" label when showing last parquet close
      const closeLabel = (px == null && lastClose != null)
        ? ' <span style="font-size:8px;color:#475569">(close)</span>' : '';

      return `<tr>
        <td><span class="cov-sym">${tk}</span></td>
        <td>
          <span style="display:inline-flex;align-items:center;gap:5px">
            <span class="cov-dot ${dotCls}"></span>
            <span style="font-size:10px;font-weight:700;color:${streamClr}">${streamTxt}</span>
          </span>
        </td>
        <td><span class="cov-lat ${latCls}">${latTxt}</span></td>
        <td style="text-align:right"><span class="cov-px ${pxDir}" style="${pxStyle}">${pxTxt}</span>${closeLabel}</td>
        <td style="text-align:right"><span class="cov-vwap-pct ${pvDir}">${pvTxt}</span></td>
        <td style="text-align:right"><span class="cov-date">$${vwap?vwap.toFixed(2):'—'}</span></td>
        <td style="text-align:right"><span class="cov-date">$${d.wvwap?d.wvwap.toFixed(2):'—'}</span></td>
        <td style="text-align:right"><span class="cov-today">${todayTxt}</span></td>
        <td style="text-align:right"><span class="cov-hist">${histTxt}</span></td>
        <td><span class="cov-date">${firstD} → ${lastD}</span>${pullBtn}</td>
        <td>
          <span class="cov-bar-wrap">
            <span class="cov-bar-fill" style="width:${Math.min(pct,100)}%;background:${pctColor}"></span>
          </span>
          <span class="cov-pct-lbl" style="color:${pctColor}">${pct.toFixed(1)}%</span>
          <span class="cov-date" style="margin-left:2px">(${(d.hist_expected_days||0).toLocaleString()}d exp)</span>
        </td>
      </tr>`;
    }).join('');

    document.getElementById('cov-upd').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch(e) {
    const tbody = document.getElementById('cov-tbody');
    if (tbody) tbody.innerHTML = '<tr><td colspan="11" style="color:#475569;padding:8px">Could not load coverage data</td></tr>';
  }
}

// ── Signal Matrix ──────────────────────────────────────────────────────────
async function loadSignalMatrix() {
  try {
    const [data, inds] = await Promise.all([
      api('/api/signal-matrix'),
      api('/api/indicators'),
    ]);
    if (!data || !inds) return;

    const tickers = data.tickers || [];
    const matrix  = data.matrix  || {};

    // Render ticker pills
    const pillCont = document.getElementById('mx-tk-pills');
    if (pillCont) {
      pillCont.innerHTML = tickers.map(tk =>
        `<span class="mx-tk-pill">${tk}<span class="rm" onclick="removeMatrixTicker('${tk}')">&#x2715;</span></span>`
      ).join('');
    }

    // Build header row
    const thead = document.getElementById('mx-thead');
    if (thead) {
      thead.innerHTML = `<tr>
        <th class="ind-col">Indicator</th>
        <th class="tf-col">TF</th>
        ${tickers.map(tk => `<th style="min-width:90px">${tk}</th>`).join('')}
      </tr>`;
    }

    // Build body rows using the ordered indicators array from /api/indicators
    const tbody = document.getElementById('mx-tbody');
    if (!tbody) return;
    if (!inds || inds.length === 0) {
      tbody.innerHTML = `<tr><td colspan="${2+tickers.length}" style="color:#374151;padding:8px;font-style:italic">No indicators configured</td></tr>`;
      return;
    }

    tbody.innerHTML = inds.map(ind => {
      const hits  = matrix[ind.name] || {};
      const cells = tickers.map(tk => {
        const h = hits[tk];
        if (!h) return `<td style="text-align:center"><span class="mx-miss">—</span></td>`;
        return `<td style="text-align:center">
          <div class="mx-hit" style="background:${ind.color}1a;border:1px solid ${ind.color}55">
            <span class="mx-hit-time" style="color:${ind.color}">${h.last_time}</span>
            <span class="mx-hit-cnt">${h.count}&times;</span>
          </div>
        </td>`;
      }).join('');
      return `<tr>
        <td class="ind-col" style="color:${ind.color}">${ind.label}</td>
        <td class="tf-col">${ind.tf||''}</td>
        ${cells}
      </tr>`;
    }).join('');

    // Update timestamp chip
    const upd = document.getElementById('mx-upd');
    if (upd) upd.textContent = 'upd ' + new Date().toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
  } catch(e) {
    const tbody = document.getElementById('mx-tbody');
    if (tbody) tbody.innerHTML = `<tr><td colspan="4" style="color:#374151;padding:8px;font-style:italic">Could not load matrix</td></tr>`;
  }
}

async function addMatrixTicker() {
  const inp = document.getElementById('mx-new-tk');
  const tk  = (inp ? inp.value : '').trim().toUpperCase();
  if (!tk) return;
  inp.value = '';
  await postApi('/api/matrix/tickers/add', {ticker: tk});
  await loadSignalMatrix();
}

async function removeMatrixTicker(tk) {
  await postApi('/api/matrix/tickers/remove', {ticker: tk});
  await loadSignalMatrix();
}

// ── Boot ──────────────────────────────────────────────────────────────────
(async () => {
  updateMarketBanner();
  connectWS();
  await loadSignalMatrix();
  await loadCoverage();
  await loadIndicators();       // also calls loadIndicatorRecent()
  await loadTickerAlerts();
  await loadTelegramStatus();   // also populates channel routing table
  await refreshStatus();
  await refreshAlerts();
  await loadAllHistStatus();
  await updateStreamChips();
  setInterval(updateMarketBanner,   30000);   // market banner every 30s
  setInterval(loadSignalMatrix,     30000);   // signal matrix every 30s
  setInterval(loadCoverage,         15000);   // coverage table every 15s
  setInterval(loadTelegramStatus,   60000);   // telegram status every 60s
  setInterval(refreshStatus,        30000);
  setInterval(refreshAlerts,        60000);
  setInterval(updateStreamChips,    60000);
  setInterval(loadIndicatorRecent,  30000);   // recent triggers every 30s
  setInterval(loadTickerAlerts,     60000);   // ticker alert state every 60s
  // Refresh latency pills every 15s (ticks the age counter without a full status poll)
  setInterval(() => {
    const tickers = Object.keys(allStatus).sort();
    renderPills(tickers, allStatus);
  }, 15000);
})();
</script>
</body>
</html>"""


# ── WebSocket endpoint ─────────────────────────────────────────────────────────

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket, token: Optional[str] = Query(None)):
    host = websocket.client.host if websocket.client else ""
    if host not in ("127.0.0.1", "::1", "localhost"):
        if not token or not (_config and _config.AUTH_TOKENS.get(token)):
            await websocket.close(code=4001)
            return
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)


# ── REST routes ───────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def root(user: str = Depends(auth)):
    return HTMLResponse(_HTML)


@app.get("/api/status")
def api_status(user: str = Depends(auth)):
    if not _engine:
        return JSONResponse({"error": "engine not ready"}, status_code=503)
    return JSONResponse(_engine.get_status())


@app.get("/api/bars/{ticker}")
def api_bars(ticker: str, n: int = 200, user: str = Depends(auth)):
    if not _engine:
        return JSONResponse({"error": "engine not ready"}, status_code=503)
    return JSONResponse(_engine.get_bars(ticker.upper(), n))


@app.get("/api/alerts")
def api_alerts(n: int = 50, user: str = Depends(auth)):
    if not _alerter:
        return JSONResponse([])
    return JSONResponse(_alerter.alert_log.recent(n))


@app.get("/api/tickers")
def api_tickers(user: str = Depends(auth)):
    return JSONResponse({"tickers": _load_tickers_file()})


@app.get("/api/tickers/add")
def api_add_ticker(ticker: str, user: str = Depends(auth)):
    ticker = ticker.upper().strip()
    if not ticker or not ticker.isalpha():
        raise HTTPException(status_code=400, detail="Invalid ticker symbol")
    tickers = _load_tickers_file()
    if ticker not in tickers:
        tickers.append(ticker)
        _save_tickers_file(tickers)
        if _engine:   _engine.add_ticker(ticker)
        if _streamer: _streamer.reload()
        if ticker not in _ticker_alert_enabled:
            _ticker_alert_enabled[ticker] = True
        log.info(f"Added ticker: {ticker}")
    return JSONResponse({"ok": True, "tickers": tickers})


@app.get("/api/tickers/remove")
def api_remove_ticker(ticker: str, user: str = Depends(auth)):
    ticker = ticker.upper().strip()
    tickers = _load_tickers_file()
    if ticker in tickers:
        tickers.remove(ticker)
        _save_tickers_file(tickers)
        if _engine:   _engine.remove_ticker(ticker)
        if _streamer: _streamer.reload()
        _ticker_alert_enabled.pop(ticker, None)
        log.info(f"Removed ticker: {ticker}")
    return JSONResponse({"ok": True, "tickers": tickers})


@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.now(ET).isoformat()}


# ── History endpoints ─────────────────────────────────────────────────────────

@app.get("/api/history/{ticker}/pull")
def api_history_pull(ticker: str, user: str = Depends(auth)):
    if not _history:
        raise HTTPException(status_code=503, detail="HistoryPuller not initialized")
    return JSONResponse(_history.pull(ticker.upper()))


@app.get("/api/history/{ticker}/status")
def api_history_status(ticker: str, user: str = Depends(auth)):
    if not _history:
        return JSONResponse({"status": "idle", "ticker": ticker.upper(), "bars": 0})
    return JSONResponse(_history.status(ticker.upper()))


@app.get("/api/history/all")
def api_history_all(user: str = Depends(auth)):
    if not _history:
        return JSONResponse({})
    return JSONResponse(_history.all_status())


@app.get("/api/history/{ticker}/bars")
def api_history_bars(ticker: str, n: int = 600, after: int = 0, user: str = Depends(auth)):
    """Return recent N historical 1-min bars from local parquet (for DMA-warmup chart overlay)."""
    if not _history:
        return JSONResponse([])
    bars = _history.get_bars(ticker.upper(), n=n, after_ts=after if after else None)
    return JSONResponse(bars)


# ── Chart launch ──────────────────────────────────────────────────────────────

@app.get("/api/chart/{ticker}")
def api_launch_chart(ticker: str, user: str = Depends(auth)):
    """Spawn sn_chart.py desktop window for this ticker."""
    ticker = ticker.upper().strip()
    python = sys.executable
    script = os.path.join(os.path.dirname(os.path.dirname(__file__)), "STOCK_NOTIFY", "sn_chart.py")
    port   = _config.PORT if _config else 8436
    if not os.path.exists(script):
        raise HTTPException(status_code=404, detail=f"sn_chart.py not found at {script}")
    try:
        flags = 0
        if sys.platform == "win32":
            flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        subprocess.Popen([python, script, ticker, "--port", str(port)],
                         creationflags=flags, close_fds=True)
        log.info(f"Launched chart for {ticker}")
        return JSONResponse({"ok": True, "ticker": ticker})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Stream endpoints ──────────────────────────────────────────────────────────

@app.get("/api/stream/status")
def api_stream_status(user: str = Depends(auth)):
    return JSONResponse({
        "streaming": _streamer.get_streaming_tickers() if _streamer else [],
        "engine":    _engine.get_tickers()              if _engine   else [],
        "running":   _streamer._running                 if _streamer else False,
    })


@app.get("/api/stream/reload")
def api_stream_reload(user: str = Depends(auth)):
    """Force the live stream to reconnect with current tickers.json."""
    if not _streamer:
        raise HTTPException(status_code=503, detail="Streamer not running")
    _streamer.reload()
    log.info("Stream reload triggered via API")
    return JSONResponse({"ok": True})


# ── Natural language command endpoint ─────────────────────────────────────────

@app.post("/api/cmd")
async def api_cmd(request: Request, user: str = Depends(auth)):
    """
    Natural language command handler.
    Uses Claude API if ANTHROPIC_API_KEY is set, otherwise returns a help message.
    """
    body = await request.json()
    cmd  = body.get("cmd", "").strip()
    if not cmd:
        return JSONResponse({"result": "Empty command"})

    try:
        import anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ImportError("No API key")

        tickers = _load_tickers_file()
        system = f"""You are an assistant for StockNotify, a live stock monitoring system.
Current tracked tickers: {', '.join(tickers)}
Available actions (respond with a JSON object with 'action' and 'args' keys):
  - add_ticker: {{"action":"add_ticker","args":{{"ticker":"SYMBOL"}}}}
  - remove_ticker: {{"action":"remove_ticker","args":{{"ticker":"SYMBOL"}}}}
  - pull_history: {{"action":"pull_history","args":{{"ticker":"SYMBOL"}} or {{"ticker":"all"}}}}
  - reload_stream: {{"action":"reload_stream","args":{{}}}}
  - info: {{"action":"info","args":{{"message":"..."}}}}
Parse the user's request and return a single JSON object only. No explanation."""

        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            system=system,
            messages=[{"role": "user", "content": cmd}],
        )
        import json as _json
        resp = _json.loads(msg.content[0].text)
        action = resp.get("action")
        args   = resp.get("args", {})

        if action == "add_ticker":
            tk = args.get("ticker", "").upper()
            if tk and tk.isalpha():
                tickers_list = _load_tickers_file()
                if tk not in tickers_list:
                    tickers_list.append(tk)
                    _save_tickers_file(tickers_list)
                    if _engine:   _engine.add_ticker(tk)
                    if _streamer: _streamer.reload()
                return JSONResponse({"result": f"Added {tk} to tracked tickers"})
        elif action == "remove_ticker":
            tk = args.get("ticker", "").upper()
            tickers_list = _load_tickers_file()
            if tk in tickers_list:
                tickers_list.remove(tk)
                _save_tickers_file(tickers_list)
                if _engine:   _engine.remove_ticker(tk)
                if _streamer: _streamer.reload()
            return JSONResponse({"result": f"Removed {tk}"})
        elif action == "pull_history":
            tk = args.get("ticker", "").upper()
            if tk == "ALL":
                for t in _load_tickers_file():
                    if _history: _history.pull(t)
                return JSONResponse({"result": "Pulling history for all tickers"})
            elif tk and _history:
                _history.pull(tk)
                return JSONResponse({"result": f"Pulling history for {tk}"})
        elif action == "reload_stream":
            if _streamer: _streamer.reload()
            return JSONResponse({"result": "Stream reload triggered"})
        elif action == "info":
            return JSONResponse({"result": args.get("message", "")})

        return JSONResponse({"result": f"Executed: {action}"})

    except (ImportError, Exception):
        return JSONResponse({"result": (
            "Natural language commands require ANTHROPIC_API_KEY. "
            "Type 'help' to see available commands."
        )})


# ── Indicator endpoints ────────────────────────────────────────────────────────

@app.get("/api/indicators")
def api_get_indicators(user: str = Depends(auth)):
    """Return list of all indicator configs with runtime enabled state."""
    if _indicator_state:
        return JSONResponse([
            {"name": k, **v}
            for k, v in _indicator_state.items()
        ])
    if _config and hasattr(_config, "ALERT_INDICATORS"):
        return JSONResponse([
            {"name": k, **v}
            for k, v in _config.ALERT_INDICATORS.items()
        ])
    return JSONResponse([])


@app.post("/api/telegram/test-chart")
def api_telegram_test_chart(user: str = Depends(auth)):
    """
    Fire a test ORB_30_RETEST_HIGH signal through the full pipeline for the first
    available ticker — generates a real chart from live bar data and sends it
    to all configured Telegram chats as a photo.
    """
    import importlib, datetime as _dt

    if not _alerter or not _engine:
        return JSONResponse({"ok": False, "error": "Engine or alerter not ready"})

    # Pick a ticker that has bar data
    status = _engine.get_status()
    ticker = next(
        (t for t, s in status.items() if s.get("bars_today", 0) > 5),
        next(iter(status), None),
    )
    if not ticker:
        return JSONResponse({"ok": False, "error": "No tickers with bar data"})

    # Grab live bars from engine state (thread-safe copy)
    try:
        with _engine._lock:
            state = _engine._states.get(ticker)
            bars  = list(state.bars) if state else []
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"Could not read bars: {exc}"})

    if not bars:
        return JSONResponse({"ok": False, "error": f"No bars available for {ticker}"})

    # Build a test signal using the last bar's price and current indicator values
    from stocknotify.analysis import Signal
    last_bar = bars[-1]
    test_sig = Signal(
        ticker      = ticker,
        signal_type = "ORB_30_RETEST_HIGH",
        price       = last_bar.get("close", 0),
        vwap        = last_bar.get("vwap"),
        wvwap       = last_bar.get("wvwap"),
        bar_time    = last_bar.get("bar_time", _dt.datetime.now(ET)),
        extra       = {"_bars": list(bars)},
    )

    # Override Telegram text to make it obvious it's a test
    original_to_telegram = test_sig.to_telegram
    test_sig.to_telegram = lambda: f"🧪 TEST CHART\n\n{original_to_telegram()}"

    try:
        _alerter.send(test_sig)
        log.info(f"Test chart sent for {ticker} — {len(bars)} bars")
        return JSONResponse({
            "ok": True,
            "ticker": ticker,
            "bars": len(bars),
            "price": last_bar.get("close"),
            "chats": list(_config.TELEGRAM_CHATS.values()) if _config else [],
        })
    except Exception as exc:
        log.error(f"Test chart send failed: {exc}")
        return JSONResponse({"ok": False, "error": str(exc)})


@app.get("/api/indicators/recent")
def api_indicators_recent(user: str = Depends(auth)):
    """Return last 3 triggers per signal type from the in-memory alert log."""
    if not _alerter:
        return JSONResponse({})
    entries = _alerter.alert_log.recent(500)
    by_type: dict = {}
    for entry in entries:
        sig = entry.get("type", "")
        if not sig:
            continue
        lst = by_type.setdefault(sig, [])
        if len(lst) < 3:
            lst.append({
                "ticker":  entry.get("ticker", ""),
                "ts":      entry.get("ts", ""),
                "price":   entry.get("price", 0),
                "tg_sent": entry.get("tg_sent", False),
            })
    return JSONResponse(by_type)


@app.get("/api/tickers/alerts")
def api_get_ticker_alerts(user: str = Depends(auth)):
    """Return per-ticker alert enabled state."""
    return JSONResponse(dict(_ticker_alert_enabled))


@app.post("/api/tickers/{ticker}/toggle-alerts")
def api_toggle_ticker_alert(ticker: str, user: str = Depends(auth)):
    """Toggle alert suppression on/off for a specific ticker."""
    ticker = ticker.upper()
    _ticker_alert_enabled[ticker] = not _ticker_alert_enabled.get(ticker, True)
    state = _ticker_alert_enabled[ticker]
    log.info(f"Ticker {ticker} alerts {'enabled' if state else 'disabled'} by {user}")
    return JSONResponse({"ticker": ticker, "enabled": state})


@app.post("/api/indicators/{name}/toggle")
def api_toggle_indicator(name: str, user: str = Depends(auth)):
    """Toggle an indicator's enabled state at runtime."""
    if name not in _indicator_state:
        raise HTTPException(status_code=404, detail=f"Unknown indicator: {name}")
    _indicator_state[name]["enabled"] = not _indicator_state[name]["enabled"]
    state = _indicator_state[name]["enabled"]
    log.info(f"Indicator {name} {'enabled' if state else 'disabled'} by {user}")
    return JSONResponse({"name": name, "enabled": state})


# ── Telegram endpoints ─────────────────────────────────────────────────────────

@app.post("/api/telegram/test")
def api_telegram_test(user: str = Depends(auth)):
    """Send a test Telegram message to all configured chats."""
    import urllib.request, json as _json
    bot_token = (_config.TELEGRAM_BOT_TOKEN if _config else "") or ""
    chats     = (_config.TELEGRAM_CHATS     if _config else {}) or {}

    if not bot_token or bot_token.startswith("YOUR"):
        return JSONResponse({"ok": False, "error": "TELEGRAM_BOT_TOKEN not configured"})
    if not chats:
        return JSONResponse({"ok": False, "error": "TELEGRAM_CHATS is empty — add chat IDs in sn_config.py"})

    now_str = datetime.now(ET).strftime("%I:%M %p ET")
    text = (
        f"✅ *StockNotify — Test Alert*\n"
        f"Service running on port {_config.PORT if _config else 8436}\n"
        f"Time: {now_str}\n"
        f"Tracking {len(_load_tickers_file())} tickers: {', '.join(_load_tickers_file())}\n"
        f"All systems nominal."
    )

    results, any_ok = [], False
    for chat_id, label in chats.items():
        url     = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = _json.dumps({"chat_id": str(chat_id), "text": text, "parse_mode": "Markdown"}).encode()
        try:
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
            results.append({"chat_id": chat_id, "label": label, "ok": True})
            any_ok = True
            log.info(f"Test Telegram sent to {label} ({chat_id})")
        except Exception as exc:
            results.append({"chat_id": chat_id, "label": label, "ok": False, "error": str(exc)})
            log.error(f"Test Telegram failed for {label} ({chat_id}): {exc}")

    return JSONResponse({"ok": any_ok, "results": results})


@app.get("/api/telegram/status")
def api_telegram_status(user: str = Depends(auth)):
    """Return Telegram config status (no secrets exposed)."""
    bot_token = (_config.TELEGRAM_BOT_TOKEN if _config else "") or ""
    chats     = (_config.TELEGRAM_CHATS     if _config else {}) or {}
    configured = bool(bot_token and not bot_token.startswith("YOUR"))
    return JSONResponse({
        "configured":   configured,
        "token_prefix": bot_token[:8]+"…" if configured else None,
        "chat_count":   len(chats),
        "chats":        {cid: label for cid, label in chats.items()},
    })


@app.post("/api/telegram/test-all-tickers")
def api_telegram_test_all_tickers(user: str = Depends(auth)):
    """
    Send one combined test message per configured Telegram chat/channel
    listing all tracked tickers + live prices, then return per-ticker results
    showing which channels succeeded.
    """
    import urllib.request as _req, json as _json, time as _time
    bot_token = (_config.TELEGRAM_BOT_TOKEN if _config else "") or ""
    chats     = (_config.TELEGRAM_CHATS     if _config else {}) or {}
    tickers   = _load_tickers_file()

    if not bot_token or bot_token.startswith("YOUR"):
        return JSONResponse({"ok": False, "error": "Bot token not configured", "results": []})
    if not chats:
        return JSONResponse({"ok": False, "error": "No TELEGRAM_CHATS configured", "results": []})

    now_str  = datetime.now(ET).strftime("%I:%M %p ET")
    eng_st   = _engine.get_status() if _engine else {}

    # Build the ticker price table
    lines = []
    for tk in tickers:
        st    = eng_st.get(tk, {})
        price = st.get("last_price")
        vwap  = st.get("vwap")
        px_s  = f"${price:.2f}" if price else "  —  "
        vw_s  = f"${vwap:.2f}"  if vwap  else "  —  "
        pct   = f"{(price-vwap)/vwap*100:+.2f}%" if (price and vwap) else ""
        lines.append(f"  {tk:<6} {px_s:>9}  VWAP {vw_s:>9}  {pct}")

    text = (
        f"*StockNotify — Delivery Test*\n"
        f"Time: {now_str}\n"
        f"Tracking {len(tickers)} symbols:\n\n"
        + "\n".join(lines) +
        f"\n\nAll {len(tickers)} tickers active. Signals routing to this channel."
    )

    # Send one message per channel
    ch_outcome: dict[str, dict] = {}   # chat_id -> {ok, label, error}
    for chat_id, label in chats.items():
        url     = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = _json.dumps({
            "chat_id": str(chat_id), "text": text, "parse_mode": "Markdown"
        }).encode()
        try:
            req = _req.Request(url, data=payload, headers={"Content-Type": "application/json"})
            _req.urlopen(req, timeout=10)
            ch_outcome[chat_id] = {"label": label, "ok": True}
            log.info(f"Test batch sent to {label} ({chat_id})")
        except Exception as exc:
            ch_outcome[chat_id] = {"label": label, "ok": False, "error": str(exc)}
            log.error(f"Test batch failed for {label} ({chat_id}): {exc}")
        _time.sleep(0.5)   # respect Telegram rate limits

    # Return per-ticker results (all tickers share the same channel outcomes)
    any_ok = any(v["ok"] for v in ch_outcome.values())
    results = []
    for tk in tickers:
        ch_results = [{"chat_id": cid, "label": v["label"], "ok": v["ok"],
                       "error": v.get("error", "")}
                      for cid, v in ch_outcome.items()]
        results.append({"ticker": tk, "ok": any_ok, "channels": ch_results})

    return JSONResponse({"ok": any_ok, "results": results})


# ── Self-restart endpoint ─────────────────────────────────────────────────────

@app.post("/api/self-restart")
def api_self_restart(user: str = Depends(auth)):
    """Ask Central (port 8400) to restart this service, then return immediately."""
    import urllib.request as _req, threading as _thr
    def _do():
        import time as _t
        _t.sleep(0.3)   # let the HTTP response leave before we die
        try:
            req = _req.Request(
                "http://localhost:8400/api/services/stock_notify/restart",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            _req.urlopen(req, timeout=5)
        except Exception as exc:
            log.warning(f"Self-restart via Central failed: {exc} — falling back to os._exit")
            import os as _os
            _os.kill(_os.getpid(), 15)   # SIGTERM — Central watchdog will restart
    _thr.Thread(target=_do, daemon=True).start()
    return JSONResponse({"ok": True, "msg": "Restart triggered"})


# ── Coverage endpoint ──────────────────────────────────────────────────────────

@app.get("/api/coverage")
def api_coverage(user: str = Depends(auth)):
    """Per-ticker streaming latency + historical parquet coverage stats."""
    from datetime import timezone as _tz
    tickers  = _load_tickers_file()
    streaming = set(_streamer.get_streaming_tickers() if _streamer else [])
    eng_st    = _engine.get_status() if _engine else {}

    result = {}
    for tk in tickers:
        st   = eng_st.get(tk, {})
        hist = _get_parquet_stats(tk)

        last_bar_time = st.get("last_bar_time")
        latency_s = None
        if last_bar_time:
            try:
                dt = datetime.fromisoformat(last_bar_time.replace("Z", "+00:00"))
                latency_s = int((datetime.now(_tz.utc) - dt).total_seconds())
            except Exception:
                pass

        lp   = st.get("last_price")
        vwap = st.get("vwap")
        pct_vs_vwap = round((lp - vwap) / vwap * 100, 3) if lp and vwap else None

        result[tk] = {
            "streaming":       tk in streaming,
            "last_bar_time":   last_bar_time,
            "latency_s":       latency_s,
            "last_price":      lp,
            "vwap":            vwap,
            "wvwap":           st.get("wvwap"),
            "pct_vs_vwap":     pct_vs_vwap,
            "bars_today":      st.get("bars_today", 0),
            "hist_bars":       hist.get("bars", 0),
            "hist_first_date": hist.get("first_date"),
            "hist_last_date":  hist.get("last_date"),
            "hist_pct":        hist.get("pct", 0.0),
            "hist_expected_days": hist.get("expected_days", 0),
            "last_close":      hist.get("last_close"),
            "needs_pull":      hist.get("needs_pull", False),
        }
    return JSONResponse(result)


# ── Signal Matrix endpoints ────────────────────────────────────────────────────

@app.get("/api/signal-matrix")
def api_signal_matrix(user: str = Depends(auth)):
    """Today's signal hits per indicator × ticker for the matrix table."""
    tickers    = _load_matrix_tickers()
    indicators = (list(_indicator_state.keys()) if _indicator_state
                  else (list(_config.ALERT_INDICATORS.keys()) if _config else []))

    today_str = datetime.now(ET).strftime("%Y-%m-%d")
    entries   = _alerter.alert_log.recent(500) if _alerter else []

    # hits[signal_type][ticker] = list of "HH:MM" strings (newest first, appendleft order)
    hits: dict = {}
    for entry in entries:
        ts = entry.get("ts", "")
        if not ts.startswith(today_str):
            continue
        sig = entry.get("type", "")
        tk  = entry.get("ticker", "")
        if not sig or tk not in tickers:
            continue
        time_str = ts[11:16]   # "HH:MM" from ISO string — no platform-specific strftime needed
        hits.setdefault(sig, {}).setdefault(tk, []).append(time_str)

    # Build compact summary: {sig: {tk: {last_time, count}}}
    matrix: dict = {}
    for sig in indicators:
        tk_hits: dict = {}
        for tk in tickers:
            times = hits.get(sig, {}).get(tk, [])
            if times:
                tk_hits[tk] = {"last_time": times[0], "count": len(times)}
        if tk_hits:
            matrix[sig] = tk_hits

    return JSONResponse({"tickers": tickers, "matrix": matrix})


@app.get("/api/matrix/tickers")
def api_get_matrix_tickers(user: str = Depends(auth)):
    return JSONResponse({"tickers": _load_matrix_tickers()})


@app.post("/api/matrix/tickers/add")
async def api_add_matrix_ticker(request: Request, user: str = Depends(auth)):
    body = await request.json()
    tk   = body.get("ticker", "").strip().upper()
    if not tk:
        raise HTTPException(status_code=400, detail="ticker required")
    if tk not in _matrix_tickers:
        _matrix_tickers.append(tk)
        _save_matrix_tickers()
        log.info(f"Matrix ticker added: {tk}")
    return JSONResponse({"tickers": list(_matrix_tickers)})


@app.post("/api/matrix/tickers/remove")
async def api_remove_matrix_ticker(request: Request, user: str = Depends(auth)):
    global _matrix_tickers
    body = await request.json()
    tk   = body.get("ticker", "").strip().upper()
    _matrix_tickers = [t for t in _matrix_tickers if t != tk]
    _save_matrix_tickers()
    log.info(f"Matrix ticker removed: {tk}")
    return JSONResponse({"tickers": list(_matrix_tickers)})
