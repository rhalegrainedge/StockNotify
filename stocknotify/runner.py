"""
StockNotify — Entry point

Wires together: Databento streamer → analysis engine → WebSocket broadcast + Telegram alerts
Then serves the FastAPI dashboard on the configured port.

Usage:
    python -m stocknotify                      # normal
    python -m stocknotify --no-stream          # dashboard only (no Databento)
    python -m stocknotify --port 8436          # override port
"""

import argparse
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)-14s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sn_runner")


def main():
    parser = argparse.ArgumentParser(description="StockNotify runner")
    parser.add_argument("--port",      type=int, default=None, help="Dashboard port")
    parser.add_argument("--no-stream", action="store_true",    help="Skip Databento stream")
    args = parser.parse_args()

    from stocknotify import config
    from stocknotify.analysis  import AnalysisEngine
    from stocknotify.alerts    import TelegramAlerter
    from stocknotify.streamer  import StockStreamer
    import stocknotify.dashboard as dashboard

    port = args.port or config.PORT

    log.info(f"StockNotify starting on port {port}")

    # ── Alerter (Telegram + in-memory log) ───────────────────────────────────
    alerter = TelegramAlerter(config)

    # ── Callbacks ─────────────────────────────────────────────────────────────

    def on_signal(ticker, signal):
        """Called for every generated signal — send Telegram + push to WS clients."""
        if not dashboard.is_indicator_enabled(signal.signal_type):
            return  # indicator disabled — skip
        if not dashboard.is_ticker_alert_enabled(ticker):
            return  # ticker alerts suppressed — skip
        alerter.send(signal)
        dashboard.broadcast(signal.to_ws_dict())

    def on_bar(bar_dict):
        """Called for every completed 1-min bar — push to WS clients for live chart update."""
        dashboard.broadcast(bar_dict)

    # ── Analysis engine ───────────────────────────────────────────────────────
    engine = AnalysisEngine(config, on_signal=on_signal, on_bar=on_bar)

    # ── Streamer ──────────────────────────────────────────────────────────────
    streamer = None
    if not args.no_stream:
        streamer = StockStreamer(config, on_bar_complete=engine.on_bar_complete)
        streamer.start()
        log.info(f"Streamer started — dataset: {config.DATASET}")
        from stocknotify.streamer import _load_tickers
        log.info(f"Tickers: {', '.join(_load_tickers(config))}")
    else:
        log.info("--no-stream: running in dashboard-only mode")

    # ── Dashboard (FastAPI + WebSocket) ───────────────────────────────────────
    dashboard.init(config, engine, alerter, streamer=streamer)

    import uvicorn
    log.info(f"Dashboard → http://0.0.0.0:{port}")
    log.info("Localhost: no token needed (bypass auth)")
    log.info(f"External:  http://YOUR_IP:{port}/?token=admin-change-this-token")

    uvicorn.run(dashboard.app, host="0.0.0.0", port=port, log_level="warning")


if __name__ == "__main__":
    main()
