"""
StockNotify — Databento EQUS.MINI live streamer

Streams trades for all tickers in tickers.json (refreshed on each reconnect).
Aggregates tick trades into 1-minute OHLCV bars.
Calls on_bar_complete(ticker, bar) for each completed bar.
Supports dynamic ticker reload: call reload() to force reconnect with new list.
"""

import os
import json
import threading
import time
import logging
from datetime import datetime, timezone

import pytz

log = logging.getLogger("sn_streamer")

ET = pytz.timezone("America/New_York")

_MARKET_OPEN_H  = 9
_MARKET_OPEN_M  = 30


def _floor_to_minute(ts_ns: int) -> datetime:
    ts = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).astimezone(ET)
    return ts.replace(second=0, microsecond=0)


def _market_open_ns() -> int:
    now = datetime.now(ET)
    open_et = now.replace(hour=_MARKET_OPEN_H, minute=_MARKET_OPEN_M, second=0, microsecond=0)
    return int(open_et.timestamp() * 1e9)


def _load_tickers(config) -> list:
    """Read current ticker list from tickers.json, fall back to config.TICKERS."""
    f = os.path.join(os.path.dirname(__file__), "tickers.json")
    if os.path.exists(f):
        try:
            data = json.load(open(f))
            tickers = [t.upper().strip() for t in data.get("tickers", [])]
            if tickers:
                return tickers
        except Exception as exc:
            log.warning(f"Could not read tickers.json: {exc}")
    return list(config.TICKERS)


class StockStreamer(threading.Thread):
    """
    Background thread — streams EQUS.MINI trades, aggregates to 1-min bars.
    Reconnects automatically on error. Reads tickers.json fresh on each reconnect.

    Usage:
        s = StockStreamer(config, on_bar_complete=engine.on_bar_complete)
        s.start()
        s.reload()   # force reconnect with updated tickers.json
        s.stop()
    """

    def __init__(self, config, on_bar_complete):
        super().__init__(daemon=True, name="sn-streamer")
        self.config          = config
        self.on_bar_complete = on_bar_complete

        self._running          = False
        self._force_reconnect  = False
        self._lock             = threading.Lock()

        self._id_to_sym: dict = {}     # instrument_id → ticker
        self._bar_state: dict = {}     # ticker → in-progress bar accumulator
        self._last_ts_ns: int = 0      # latest ts_event seen — used to resume on reconnect

    # ── public ───────────────────────────────────────────────────────────────

    def start(self):
        self._running = True
        super().start()

    def stop(self):
        self._running = False

    def reload(self):
        """Force a stream reconnect so tickers.json changes take effect immediately."""
        log.info("Streamer reload requested — reconnecting with updated ticker list")
        self._force_reconnect = True

    def get_streaming_tickers(self) -> list:
        with self._lock:
            return list(self._bar_state.keys())

    # ── thread ───────────────────────────────────────────────────────────────

    def run(self):
        while self._running:
            try:
                self._stream_once()
            except Exception as exc:
                if self._running:
                    log.error(f"Streamer error: {exc} — reconnecting in 15s")
                    time.sleep(15)

    # How long with no records during market hours before we force a reconnect
    _STALE_RECONNECT_SECS = 300   # 5 minutes

    @staticmethod
    def _is_market_hours() -> bool:
        """True if current ET time is between 9:25 and 16:05 on a weekday."""
        now = datetime.now(ET)
        if now.weekday() >= 5:          # Sat / Sun
            return False
        t = now.hour * 60 + now.minute
        return (9 * 60 + 25) <= t <= (16 * 60 + 5)

    def _stream_once(self):
        import databento as db

        api_key = os.environ.get("SN_DB_KEY") or self.config.DATABENTO_API_KEY
        if not api_key or api_key.startswith("YOUR"):
            log.error("Databento API key not configured. Set SN_DB_KEY env var or edit config.py")
            time.sleep(60)
            return

        # Read current ticker list fresh from file on every reconnect
        tickers = _load_tickers(self.config)
        self._force_reconnect = False

        # Reset state for new connection
        with self._lock:
            self._id_to_sym = {}
            self._bar_state = {t: self._fresh_bar() for t in tickers}

        mkt_open_ns = _market_open_ns()
        if self._last_ts_ns > mkt_open_ns:
            start_ns = max(mkt_open_ns, self._last_ts_ns - int(2 * 60 * 1e9))
            log.info(f"Resuming stream from {start_ns} (2 min before last seen trade)")
        else:
            start_ns = mkt_open_ns
        log.info(f"Connecting EQUS.MINI — tickers: {', '.join(tickers)}")

        client = db.Live(key=api_key)

        # Watchdog: if no records arrive during market hours for N minutes, stop the
        # client so the iterator exits and _stream_once returns → triggers reconnect.
        last_record_time = [time.time()]
        stop_watchdog    = threading.Event()

        def _stale_watchdog():
            while not stop_watchdog.is_set():
                time.sleep(30)
                if stop_watchdog.is_set():
                    break
                silent = time.time() - last_record_time[0]
                if silent >= self._STALE_RECONNECT_SECS and self._is_market_hours():
                    log.warning(
                        f"No records for {silent:.0f}s during market hours — "
                        "forcing stream reconnect"
                    )
                    try:
                        client.stop()
                    except Exception:
                        pass
                    break

        watchdog_thread = threading.Thread(target=_stale_watchdog, daemon=True)
        watchdog_thread.start()

        try:
            client.subscribe(
                dataset=self.config.DATASET,
                schema="trades",
                stype_in="raw_symbol",
                symbols=tickers,
                start=start_ns,
            )
            log.info("Stream connected — waiting for records…")

            for record in client:
                if not self._running or self._force_reconnect:
                    break
                last_record_time[0] = time.time()
                self._dispatch(record)

        finally:
            stop_watchdog.set()
            try:
                client.stop()
            except Exception:
                pass

        if self._force_reconnect and self._running:
            log.info("Streamer reconnecting now with updated tickers")

    # ── record dispatch ───────────────────────────────────────────────────────

    @staticmethod
    def _get_iid(record) -> int:
        """Extract instrument_id — supports both old (.hd.instrument_id) and new (.instrument_id) APIs."""
        iid = getattr(record, "instrument_id", None)
        if iid is None:
            hd  = getattr(record, "hd", None)
            iid = getattr(hd, "instrument_id", None) if hd is not None else None
        return iid or 0

    @staticmethod
    def _get_ts_event(record) -> int:
        """Extract ts_event nanoseconds — supports both API generations."""
        ts = getattr(record, "ts_event", None)
        if ts is None:
            hd = getattr(record, "hd", None)
            ts = getattr(hd, "ts_event", None) if hd is not None else None
        return ts or 0

    def _dispatch(self, record):
        cls = record.__class__.__name__

        if cls == "SymbolMappingMsg":
            iid = self._get_iid(record)
            sym = (
                getattr(record, "stype_in_symbol",  None)
                or getattr(record, "stype_out_symbol", None)
                or getattr(record, "raw_symbol",      None)
            )
            if sym and iid:
                sym = sym.upper().strip()
                with self._lock:
                    if sym in self._bar_state:
                        self._id_to_sym[iid] = sym
                        log.info(f"Mapped {sym} → instrument_id {iid}")

        elif cls == "TradeMsg":
            iid = self._get_iid(record)
            with self._lock:
                ticker = self._id_to_sym.get(iid)
            if ticker:
                ts_event = self._get_ts_event(record)
                self._process_trade(ticker, record.price, record.size, ts_event)

    # ── bar aggregation ───────────────────────────────────────────────────────

    @staticmethod
    def _fresh_bar() -> dict:
        return {
            "bar_time": None,
            "open":     None,
            "high":     None,
            "low":      None,
            "close":    None,
            "volume":   0,
            "pw_sum":   0.0,
        }

    def _process_trade(self, ticker: str, price_raw: int, size: int, ts_ns: int):
        price = price_raw / 1e9
        if price <= 0 or size <= 0:
            return

        if ts_ns > self._last_ts_ns:
            self._last_ts_ns = ts_ns

        bar_time  = _floor_to_minute(ts_ns)
        completed = None

        with self._lock:
            if ticker not in self._bar_state:
                return
            s = self._bar_state[ticker]

            if s["bar_time"] is None:
                s["bar_time"] = bar_time
                s["open"] = s["high"] = s["low"] = s["close"] = price
                s["volume"] = size
                s["pw_sum"] = price * size

            elif bar_time > s["bar_time"]:
                completed = dict(s)
                s["bar_time"] = bar_time
                s["open"] = s["high"] = s["low"] = s["close"] = price
                s["volume"] = size
                s["pw_sum"] = price * size

            else:
                if price > s["high"]: s["high"] = price
                if price < s["low"]:  s["low"]  = price
                s["close"]   = price
                s["volume"] += size
                s["pw_sum"] += price * size

        if completed and completed["bar_time"] is not None:
            try:
                self.on_bar_complete(ticker, completed)
            except Exception as exc:
                log.error(f"on_bar_complete error ({ticker}): {exc}")
