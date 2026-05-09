"""
StockNotify — Historical 1-minute bar puller

Pulls OHLCV 1-minute bars from Databento EQUS.MINI (available from ~March 2023).
Stores to: D:/CentralFolder/STOCKNOTIFY/{ticker}/{ticker}_1m.parquet

Usage (from dashboard):
    puller = HistoryPuller(config)
    puller.pull(ticker)              # non-blocking, runs in background thread
    info = puller.status(ticker)     # {"status": "running"|"done"|"error"|"idle", ...}
    bars = puller.get_bars(ticker, n=500)  # recent N bars as list of dicts
"""

import logging
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import pytz

log = logging.getLogger("sn_history")
ET  = pytz.timezone("America/New_York")


class HistoryPuller:
    """Thread-safe historical bar puller. One background thread per active pull."""

    def __init__(self, config):
        self.config = config
        self._lock   = threading.Lock()
        self._status: dict = {}   # ticker → status dict
        self._threads: dict = {}  # ticker → Thread

    # ── public API ────────────────────────────────────────────────────────────

    def pull(self, ticker: str) -> dict:
        """Start a background pull for ticker. Returns current status dict."""
        ticker = ticker.upper().strip()
        with self._lock:
            st = self._status.get(ticker, {})
            if st.get("status") == "running":
                return dict(st)
            self._status[ticker] = {
                "status":    "running",
                "ticker":    ticker,
                "progress":  "Starting…",
                "bars":      0,
                "started_at": datetime.now(ET).isoformat(),
                "finished_at": None,
                "error":     None,
            }
        t = threading.Thread(
            target=self._run_pull,
            args=(ticker,),
            daemon=True,
            name=f"sn-hist-{ticker}",
        )
        with self._lock:
            self._threads[ticker] = t
        t.start()
        with self._lock:
            return dict(self._status[ticker])

    def status(self, ticker: str) -> dict:
        ticker = ticker.upper().strip()
        with self._lock:
            st = self._status.get(ticker)
            if not st:
                path = self._parquet_path(ticker)
                if os.path.exists(path):
                    return {"status": "done", "ticker": ticker,
                            "progress": "Data on disk", "bars": self._count_rows(path),
                            "started_at": None, "finished_at": None, "error": None}
                return {"status": "idle", "ticker": ticker, "progress": "Not pulled yet",
                        "bars": 0, "started_at": None, "finished_at": None, "error": None}
            return dict(st)

    def all_status(self) -> dict:
        tickers = []
        with self._lock:
            tickers = list(self._status.keys())
        return {tk: self.status(tk) for tk in tickers}

    def get_bars(self, ticker: str, n: int = 500, after_ts: Optional[int] = None) -> list:
        """
        Returns the most recent N 1-minute bars as dicts suitable for TradingView.
        If after_ts (Unix seconds) provided, only returns bars after that timestamp.
        """
        path = self._parquet_path(ticker.upper())
        if not os.path.exists(path):
            return []
        try:
            import pandas as pd
            df = pd.read_parquet(path)
            if df.empty:
                return []
            # Ensure timestamp column exists
            if "ts" not in df.columns and df.index.name == "ts":
                df = df.reset_index()
            if "ts" not in df.columns:
                return []
            if after_ts is not None:
                df = df[df["ts"] > after_ts]
            df = df.tail(n)
            return [
                {
                    "t": int(row["ts"]),
                    "o": round(float(row["open"]),  2),
                    "h": round(float(row["high"]),  2),
                    "l": round(float(row["low"]),   2),
                    "c": round(float(row["close"]), 2),
                    "v": int(row.get("volume", 0)),
                }
                for _, row in df.iterrows()
                if not any(
                    __import__("math").isnan(float(row.get(col, 0)))
                    for col in ("open", "high", "low", "close")
                )
            ]
        except Exception as exc:
            log.error(f"get_bars({ticker}): {exc}")
            return []

    # ── internal ──────────────────────────────────────────────────────────────

    def _parquet_path(self, ticker: str) -> str:
        root = getattr(self.config, "HIST_STORAGE_ROOT", "D:/CentralFolder/STOCKNOTIFY")
        return os.path.join(root, ticker, f"{ticker}_1m.parquet")

    def _count_rows(self, path: str) -> int:
        try:
            import pandas as pd
            return len(pd.read_parquet(path, columns=["open"]))
        except Exception:
            return 0

    def _update_status(self, ticker: str, **kwargs):
        with self._lock:
            if ticker in self._status:
                self._status[ticker].update(kwargs)

    def _run_pull(self, ticker: str):
        try:
            self._do_pull(ticker)
        except Exception as exc:
            log.error(f"History pull failed for {ticker}: {exc}", exc_info=True)
            self._update_status(ticker,
                status="error",
                error=str(exc),
                finished_at=datetime.now(ET).isoformat(),
            )

    def _do_pull(self, ticker: str):
        import databento as db
        import pandas as pd

        api_key   = self.config.DATABENTO_API_KEY
        dataset   = self.config.DATASET          # "EQUS.MINI"
        start_str = getattr(self.config, "HIST_START_DATE", "2023-03-28")
        end_dt    = datetime.now(timezone.utc)
        end_str   = end_dt.strftime("%Y-%m-%d")
        path      = self._parquet_path(ticker)

        # ── Check if we already have data; if so, do an incremental update ──
        existing_df = None
        if os.path.exists(path):
            try:
                existing_df = pd.read_parquet(path)
                if not existing_df.empty and "ts" in existing_df.columns:
                    last_ts = int(existing_df["ts"].max())
                    # Resume from last bar + 1 minute
                    resume_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc) + timedelta(minutes=1)
                    if resume_dt.date() >= end_dt.date():
                        self._update_status(ticker,
                            status="done",
                            progress=f"Up to date ({len(existing_df):,} bars)",
                            bars=len(existing_df),
                            finished_at=datetime.now(ET).isoformat(),
                        )
                        log.info(f"{ticker} history already up to date ({len(existing_df):,} bars)")
                        return
                    start_str = resume_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    log.info(f"{ticker} incremental pull from {start_str}")
            except Exception as exc:
                log.warning(f"Could not read existing parquet for {ticker}: {exc} — full pull")
                existing_df = None

        self._update_status(ticker, progress=f"Connecting to Databento ({start_str} → {end_str})…")
        log.info(f"Pulling {ticker} OHLCV-1m: {start_str} → {end_str}")

        client = db.Historical(key=api_key)

        # Databento timeseries.get_range returns a DBNStore
        try:
            dbn = client.timeseries.get_range(
                dataset=dataset,
                schema="ohlcv-1m",
                stype_in="raw_symbol",
                symbols=[ticker],
                start=start_str,
                end=end_str,
            )
        except Exception as exc:
            raise RuntimeError(f"Databento API error: {exc}") from exc

        self._update_status(ticker, progress="Converting data…")
        df_new = dbn.to_df()

        if df_new is None or df_new.empty:
            if existing_df is not None and not existing_df.empty:
                self._update_status(ticker,
                    status="done",
                    progress=f"No new bars; existing: {len(existing_df):,} bars",
                    bars=len(existing_df),
                    finished_at=datetime.now(ET).isoformat(),
                )
            else:
                self._update_status(ticker,
                    status="done",
                    progress="No data returned (check dataset availability)",
                    bars=0,
                    finished_at=datetime.now(ET).isoformat(),
                )
            return

        # ── Normalize DataFrame ──────────────────────────────────────────────
        df_new = self._normalize_df(df_new, ticker)

        # Merge with existing if incremental
        if existing_df is not None and not existing_df.empty:
            df_out = pd.concat([existing_df, df_new], ignore_index=True)
            df_out = df_out.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        else:
            df_out = df_new

        total = len(df_out)
        self._update_status(ticker, progress=f"Saving {total:,} bars…", bars=total)

        # ── Save to parquet ──────────────────────────────────────────────────
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        df_out.to_parquet(tmp, index=False)
        os.replace(tmp, path)

        log.info(f"{ticker} history saved: {total:,} bars → {path}")
        self._update_status(ticker,
            status="done",
            progress=f"Done — {total:,} bars saved",
            bars=total,
            finished_at=datetime.now(ET).isoformat(),
        )

    @staticmethod
    def _normalize_df(df, ticker: str):
        """Normalize Databento OHLCV-1m DataFrame to our standard schema."""
        import pandas as pd
        import numpy as np

        # Databento OHLCV columns: open, high, low, close, volume (as int64, scaled by 1e9)
        # ts_event is the bar timestamp (nanoseconds since epoch)

        # Find price columns (Databento uses fixed-point int64 for prices)
        price_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]
        for col in price_cols:
            if df[col].dtype in (np.int64, "int64"):
                # Databento price scale: divide by 1e9
                df[col] = df[col] / 1e9

        # Volume
        if "volume" in df.columns and df["volume"].dtype in (np.int64, "int64"):
            pass  # keep as-is

        # Timestamp → Unix seconds
        if "ts_event" in df.columns:
            ts = df["ts_event"]
            if ts.dtype in (np.int64, "int64"):
                # nanoseconds → seconds
                df["ts"] = (ts // 1_000_000_000).astype(np.int64)
            else:
                df["ts"] = pd.to_datetime(ts, utc=True).astype(np.int64) // 1_000_000_000
        elif df.index.name == "ts_event":
            ts_idx = df.index
            if hasattr(ts_idx, "astype"):
                df["ts"] = ts_idx.astype(np.int64) // 1_000_000_000
            df = df.reset_index(drop=True)
        else:
            # Fall back: use row number as placeholder
            log.warning(f"{ticker}: no ts_event column found")
            df["ts"] = range(len(df))

        # Keep only the columns we need
        keep = ["ts", "open", "high", "low", "close", "volume"]
        keep = [c for c in keep if c in df.columns]
        df = df[keep].copy()

        # Filter out zero/negative prices (Databento uses 0 for missing data)
        for col in ("open", "high", "low", "close"):
            if col in df.columns:
                df = df[df[col] > 0]

        # Filter market hours (9:30–16:00 ET) to discard pre/post market if desired
        # (skipped — keep all bars, let chart decide)

        df = df.sort_values("ts").reset_index(drop=True)
        return df
