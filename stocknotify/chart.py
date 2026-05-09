"""
StockNotify — Desktop candlestick chart (PySide6 + pyqtgraph)

Spawned by the dashboard as a subprocess:
    python -m stocknotify.chart TICKER [--port 8436] [--token TOKEN]

Shows:
  - Candlestick bars (historical parquet + live 1-min bars)
  - Daily VWAP line
  - Weekly VWAP line (dashed)
  - ORB high/low lines (dashed amber)
  - Signal markers (colored arrows)

Polls the StockNotify API every 60 seconds for new bars.
"""

import argparse
import logging
import os
import sys
import threading
import time
from datetime import datetime

# Force pyqtgraph to use PySide6 (must be set before any pyqtgraph import)
os.environ.setdefault("PYQTGRAPH_QT_LIB", "PySide6")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)-12s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sn_chart")


# ── Chart constants ────────────────────────────────────────────────────────────
BG      = "#1a1a2e"
CARD_BG = "#161b22"
BULL    = "#26a69a"
BEAR    = "#ef5350"
GRID    = "#2a2a4e"
TEXT    = "#c9d1d9"
DIM     = "#8b949e"
BLUE    = "#58a6ff"
ORANGE  = "#ffb74d"
PURPLE  = "#ba68c8"
AMBER   = "#f59e0b"
GREEN   = "#22c55e"
RED     = "#ef4444"


def _hex(h: str):
    from PySide6.QtGui import QColor
    return QColor(h)


class StockChart:
    """Main chart window for one ticker."""

    def __init__(self, ticker: str, port: int, token: str):
        import pyqtgraph as pg
        from PySide6.QtWidgets import (
            QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
            QLabel, QPushButton, QSizePolicy,
        )
        from PySide6.QtCore import Qt, QTimer
        from PySide6.QtGui import QFont, QColor

        self.ticker = ticker.upper()
        self.port   = port
        self.token  = token

        # ── State ────────────────────────────────────────────────────────────
        self._bars       = []
        self._ts_map     = {}
        self._vwap_pts   = []
        self._wvwap_pts  = []
        self._orb_h      = None
        self._orb_l      = None
        self._orb_h_line = None
        self._orb_l_line = None
        self._signals    = []
        self._lock       = threading.Lock()
        self._last_ts    = 0

        # ── Use fallback CandleItem (standalone — no CT_AlgorithmV3 dep) ─────
        CandleItem = self._make_fallback_candle(pg)
        TimeAxis   = None

        # ── Window setup ─────────────────────────────────────────────────────
        self.win = QMainWindow()
        self.win.setWindowTitle(f"StockNotify — {self.ticker}")
        self.win.resize(1200, 700)

        central = QWidget()
        self.win.setCentralWidget(central)
        layout  = QVBoxLayout(central)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(4)

        self.win.setStyleSheet(f"""
            QMainWindow, QWidget {{ background: {BG}; color: {TEXT}; font-family: 'Segoe UI', sans-serif; }}
            QLabel {{ color: {TEXT}; font-size: 12px; }}
            QPushButton {{
                background: #1e293b; color: {TEXT}; border: 1px solid #334155;
                border-radius: 4px; padding: 4px 10px; font-size: 11px;
            }}
            QPushButton:hover {{ background: #334155; }}
        """)

        # ── Header bar ───────────────────────────────────────────────────────
        hdr = QHBoxLayout()
        hdr.setContentsMargins(4, 2, 4, 2)

        self.lbl_ticker = QLabel(self.ticker)
        f = QFont("Segoe UI", 16, QFont.Weight.Bold)
        self.lbl_ticker.setFont(f)
        self.lbl_ticker.setStyleSheet(f"color: {BLUE};")
        hdr.addWidget(self.lbl_ticker)

        self.lbl_price = QLabel("—")
        fp = QFont("Segoe UI", 14, QFont.Weight.Bold)
        self.lbl_price.setFont(fp)
        hdr.addWidget(self.lbl_price)

        hdr.addSpacing(20)

        self.lbl_vwap  = QLabel("VWAP: —")
        self.lbl_vwap.setStyleSheet(f"color: {BLUE};")
        hdr.addWidget(self.lbl_vwap)

        self.lbl_wvwap = QLabel("W.VWAP: —")
        self.lbl_wvwap.setStyleSheet(f"color: {PURPLE};")
        hdr.addWidget(self.lbl_wvwap)

        self.lbl_orb = QLabel("ORB: Pending")
        self.lbl_orb.setStyleSheet(f"color: {AMBER};")
        hdr.addWidget(self.lbl_orb)

        hdr.addStretch()

        self.lbl_status = QLabel("Loading…")
        self.lbl_status.setStyleSheet(f"color: {DIM}; font-size: 10px;")
        hdr.addWidget(self.lbl_status)

        btn_refresh = QPushButton("⟳ Refresh")
        btn_refresh.clicked.connect(self.refresh)
        hdr.addWidget(btn_refresh)

        hdr_widget = QWidget()
        hdr_widget.setLayout(hdr)
        layout.addWidget(hdr_widget)

        # ── Signal label row ─────────────────────────────────────────────────
        self.lbl_signals = QLabel("")
        self.lbl_signals.setStyleSheet(f"color: {DIM}; font-size: 10px; padding: 0 4px;")
        self.lbl_signals.setWordWrap(True)
        layout.addWidget(self.lbl_signals)

        # ── Chart (pyqtgraph) ────────────────────────────────────────────────
        pg.setConfigOption("background", BG)
        pg.setConfigOption("foreground", TEXT)

        self._time_axis = pg.AxisItem("bottom")

        self._pw = pg.PlotWidget(axisItems={"bottom": self._time_axis})
        self._pw.setBackground(BG)
        self._pw.getAxis("left").setPen(pg.mkPen(DIM))
        self._pw.getAxis("left").setTextPen(pg.mkPen(DIM))
        self._pw.getAxis("bottom").setPen(pg.mkPen(DIM))
        self._pw.getAxis("bottom").setTextPen(pg.mkPen(DIM))
        self._pw.showGrid(x=True, y=True, alpha=0.15)
        self._pw.setMouseEnabled(x=True, y=True)
        self._pw.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        layout.addWidget(self._pw)

        # Candle item
        self._candle = CandleItem()
        self._pw.addItem(self._candle)

        # VWAP lines
        self._vwap_line  = self._pw.plot(pen=pg.mkPen(BLUE,   width=1, cosmetic=True))
        self._wvwap_line = self._pw.plot(pen=pg.mkPen(PURPLE, width=1, style=pg.QtCore.Qt.PenStyle.DashLine, cosmetic=True))

        # Signal scatter
        self._sig_scatter = pg.ScatterPlotItem(size=10, pxMode=True)
        self._pw.addItem(self._sig_scatter)

        layout.setStretch(2, 1)

        # ── Timer: poll API every 60s ─────────────────────────────────────────
        self._qtimer = QTimer()
        self._qtimer.setInterval(60_000)
        self._qtimer.timeout.connect(self.refresh)

        # Initial load in background
        threading.Thread(target=self._bg_load, daemon=True).start()

    def show(self):
        self.win.show()
        self._qtimer.start()

    # ── Data loading ──────────────────────────────────────────────────────────

    def _bg_load(self):
        self._set_status(f"[1/4] Loading {self.ticker} historical parquet…")
        hist_bars = self._fetch_hist_parquet()
        if hist_bars:
            log.info(f"{self.ticker}: loaded {len(hist_bars):,} historical bars from parquet")
            self._set_status(f"[2/4] {len(hist_bars):,} hist bars loaded — fetching live session…")
        else:
            self._set_status(f"[2/4] No local history — fetching live session…")
        api_bars = self._fetch_api_bars()
        n_live = len(api_bars)
        log.info(f"{self.ticker}: {n_live} live bars from API")
        self._set_status(f"[3/4] {len(hist_bars):,} hist + {n_live} live bars — rendering…")
        self._merge_and_render(hist_bars, api_bars)

    def refresh(self):
        threading.Thread(target=self._bg_refresh, daemon=True).start()

    def _bg_refresh(self):
        self._set_status("Refreshing…")
        api_bars = self._fetch_api_bars()
        if api_bars:
            with self._lock:
                existing_ts = {b["t"] for b in (self._bars_raw if hasattr(self, "_bars_raw") else [])}
                new = [b for b in api_bars if b["t"] not in existing_ts]
            if new:
                log.info(f"{self.ticker}: {len(new)} new bars from API")
                self._merge_and_render([], api_bars)
            else:
                self._set_status(f"Up to date — {len(self._bars)} bars")
        else:
            self._set_status("No new bars")

    def _fetch_hist_parquet(self) -> list:
        try:
            import pandas as pd
            from stocknotify import config
            root = getattr(config, "HIST_STORAGE_ROOT", "./data/bars")
            path = os.path.join(root, self.ticker, f"{self.ticker}_1m.parquet")
            if not os.path.exists(path):
                return []
            df = pd.read_parquet(path)
            if df.empty or "ts" not in df.columns:
                return []
            df = df.tail(1500)
            return [
                {"t": int(r["ts"]), "o": float(r["open"]), "h": float(r["high"]),
                 "l": float(r["low"]), "c": float(r["close"]), "v": int(r.get("volume", 0))}
                for _, r in df.iterrows()
                if float(r["open"]) > 0 and float(r["close"]) > 0
            ]
        except Exception as exc:
            log.warning(f"hist parquet load: {exc}")
            return []

    def _fetch_api_bars(self) -> list:
        try:
            import urllib.request, json
            url = f"http://127.0.0.1:{self.port}/api/bars/{self.ticker}?n=500"
            with urllib.request.urlopen(url, timeout=10) as resp:
                return json.loads(resp.read())
        except Exception as exc:
            log.debug(f"API bars fetch: {exc}")
            return []

    def _fetch_api_status(self) -> dict:
        try:
            import urllib.request, json
            url = f"http://127.0.0.1:{self.port}/api/status"
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read())
                return data.get(self.ticker, {})
        except Exception:
            return {}

    def _merge_and_render(self, hist_bars: list, api_bars: list):
        seen = {}
        for b in hist_bars:
            seen[b["t"]] = b
        for b in api_bars:
            seen[b["t"]] = b

        all_bars = sorted(seen.values(), key=lambda b: b["t"])
        if not all_bars:
            self._set_status("No data available")
            return

        indexed = []
        ts_map  = {}
        for i, b in enumerate(all_bars):
            indexed.append((i, b["o"], b["h"], b["l"], b["c"]))
            ts_map[i] = b["t"]

        status = self._fetch_api_status()

        vwap_pts  = [(i, b["vwap"])  for i, b in enumerate(all_bars) if b.get("vwap")]
        wvwap_pts = [(i, b["wvwap"]) for i, b in enumerate(all_bars) if b.get("wvwap")]

        signals = self._fetch_signals_for_chart(ts_map)

        with self._lock:
            self._bars      = indexed
            self._bars_raw  = all_bars
            self._ts_map    = ts_map
            self._vwap_pts  = vwap_pts
            self._wvwap_pts = wvwap_pts
            self._signals   = signals
            self._orb_h     = status.get("orb30_high")
            self._orb_l     = status.get("orb30_low")
            self._orb_set   = status.get("orb30_set", False)
            last_bar = all_bars[-1] if all_bars else None
            last_price = last_bar["c"] if last_bar else None
            vwap  = status.get("vwap")
            wvwap = status.get("wvwap")

        from PySide6.QtCore import QTimer
        QTimer.singleShot(0, lambda: self._qt_render(last_price, vwap, wvwap, status))

    def _fetch_signals_for_chart(self, ts_map: dict) -> list:
        try:
            import urllib.request, json
            url = f"http://127.0.0.1:{self.port}/api/alerts?n=200"
            with urllib.request.urlopen(url, timeout=5) as resp:
                alerts = json.loads(resp.read())
            ts_to_idx = {ts: idx for idx, ts in ts_map.items()}
            signals = []
            for a in alerts:
                if a.get("ticker", "").upper() != self.ticker:
                    continue
                ts_str = a.get("ts") or a.get("bar_time")
                if not ts_str:
                    continue
                try:
                    ts_sec = int(datetime.fromisoformat(ts_str.replace("Z","")).timestamp())
                except Exception:
                    continue
                idx = ts_to_idx.get(ts_sec)
                if idx is None:
                    candidates = [(abs(t - ts_sec), i) for i, t in ts_map.items()]
                    if candidates:
                        _, idx = min(candidates)
                if idx is not None:
                    signals.append((idx, a.get("type", ""), a.get("price", 0)))
            return signals
        except Exception:
            return []

    def _qt_render(self, last_price, vwap, wvwap, status: dict):
        import pyqtgraph as pg

        with self._lock:
            bars      = list(self._bars)
            vwap_pts  = list(self._vwap_pts)
            wvwap_pts = list(self._wvwap_pts)
            signals   = list(self._signals)
            orb_h     = self._orb_h
            orb_l     = self._orb_l
            orb_set   = getattr(self, "_orb_set", False)

        if not bars:
            return

        self._candle.set_data(bars)
        self._pw.autoRange()

        if vwap_pts:
            xs, ys = zip(*vwap_pts)
            self._vwap_line.setData(list(xs), list(ys))
        else:
            self._vwap_line.setData([], [])

        if wvwap_pts:
            xs, ys = zip(*wvwap_pts)
            self._wvwap_line.setData(list(xs), list(ys))
        else:
            self._wvwap_line.setData([], [])

        self._update_orb_lines(orb_h, orb_l, orb_set)
        self._render_signals(signals, bars)
        self._update_header(last_price, vwap, wvwap, orb_h, orb_l, orb_set, len(bars))
        log.info(f"{self.ticker}: rendered {len(bars):,} bars on chart")

    def _update_orb_lines(self, orb_h, orb_l, orb_set):
        import pyqtgraph as pg
        if not orb_set or not orb_h or not orb_l:
            if self._orb_h_line:
                self._pw.removeItem(self._orb_h_line)
                self._orb_h_line = None
            if self._orb_l_line:
                self._pw.removeItem(self._orb_l_line)
                self._orb_l_line = None
            return
        _dash = pg.QtCore.Qt.PenStyle.DashLine
        pen_h = pg.mkPen(AMBER, width=1, style=_dash, cosmetic=True)
        pen_l = pg.mkPen(AMBER, width=1, style=_dash, cosmetic=True)
        if self._orb_h_line is None:
            self._orb_h_line = pg.InfiniteLine(pos=orb_h, angle=0, pen=pen_h,
                                                label=f"ORB Hi {orb_h:.2f}",
                                                labelOpts={"color": AMBER, "position": 0.05, "fill": (0,0,0,100)})
            self._pw.addItem(self._orb_h_line)
        else:
            self._orb_h_line.setPos(orb_h)
        if self._orb_l_line is None:
            self._orb_l_line = pg.InfiniteLine(pos=orb_l, angle=0, pen=pen_l,
                                                label=f"ORB Lo {orb_l:.2f}",
                                                labelOpts={"color": AMBER, "position": 0.05, "fill": (0,0,0,100)})
            self._pw.addItem(self._orb_l_line)
        else:
            self._orb_l_line.setPos(orb_l)

    def _render_signals(self, signals: list, bars: list):
        import pyqtgraph as pg

        SIG_CONFIG = {
            "ORB_30_RETEST_HIGH": {"color": "#fbbf24", "symbol": "t1", "size": 12},
            "ORB_30_RETEST_LOW":  {"color": "#fb923c", "symbol": "t",  "size": 12},
            "ORB_60_RETEST_HIGH": {"color": "#f59e0b", "symbol": "t1", "size": 12},
            "ORB_60_RETEST_LOW":  {"color": "#ea580c", "symbol": "t",  "size": 12},
            "MACD_CROSS_BULL":    {"color": "#22d3ee", "symbol": "t1", "size": 10},
            "MACD_CROSS_BEAR":    {"color": "#f43f5e", "symbol": "t",  "size": 10},
            "VWAP_CROSS_PDH":     {"color": "#facc15", "symbol": "d",  "size": 10},
            "VWAP_CROSS_PDL":     {"color": "#e879f9", "symbol": "d",  "size": 10},
        }

        bar_map = {b[0]: b for b in bars}
        spots = []
        for idx, sig_type, price in signals:
            cfg = SIG_CONFIG.get(sig_type, {"color": DIM, "symbol": "o", "size": 8})
            b   = bar_map.get(idx)
            if b is None:
                continue
            if "UP" in sig_type or "HIGH" in sig_type or "BULL" in sig_type:
                y = b[3] - (b[2] - b[3]) * 0.3
            else:
                y = b[2] + (b[2] - b[3]) * 0.3
            spots.append({
                "pos": (idx, y),
                "symbol": cfg["symbol"],
                "size": cfg["size"],
                "brush": pg.mkBrush(cfg["color"]),
                "pen": pg.mkPen(cfg["color"]),
            })
        self._sig_scatter.setData(spots)

    def _update_header(self, last_price, vwap, wvwap, orb_h, orb_l, orb_set, n_bars):
        if last_price:
            px_str = f"${last_price:.2f}"
            color  = GREEN if (vwap and last_price > vwap) else RED
            self.lbl_price.setText(px_str)
            self.lbl_price.setStyleSheet(f"color: {color}; font-size: 14px; font-weight: bold;")
        self.lbl_vwap.setText(f"VWAP: ${vwap:.2f}" if vwap else "VWAP: —")
        self.lbl_wvwap.setText(f"W.VWAP: ${wvwap:.2f}" if wvwap else "W.VWAP: —")
        if orb_set and orb_h and orb_l:
            self.lbl_orb.setText(f"ORB30: ${orb_l:.2f} – ${orb_h:.2f}")
        else:
            self.lbl_orb.setText("ORB30: Pending")
        self._set_status(f"{n_bars:,} bars · refreshes every 60s")

    def _set_status(self, msg: str):
        try:
            from PySide6.QtCore import QTimer
            lbl = self.lbl_status
            QTimer.singleShot(0, lambda: lbl.setText(msg))
        except Exception:
            pass

    @staticmethod
    def _make_fallback_candle(pg):
        """Minimal CandleItem."""

        class CandleItem(pg.GraphicsObject):
            def __init__(self):
                super().__init__()
                self._data = []

            def set_data(self, data):
                self._data = data
                self.update()

            def boundingRect(self):
                if not self._data:
                    return pg.QtCore.QRectF(0, 0, 1, 1)
                xs = [d[0] for d in self._data]
                ys = [y for d in self._data for y in (d[3], d[2])]
                return pg.QtCore.QRectF(min(xs), min(ys), max(xs)-min(xs)+1, max(ys)-min(ys))

            def paint(self, p, *args):
                for x, o, h, l, c in self._data:
                    bull  = c >= o
                    color = "#26a69a" if bull else "#ef5350"
                    pen   = pg.mkPen(color, width=1)
                    p.setPen(pen)
                    p.drawLine(pg.QtCore.QPointF(x, l), pg.QtCore.QPointF(x, h))
                    bt, bb = max(o, c), min(o, c)
                    if bt - bb > 0.001:
                        p.setBrush(pg.mkBrush(color))
                        p.drawRect(pg.QtCore.QRectF(x-0.35, bb, 0.7, bt-bb))

        return CandleItem


def main():
    parser = argparse.ArgumentParser(description="StockNotify Chart")
    parser.add_argument("ticker",          type=str,               help="Ticker symbol (e.g. NVDA)")
    parser.add_argument("--port",          type=int, default=8436, help="StockNotify dashboard port")
    parser.add_argument("--token",         type=str, default="",   help="Auth token")
    args = parser.parse_args()

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication(sys.argv)
    app.setApplicationName("StockNotify Chart")

    chart = StockChart(args.ticker, args.port, args.token)
    chart.show()

    from PySide6.QtCore import QTimer
    QTimer.singleShot(500, chart.refresh)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
