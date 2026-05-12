"""
StockNotify — Analysis engine

Signal types fired per ticker:
  ORB_30_RETEST_HIGH — 30-min ORB retest: price returns to ORB30 high after breakout
  ORB_30_RETEST_LOW  — 30-min ORB retest: price returns to ORB30 low after breakdown
  ORB_60_RETEST_HIGH — 1-hr ORB retest: price returns to ORB60 high after breakout
  ORB_60_RETEST_LOW  — 1-hr ORB retest: price returns to ORB60 low after breakdown
  MACD_CROSS_BULL    — weekly MACD histogram turns positive (bull cross, once/week)
  MACD_CROSS_BEAR    — weekly MACD histogram turns negative (bear cross, once/week)
  VWAP_CROSS_PDH     — session VWAP crosses prior day's high (every cross)
  VWAP_CROSS_PDL     — session VWAP crosses prior day's low (every cross)
"""

import threading
import logging
from datetime import datetime, timedelta
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import pytz

log = logging.getLogger("sn_analysis")
ET  = pytz.timezone("America/New_York")


# ── Signal ────────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    ticker:      str
    signal_type: str
    price:       float
    vwap:        Optional[float]
    wvwap:       Optional[float]
    bar_time:    datetime
    extra:       dict = field(default_factory=dict)

    def to_telegram(self) -> str:
        EMOJI = {
            "ORB_30_RETEST_HIGH": "🎯",   "ORB_30_RETEST_LOW": "🎯",
            "ORB_60_RETEST_HIGH": "🎯🎯", "ORB_60_RETEST_LOW": "🎯🎯",
            "MACD_CROSS_BULL":    "📊📈",  "MACD_CROSS_BEAR":   "📊📉",
            "VWAP_CROSS_PDH":     "🔑",    "VWAP_CROSS_PDL":    "🗝",
        }
        em  = EMOJI.get(self.signal_type, "📊")
        st  = self.signal_type
        ex  = self.extra or {}
        ts  = self.bar_time.strftime("%H:%M ET")
        px  = self.price
        v   = self.vwap
        vf  = f"${v:.2f}" if v else "n/a"

        # ── ORB Retest ────────────────────────────────────────────────────────
        if st in ("ORB_30_RETEST_HIGH", "ORB_30_RETEST_LOW",
                  "ORB_60_RETEST_HIGH", "ORB_60_RETEST_LOW"):
            tf_name = "1-hr" if "60" in st else "30-min"
            up      = "HIGH" in st
            h       = ex.get("orb_high", 0)
            l       = ex.get("orb_low",  0)
            level   = h if up else l
            tol_pct = ex.get("tol_pct", 0.3)
            dist    = ex.get("dist_from_level", abs(px - level) / level * 100 if level else 0)
            zone    = "buy zone — ORB high acts as support" if up else "short zone — ORB low acts as resistance"
            return (
                f"{em} *{self.ticker}* — {tf_name} ORB {'High' if up else 'Low'} Retest\n"
                f"Price: *${px:.2f}* | VWAP: {vf} | {ts}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"*What fired:* price retested {tf_name} ORB {'high' if up else 'low'} after {'breakout' if up else 'breakdown'}\n"
                f"*ORB level:* ${level:.2f} | range: ${l:.2f} – ${h:.2f}\n"
                f"*Tolerance band:* ±{tol_pct:.2f}% | distance: {dist:.3f}%\n"
                f"*Trade setup:* {zone}"
            )

        # ── VWAP × PDH ────────────────────────────────────────────────────────
        elif st == "VWAP_CROSS_PDH":
            pdh      = ex.get("pdh", 0)
            dirn     = ex.get("direction", "above")
            arrow    = "↑ above" if dirn == "above" else "↓ below"
            dist     = (v - pdh) / pdh * 100 if (v and pdh) else 0
            sig_line = "VWAP above PDH signals intraday bullish momentum" if dirn == "above" else "VWAP below PDH signals intraday bearish momentum"
            return (
                f"{em} *{self.ticker}* — VWAP × Prior Day High {arrow}\n"
                f"VWAP: *${v:.2f}* | PDH: ${pdh:.2f} | Price: ${px:.2f} | {ts}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"*What fired:* session VWAP crossed {arrow} prior day's high\n"
                f"*VWAP:* ${v:.2f} | *PDH:* ${pdh:.2f} | gap: {dist:+.2f}%\n"
                f"*PDH:* yesterday's session high price\n"
                f"*Significance:* {sig_line}"
            )

        # ── VWAP × PDL ────────────────────────────────────────────────────────
        elif st == "VWAP_CROSS_PDL":
            pdl   = ex.get("pdl", 0)
            dirn  = ex.get("direction", "above")
            arrow = "↑ above" if dirn == "above" else "↓ below"
            dist  = (v - pdl) / pdl * 100 if (v and pdl) else 0
            return (
                f"{em} *{self.ticker}* — VWAP × Prior Day Low {arrow}\n"
                f"VWAP: *${v:.2f}* | PDL: ${pdl:.2f} | Price: ${px:.2f} | {ts}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"*What fired:* session VWAP crossed {arrow} prior day's low\n"
                f"*VWAP:* ${v:.2f} | *PDL:* ${pdl:.2f} | gap: {dist:+.2f}%\n"
                f"*PDL:* yesterday's session low price\n"
                f"*Significance:* VWAP {'above PDL — holding above prior lows' if dirn == 'above' else 'below PDL — breaking below prior lows'}"
            )

        # ── Weekly MACD cross ─────────────────────────────────────────────────
        elif st in ("MACD_CROSS_BULL", "MACD_CROSS_BEAR"):
            macd_v = ex.get("macd", 0)
            sig_v  = ex.get("signal", 0)
            hist   = macd_v - sig_v
            week   = ex.get("week", "?")
            bull   = st == "MACD_CROSS_BULL"
            return (
                f"{em} *{self.ticker}* — Weekly MACD {'Bull ↑' if bull else 'Bear ↓'} Cross\n"
                f"Price: *${px:.2f}* | Week: {week} | {ts}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"*What fired:* weekly MACD histogram flipped {'positive' if bull else 'negative'}\n"
                f"*MACD line* (EMA12−EMA26): {macd_v:+.4f}\n"
                f"*Signal line* (EMA9 of MACD): {sig_v:+.4f}\n"
                f"*Histogram* (MACD−Signal): {hist:+.4f}\n"
                f"*Trigger:* EMA12(weekly closes) crossed {'above' if bull else 'below'} EMA26"
            )

        # ── Fallback ──────────────────────────────────────────────────────────
        else:
            return (
                f"{em} *{self.ticker}* — {st.replace('_', ' ')}\n"
                f"Price: *${px:.2f}* | {ts}"
            )

    def to_ws_dict(self) -> dict:
        return {
            "type":        "signal",
            "ticker":      self.ticker,
            "signal_type": self.signal_type,
            "price":       self.price,
            "vwap":        self.vwap,
            "wvwap":       self.wvwap,
            "bar_time":    self.bar_time.isoformat(),
            "extra":       self.extra,
        }


# ── Per-ticker state ──────────────────────────────────────────────────────────

class TickerState:
    def __init__(self, config):
        self.config = config
        self.bars: deque = deque(maxlen=config.MAX_BARS_STORED)

        # Daily VWAP
        self._dvwap_pw:  float = 0.0
        self._dvwap_vol: float = 0.0
        self.vwap: Optional[float] = None

        # Weekly VWAP (display only — not used for signal firing)
        self._wvwap_pw:  float = 0.0
        self._wvwap_vol: float = 0.0
        self.wvwap: Optional[float] = None
        self._last_week: Optional[tuple] = None

        # 30-min ORB
        self.orb30_high: Optional[float] = None
        self.orb30_low:  Optional[float] = None
        self.orb30_set:  bool = False
        self._orb30_bars: list = []
        self._orb30_broke_up:           bool = False   # price closed above ORB30 high (silent)
        self._orb30_broke_dn:           bool = False   # price closed below ORB30 low (silent)
        self._orb30_broke_up_bar_time:  Optional[datetime] = None  # bar_time of first break
        self._orb30_broke_dn_bar_time:  Optional[datetime] = None
        self._alerted_orb30_ret_up: bool = False   # ORB_30_RETEST_HIGH fired
        self._alerted_orb30_ret_dn: bool = False   # ORB_30_RETEST_LOW fired

        # 60-min ORB
        self.orb60_high: Optional[float] = None
        self.orb60_low:  Optional[float] = None
        self.orb60_set:  bool = False
        self._orb60_bars: list = []
        self._orb60_broke_up:           bool = False
        self._orb60_broke_dn:           bool = False
        self._orb60_broke_up_bar_time:  Optional[datetime] = None
        self._orb60_broke_dn_bar_time:  Optional[datetime] = None
        self._alerted_orb60_ret_up: bool = False
        self._alerted_orb60_ret_dn: bool = False

        # Daily/weekly reset tracking
        self._last_date = None

        # Prior day High/Low — seed via AnalysisEngine._seed_ticker on startup
        self._prev_day_high: Optional[float] = None
        self._prev_day_low:  Optional[float] = None
        self._today_high:    Optional[float] = None
        self._today_low:     Optional[float] = None
        self._pdh_side: Optional[bool] = None   # VWAP above PDH on prev bar?
        self._pdl_side: Optional[bool] = None   # VWAP above PDL on prev bar?

        # Weekly MACD (EMA12/26/signal on weekly closes) — seed on startup
        self._macd_ema12:      Optional[float] = None
        self._macd_ema26:      Optional[float] = None
        self._macd_signal_ema: Optional[float] = None
        self._macd_hist_prev:  Optional[float] = None
        self._macd_week_key:   Optional[str]   = None
        self._macd_week_close: Optional[float] = None
        self._alerted_macd_bull: bool = False
        self._alerted_macd_bear: bool = False

    # ── resets ───────────────────────────────────────────────────────────────

    def _reset_daily(self, today):
        self._dvwap_pw  = 0.0
        self._dvwap_vol = 0.0
        self.vwap       = None

        self.orb30_high  = None
        self.orb30_low   = None
        self.orb30_set   = False
        self._orb30_bars = []
        self._orb30_broke_up           = False
        self._orb30_broke_dn           = False
        self._orb30_broke_up_bar_time  = None
        self._orb30_broke_dn_bar_time  = None
        self._alerted_orb30_ret_up     = False
        self._alerted_orb30_ret_dn     = False

        self.orb60_high  = None
        self.orb60_low   = None
        self.orb60_set   = False
        self._orb60_bars = []
        self._orb60_broke_up           = False
        self._orb60_broke_dn           = False
        self._orb60_broke_up_bar_time  = None
        self._orb60_broke_dn_bar_time  = None
        self._alerted_orb60_ret_up     = False
        self._alerted_orb60_ret_dn     = False

        # Carry today's H/L forward as prior-day H/L
        if self._today_high is not None:
            self._prev_day_high = self._today_high
        if self._today_low is not None:
            self._prev_day_low  = self._today_low
        self._today_high = None
        self._today_low  = None
        self._pdh_side   = None
        self._pdl_side   = None

        self._last_date = today

    def _reset_weekly(self):
        self._wvwap_pw  = 0.0
        self._wvwap_vol = 0.0
        self.wvwap      = None
        # MACD alert flags reset each week (EMA state persists)
        self._alerted_macd_bull = False
        self._alerted_macd_bear = False

    # ── weekly MACD ──────────────────────────────────────────────────────────

    def _macd_tick_week(self, week_close: float) -> Optional[str]:
        """Update MACD EMAs with a completed weekly close. Returns 'bull', 'bear', or None."""
        k12 = 2.0 / 13
        k26 = 2.0 / 27
        k9  = 2.0 / 10

        if self._macd_ema12 is None:
            self._macd_ema12      = week_close
            self._macd_ema26      = week_close
            self._macd_signal_ema = 0.0
            self._macd_hist_prev  = 0.0
            return None

        self._macd_ema12 = week_close * k12 + self._macd_ema12 * (1 - k12)
        self._macd_ema26 = week_close * k26 + self._macd_ema26 * (1 - k26)
        macd_line        = self._macd_ema12 - self._macd_ema26

        prev_sig = self._macd_signal_ema if self._macd_signal_ema is not None else macd_line
        self._macd_signal_ema = macd_line * k9 + prev_sig * (1 - k9)
        hist      = macd_line - self._macd_signal_ema
        prev_hist = self._macd_hist_prev
        self._macd_hist_prev = hist

        if prev_hist is not None and prev_hist <= 0 < hist:
            return "bull"
        if prev_hist is not None and prev_hist >= 0 > hist:
            return "bear"
        return None

    # ── main process ─────────────────────────────────────────────────────────

    def process_bar(self, ticker: str, bar: dict) -> list:
        bt:    datetime = bar["bar_time"]
        today  = bt.date()
        close  = bar["close"]
        volume = bar["volume"]
        pw_sum = bar["pw_sum"]

        market_open      = bt.replace(hour=9, minute=30, second=0, microsecond=0)
        orb30_close_time = market_open + timedelta(minutes=self.config.ORB_30_MINUTES)
        orb60_close_time = market_open + timedelta(minutes=self.config.ORB_60_MINUTES)

        # Daily reset
        if self._last_date != today:
            self._reset_daily(today)

        # Weekly reset (Monday or new ISO week)
        iso_week = bt.isocalendar()[:2]
        if self._last_week != iso_week:
            self._last_week = iso_week
            self._reset_weekly()

        # ── Track today's session H/L ─────────────────────────────────────────
        if self._today_high is None or bar["high"] > self._today_high:
            self._today_high = bar["high"]
        if self._today_low is None or bar["low"] < self._today_low:
            self._today_low = bar["low"]

        # ── Daily VWAP ────────────────────────────────────────────────────────
        self._dvwap_pw  += pw_sum
        self._dvwap_vol += volume
        self.vwap = self._dvwap_pw / self._dvwap_vol if self._dvwap_vol > 0 else None

        # ── Weekly VWAP (display only) ────────────────────────────────────────
        self._wvwap_pw  += pw_sum
        self._wvwap_vol += volume
        self.wvwap = self._wvwap_pw / self._wvwap_vol if self._wvwap_vol > 0 else None

        # ── 30-min ORB collection ─────────────────────────────────────────────
        # Collect bars 9:30–9:59 only (bt < orb30_close_time excludes the 10:00 bar)
        if not self.orb30_set:
            if bt >= market_open and bt < orb30_close_time:
                self._orb30_bars.append(bar)
            if bt >= orb30_close_time and self._orb30_bars:
                self.orb30_high = max(b["high"] for b in self._orb30_bars)
                self.orb30_low  = min(b["low"]  for b in self._orb30_bars)
                self.orb30_set  = True
                log.info(f"{ticker} ORB-30 set: ${self.orb30_low:.2f} – ${self.orb30_high:.2f}")

        # ── 60-min ORB collection ─────────────────────────────────────────────
        # Collect bars 9:30–10:29 only (bt < orb60_close_time excludes the 10:30 bar)
        if not self.orb60_set:
            if bt >= market_open and bt < orb60_close_time:
                self._orb60_bars.append(bar)
            if bt >= orb60_close_time and self._orb60_bars:
                self.orb60_high = max(b["high"] for b in self._orb60_bars)
                self.orb60_low  = min(b["low"]  for b in self._orb60_bars)
                self.orb60_set  = True
                log.info(f"{ticker} ORB-60 set: ${self.orb60_low:.2f} – ${self.orb60_high:.2f}")

        # ── Store bar ─────────────────────────────────────────────────────────
        self.bars.append({
            **bar,
            "vwap":  self.vwap,
            "wvwap": self.wvwap,
        })

        signals = []

        # ── Weekly MACD crossover ─────────────────────────────────────────────
        year, week_num, _ = bt.isocalendar()
        cur_week_key = f"{year}-W{week_num:02d}"
        if self._macd_week_key is None:
            self._macd_week_key = cur_week_key
        elif cur_week_key != self._macd_week_key:
            if self._macd_week_close is not None:
                cross = self._macd_tick_week(self._macd_week_close)
                if cross == "bull" and not self._alerted_macd_bull:
                    self._alerted_macd_bull = True
                    macd_v = (self._macd_ema12 - self._macd_ema26) if (self._macd_ema12 and self._macd_ema26) else 0.0
                    sig_v  = self._macd_signal_ema or 0.0
                    signals.append(Signal(
                        ticker=ticker, signal_type="MACD_CROSS_BULL",
                        price=close, vwap=self.vwap, wvwap=self.wvwap, bar_time=bt,
                        extra={"macd": round(macd_v, 4), "signal": round(sig_v, 4),
                               "week": self._macd_week_key},
                    ))
                elif cross == "bear" and not self._alerted_macd_bear:
                    self._alerted_macd_bear = True
                    macd_v = (self._macd_ema12 - self._macd_ema26) if (self._macd_ema12 and self._macd_ema26) else 0.0
                    sig_v  = self._macd_signal_ema or 0.0
                    signals.append(Signal(
                        ticker=ticker, signal_type="MACD_CROSS_BEAR",
                        price=close, vwap=self.vwap, wvwap=self.wvwap, bar_time=bt,
                        extra={"macd": round(macd_v, 4), "signal": round(sig_v, 4),
                               "week": self._macd_week_key},
                    ))
            self._macd_week_key     = cur_week_key
            self._alerted_macd_bull = False
            self._alerted_macd_bear = False
        self._macd_week_close = close

        # ── VWAP cross Prior Day High ─────────────────────────────────────────
        if self.vwap is not None and self._prev_day_high is not None:
            above_pdh = self.vwap >= self._prev_day_high
            if self._pdh_side is not None and above_pdh != self._pdh_side:
                dirn = "above" if above_pdh else "below"
                signals.append(Signal(
                    ticker=ticker, signal_type="VWAP_CROSS_PDH",
                    price=close, vwap=self.vwap, wvwap=self.wvwap, bar_time=bt,
                    extra={"direction": dirn, "pdh": round(self._prev_day_high, 2)},
                ))
            self._pdh_side = above_pdh

        # ── VWAP cross Prior Day Low ──────────────────────────────────────────
        if self.vwap is not None and self._prev_day_low is not None:
            above_pdl = self.vwap >= self._prev_day_low
            if self._pdl_side is not None and above_pdl != self._pdl_side:
                dirn = "above" if above_pdl else "below"
                signals.append(Signal(
                    ticker=ticker, signal_type="VWAP_CROSS_PDL",
                    price=close, vwap=self.vwap, wvwap=self.wvwap, bar_time=bt,
                    extra={"direction": dirn, "pdl": round(self._prev_day_low, 2)},
                ))
            self._pdl_side = above_pdl

        # ── 30-min ORB retest ─────────────────────────────────────────────────
        if self.orb30_set and self.orb30_high and self.orb30_low:
            tol = getattr(self.config, "ORB_RETEST_TOL_PCT", 0.3) / 100
            # Track breaks silently; record the breakout bar_time so the retest
            # cannot fire on the same bar that first crossed the ORB level.
            if close > self.orb30_high and not self._orb30_broke_up:
                self._orb30_broke_up           = True
                self._orb30_broke_up_bar_time  = bt
            elif close < self.orb30_low and not self._orb30_broke_dn:
                self._orb30_broke_dn           = True
                self._orb30_broke_dn_bar_time  = bt
            # Fire retest: price must have returned to the level on a LATER bar
            if (self._orb30_broke_up and not self._alerted_orb30_ret_up
                    and self._orb30_broke_up_bar_time != bt):
                dist_h = abs(close - self.orb30_high) / self.orb30_high
                if dist_h <= tol:
                    signals.append(Signal(
                        ticker=ticker, signal_type="ORB_30_RETEST_HIGH",
                        price=close, vwap=self.vwap, wvwap=self.wvwap, bar_time=bt,
                        extra={"orb_high": self.orb30_high, "orb_low": self.orb30_low,
                               "tol_pct": round(tol * 100, 2),
                               "dist_from_level": round(dist_h * 100, 3)},
                    ))
                    self._alerted_orb30_ret_up = True
            if (self._orb30_broke_dn and not self._alerted_orb30_ret_dn
                    and self._orb30_broke_dn_bar_time != bt):
                dist_l = abs(close - self.orb30_low) / self.orb30_low
                if dist_l <= tol:
                    signals.append(Signal(
                        ticker=ticker, signal_type="ORB_30_RETEST_LOW",
                        price=close, vwap=self.vwap, wvwap=self.wvwap, bar_time=bt,
                        extra={"orb_high": self.orb30_high, "orb_low": self.orb30_low,
                               "tol_pct": round(tol * 100, 2),
                               "dist_from_level": round(dist_l * 100, 3)},
                    ))
                    self._alerted_orb30_ret_dn = True

        # ── 60-min ORB retest ─────────────────────────────────────────────────
        if self.orb60_set and self.orb60_high and self.orb60_low:
            tol = getattr(self.config, "ORB_RETEST_TOL_PCT", 0.3) / 100
            if close > self.orb60_high and not self._orb60_broke_up:
                self._orb60_broke_up           = True
                self._orb60_broke_up_bar_time  = bt
            elif close < self.orb60_low and not self._orb60_broke_dn:
                self._orb60_broke_dn           = True
                self._orb60_broke_dn_bar_time  = bt
            if (self._orb60_broke_up and not self._alerted_orb60_ret_up
                    and self._orb60_broke_up_bar_time != bt):
                dist_h = abs(close - self.orb60_high) / self.orb60_high
                if dist_h <= tol:
                    signals.append(Signal(
                        ticker=ticker, signal_type="ORB_60_RETEST_HIGH",
                        price=close, vwap=self.vwap, wvwap=self.wvwap, bar_time=bt,
                        extra={"orb_high": self.orb60_high, "orb_low": self.orb60_low,
                               "tol_pct": round(tol * 100, 2),
                               "dist_from_level": round(dist_h * 100, 3)},
                    ))
                    self._alerted_orb60_ret_up = True
            if (self._orb60_broke_dn and not self._alerted_orb60_ret_dn
                    and self._orb60_broke_dn_bar_time != bt):
                dist_l = abs(close - self.orb60_low) / self.orb60_low
                if dist_l <= tol:
                    signals.append(Signal(
                        ticker=ticker, signal_type="ORB_60_RETEST_LOW",
                        price=close, vwap=self.vwap, wvwap=self.wvwap, bar_time=bt,
                        extra={"orb_high": self.orb60_high, "orb_low": self.orb60_low,
                               "tol_pct": round(tol * 100, 2),
                               "dist_from_level": round(dist_l * 100, 3)},
                    ))
                    self._alerted_orb60_ret_dn = True

        return signals


# ── Engine ────────────────────────────────────────────────────────────────────

class AnalysisEngine:
    """Thread-safe engine — manages TickerState per ticker, dispatches bar events."""

    def __init__(self, config, on_signal, on_bar=None):
        self.config    = config
        self.on_signal = on_signal
        self.on_bar    = on_bar
        self._lock     = threading.Lock()
        self._states   = {t: TickerState(config) for t in self._load_tickers()}
        # Seed MACD state and prior-day H/L from historical parquet (non-blocking)
        self._seed_all()

    def _seed_all(self):
        """Seed each ticker's MACD and PDH/PDL from parquet in parallel threads."""
        import threading as _thr
        def _do(ticker):
            try:
                self._seed_ticker(ticker)
            except Exception as exc:
                log.warning(f"{ticker}: seed failed — {exc}")
        threads = [_thr.Thread(target=_do, args=(t,), daemon=True)
                   for t in list(self._states)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=30)
        log.info("Historical seed complete for all tickers")

    def _seed_ticker(self, ticker: str):
        """Load parquet → seed EMA12/26/signal for weekly MACD + prior-day H/L."""
        import os
        import pandas as pd

        root = getattr(self.config, "HIST_STORAGE_ROOT", "D:/CentralFolder/STOCKNOTIFY")
        path = os.path.join(root, ticker, f"{ticker}_1m.parquet")
        if not os.path.exists(path):
            log.debug(f"{ticker}: no parquet, MACD/PDH skipped")
            return

        df = pd.read_parquet(path, columns=["ts", "high", "low", "close"])
        if df.empty:
            return

        df["dt"] = (pd.to_datetime(df["ts"], unit="s", utc=True)
                    .dt.tz_convert("America/New_York"))
        df = df.sort_values("dt").reset_index(drop=True)

        state    = self._states[ticker]
        today_et = datetime.now(ET).date()

        # ── Prior day High/Low ────────────────────────────────────────────────
        for back in range(1, 8):
            d   = today_et - timedelta(days=back)
            sub = df[df["dt"].dt.date == d]
            if not sub.empty:
                state._prev_day_high = float(sub["high"].max())
                state._prev_day_low  = float(sub["low"].min())
                log.info(f"{ticker}: PDH={state._prev_day_high:.2f}  "
                         f"PDL={state._prev_day_low:.2f}  (from {d})")
                break

        # ── Weekly MACD — full EMA pass over all weekly closes ────────────────
        ic = df["dt"].dt.isocalendar()
        df["week_key"] = (ic["year"].astype(str) + "-W"
                          + ic["week"].astype(str).str.zfill(2))
        weekly = (df.groupby("week_key", sort=True)["close"]
                    .last()
                    .reset_index())
        if len(weekly) < 2:
            return

        k12 = 2.0 / 13;  k26 = 2.0 / 27;  k9 = 2.0 / 10
        ema12 = float(weekly["close"].iloc[0])
        ema26 = ema12
        sig   = 0.0
        hist  = 0.0
        for c in weekly["close"].iloc[1:]:
            c     = float(c)
            ema12 = c * k12 + ema12 * (1 - k12)
            ema26 = c * k26 + ema26 * (1 - k26)
            macd  = ema12 - ema26
            sig   = macd * k9 + sig * (1 - k9)
            hist  = macd - sig

        state._macd_ema12      = ema12
        state._macd_ema26      = ema26
        state._macd_signal_ema = sig
        state._macd_hist_prev  = hist

        now_et = datetime.now(ET)
        iso    = now_et.isocalendar()
        state._macd_week_key = f"{iso[0]}-W{iso[1]:02d}"
        today_df = df[df["dt"].dt.date == today_et]
        state._macd_week_close = (float(today_df["close"].iloc[-1])
                                  if not today_df.empty
                                  else float(weekly["close"].iloc[-1]))

        log.info(f"{ticker}: MACD seeded — ema12={ema12:.3f}  ema26={ema26:.3f}  "
                 f"hist={hist:+.4f}  week={state._macd_week_key}")

    def _load_tickers(self) -> list:
        import json, os
        f = os.path.join(os.path.dirname(__file__), "tickers.json")
        if os.path.exists(f):
            try:
                return json.load(open(f))["tickers"]
            except Exception:
                pass
        return list(self.config.TICKERS)

    # ── ticker management ────────────────────────────────────────────────────

    def add_ticker(self, ticker: str):
        ticker = ticker.upper().strip()
        with self._lock:
            if ticker not in self._states:
                self._states[ticker] = TickerState(self.config)
                log.info(f"Added ticker: {ticker}")

    def remove_ticker(self, ticker: str):
        ticker = ticker.upper().strip()
        with self._lock:
            self._states.pop(ticker, None)
            log.info(f"Removed ticker: {ticker}")

    def get_tickers(self) -> list:
        with self._lock:
            return sorted(self._states.keys())

    # ── bar event ────────────────────────────────────────────────────────────

    def on_bar_complete(self, ticker: str, bar: dict):
        with self._lock:
            state = self._states.get(ticker)
            if not state:
                return
            signals = state.process_bar(ticker, bar)
            bar_out = {
                "type":    "bar",
                "ticker":  ticker,
                "t":       int(bar["bar_time"].timestamp()),
                "o":       round(bar["open"],  2),
                "h":       round(bar["high"],  2),
                "l":       round(bar["low"],   2),
                "c":       round(bar["close"], 2),
                "v":       bar["volume"],
                "vwap":    round(state.vwap,  2) if state.vwap  else None,
                "wvwap":   round(state.wvwap, 2) if state.wvwap else None,
                "orb30_h": round(state.orb30_high, 2) if state.orb30_high else None,
                "orb30_l": round(state.orb30_low,  2) if state.orb30_low  else None,
                "orb60_h": round(state.orb60_high, 2) if state.orb60_high else None,
                "orb60_l": round(state.orb60_low,  2) if state.orb60_low  else None,
            }

        if self.on_bar:
            try:
                self.on_bar(bar_out)
            except Exception as exc:
                log.error(f"on_bar broadcast error: {exc}")

        for sig in signals:
            log.info(f"SIGNAL {sig.signal_type}: {ticker} @ ${sig.price:.2f}")
            sig.extra["_bars"] = list(state.bars)
            try:
                self.on_signal(ticker, sig)
            except Exception as exc:
                log.error(f"on_signal error: {exc}")

    # ── status queries ───────────────────────────────────────────────────────

    def get_status(self) -> dict:
        result = {}
        with self._lock:
            for ticker, state in self._states.items():
                last_bar = state.bars[-1] if state.bars else None
                result[ticker] = {
                    "vwap":          round(state.vwap,      2) if state.vwap      else None,
                    "wvwap":         round(state.wvwap,     2) if state.wvwap     else None,
                    "orb30_high":    round(state.orb30_high, 2) if state.orb30_high else None,
                    "orb30_low":     round(state.orb30_low,  2) if state.orb30_low  else None,
                    "orb30_set":     state.orb30_set,
                    "orb60_high":    round(state.orb60_high, 2) if state.orb60_high else None,
                    "orb60_low":     round(state.orb60_low,  2) if state.orb60_low  else None,
                    "orb60_set":     state.orb60_set,
                    "last_price":    round(last_bar["close"], 2) if last_bar else None,
                    "last_bar_time": last_bar["bar_time"].isoformat() if last_bar else None,
                    "bars_today":    len(state.bars),
                }
        return result

    def get_bars(self, ticker: str, n: int = 200) -> list:
        with self._lock:
            state = self._states.get(ticker.upper())
            if not state:
                return []
            bars = list(state.bars)[-n:]
        return [
            {
                "t":     int(b["bar_time"].timestamp()),
                "o":     round(b["open"],  2),
                "h":     round(b["high"],  2),
                "l":     round(b["low"],   2),
                "c":     round(b["close"], 2),
                "v":     b["volume"],
                "vwap":  round(b["vwap"],  2) if b.get("vwap")  else None,
                "wvwap": round(b["wvwap"], 2) if b.get("wvwap") else None,
            }
            for b in bars
        ]
