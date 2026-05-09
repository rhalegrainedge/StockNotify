"""
StockNotify — Signal chart generator

Creates a dark-themed PNG candlestick chart when an indicator fires:
  - Top panel : candlesticks + VWAP + W.VWAP + ORB/PDH/PDL levels
  - Bottom panel: indicator-specific distance/momentum line (where relevant)
  - Vertical accent line + triangle marker at the triggering bar

Returns PNG bytes for Telegram sendPhoto delivery.
Called from sn_alerts.TelegramAlerter.send().
"""

import io
import logging

import matplotlib
matplotlib.use("Agg")          # headless — must be before pyplot
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.ticker import FuncFormatter

import pytz

log = logging.getLogger("sn_chartgen")
ET  = pytz.timezone("America/New_York")

# ── Colors ────────────────────────────────────────────────────────────────────
SIG_COLOR = {
    "ORB_30_RETEST_HIGH": "#fbbf24",
    "ORB_30_RETEST_LOW":  "#fb923c",
    "ORB_60_RETEST_HIGH": "#f59e0b",
    "ORB_60_RETEST_LOW":  "#ea580c",
    "MACD_CROSS_BULL":    "#22d3ee",
    "MACD_CROSS_BEAR":    "#f43f5e",
    "VWAP_CROSS_PDH":     "#facc15",
    "VWAP_CROSS_PDL":     "#e879f9",
}
_BG    = "#0d1117"
_PANEL = "#0a0e17"
_GRID  = "#1a2032"
_TEXT  = "#94a3b8"
_UP    = "#22c55e"
_DOWN  = "#ef4444"
_VWAP  = "#60a5fa"
_WVWAP = "#67e8f9"

_IND_PANEL_TYPES = {
    "MACD_CROSS_BULL", "MACD_CROSS_BEAR",
}


def _style(ax):
    ax.set_facecolor(_PANEL)
    ax.tick_params(colors=_TEXT, labelsize=7, length=2, width=0.5)
    for sp in ax.spines.values():
        sp.set_color(_GRID)
    ax.grid(axis="y", color=_GRID, linewidth=0.4, alpha=0.5)
    ax.grid(axis="x", visible=False)


def _ema(values, period):
    k, out = 2 / (period + 1), []
    for v in values:
        out.append(v if not out else v * k + out[-1] * (1 - k))
    return out


def generate_signal_chart(signal, bars: list):
    """
    Build a PNG image for the given signal.

    Parameters
    ----------
    signal : Signal (sn_analysis.Signal dataclass)
    bars   : list[dict]  —  recent 1-min bars; each dict has keys:
             bar_time (datetime, tz-aware ET), open, high, low, close,
             volume, vwap, wvwap

    Returns
    -------
    bytes (PNG) or None on failure
    """
    if not bars:
        return None

    try:
        sig_type  = signal.signal_type
        sig_color = SIG_COLOR.get(sig_type, "#94a3b8")
        extra     = signal.extra or {}

        # ── ET-aware signal time ───────────────────────────────────────────────
        sig_et = signal.bar_time
        if not sig_et.tzinfo:
            sig_et = ET.localize(sig_et)
        else:
            sig_et = sig_et.astimezone(ET)
        today = sig_et.date()

        # ── Filter to today's session ──────────────────────────────────────────
        session = []
        for b in bars:
            bt = b.get("bar_time")
            if bt is None:
                continue
            bt_et = bt.astimezone(ET) if bt.tzinfo else ET.localize(bt)
            if bt_et.date() == today:
                session.append({**b, "_et": bt_et})

        if not session:                        # fallback: last 80 bars
            session = [{**b, "_et": (b["bar_time"].astimezone(ET)
                        if b["bar_time"].tzinfo else ET.localize(b["bar_time"]))}
                       for b in list(bars)[-80:]]

        n  = len(session)
        xs = list(range(n))

        # ── Find the triggering bar ────────────────────────────────────────────
        sig_ts  = sig_et.timestamp()
        sig_idx = n - 1
        best    = float("inf")
        for i, b in enumerate(session):
            d = abs(b["_et"].timestamp() - sig_ts)
            if d < best:
                best    = d
                sig_idx = i

        # ── Figure layout ──────────────────────────────────────────────────────
        show_ind = sig_type in _IND_PANEL_TYPES
        if show_ind:
            fig, (ax, ax2) = plt.subplots(
                2, 1, figsize=(14, 8.5), facecolor=_BG,
                gridspec_kw={"height_ratios": [3, 1], "hspace": 0.06},
            )
        else:
            fig, ax = plt.subplots(1, 1, figsize=(14, 6.5), facecolor=_BG)
            ax2 = None

        _style(ax)

        # ── Candlestick bars ───────────────────────────────────────────────────
        W = 0.65
        for i, b in enumerate(session):
            o, h, l, c = b["open"], b["high"], b["low"], b["close"]
            clr = _UP if c >= o else _DOWN
            body_h = max(abs(c - o), 0.01)
            ax.add_patch(Rectangle(
                (i - W / 2, min(o, c)), W, body_h,
                facecolor=clr, edgecolor=clr, linewidth=0.4, zorder=3,
            ))
            ax.plot([i, i], [l, h], color=clr, linewidth=0.7, zorder=2)

        # ── VWAP line ─────────────────────────────────────────────────────────
        v_pairs = [(i, b["vwap"]) for i, b in enumerate(session) if b.get("vwap")]
        if v_pairs:
            ax.plot(*zip(*v_pairs), color=_VWAP, linewidth=1.5,
                    label="VWAP", zorder=5, alpha=0.95)

        # ── W.VWAP line (dashed, display only) ───────────────────────────────
        w_pairs = [(i, b["wvwap"]) for i, b in enumerate(session) if b.get("wvwap")]
        if w_pairs:
            ax.plot(*zip(*w_pairs), color=_WVWAP, linewidth=1.0,
                    linestyle="--", label="W.VWAP", zorder=5, alpha=0.70)

        # ── ORB levels ────────────────────────────────────────────────────────
        orb_h = extra.get("orb_high")
        orb_l = extra.get("orb_low")
        if orb_h and orb_l:
            ax.axhline(orb_h, color="#fbbf24", linewidth=1.0, linestyle="-.",
                       alpha=0.85, label=f"ORB H ${orb_h:.2f}", zorder=4)
            ax.axhline(orb_l, color="#fb923c", linewidth=1.0, linestyle="-.",
                       alpha=0.85, label=f"ORB L ${orb_l:.2f}", zorder=4)
            if "RETEST" in sig_type:
                for lev in (orb_h, orb_l):
                    ax.axhspan(lev * 0.997, lev * 1.003,
                               alpha=0.10, color="#fbbf24", zorder=1)

        # ── PDH / PDL ─────────────────────────────────────────────────────────
        if extra.get("pdh"):
            ax.axhline(extra["pdh"], color="#facc15", linewidth=1.0, linestyle=":",
                       alpha=0.85, label=f"PDH ${extra['pdh']:.2f}", zorder=4)
        if extra.get("pdl"):
            ax.axhline(extra["pdl"], color="#fb923c", linewidth=1.0, linestyle=":",
                       alpha=0.85, label=f"PDL ${extra['pdl']:.2f}", zorder=4)

        # ── Signal vertical + marker ───────────────────────────────────────────
        ax.axvline(sig_idx, color=sig_color, linewidth=2.0, alpha=0.92,
                   zorder=7, linestyle="-")
        going_up = any(k in sig_type for k in ("UP", "HIGH", "BULL", "BOUNCE"))
        ax.scatter([sig_idx], [signal.price], color=sig_color, s=170,
                   marker="^" if going_up else "v",
                   zorder=8, edgecolors="white", linewidths=0.6)
        ax.annotate(f"  ${signal.price:.2f}",
                    xy=(sig_idx, signal.price),
                    fontsize=8, color=sig_color, fontweight="bold",
                    va="bottom" if going_up else "top", zorder=9)

        # ── X-axis ticks ──────────────────────────────────────────────────────
        step     = max(1, n // 10)
        tick_xs  = list(range(0, n, step))
        tick_lbl = [session[i]["_et"].strftime("%H:%M") for i in tick_xs]
        ax.set_xlim(-0.5, n - 0.5)
        ax.set_xticks(tick_xs)
        ax.set_xticklabels(
            [""] * len(tick_xs) if show_ind else tick_lbl,
            color=_TEXT, fontsize=7,
        )

        # ── Y-axis (price) ────────────────────────────────────────────────────
        all_px = [b["low"] for b in session] + [b["high"] for b in session]
        for lev in (orb_h, orb_l, extra.get("pdh"), extra.get("pdl"),
                    signal.vwap, signal.wvwap):
            if lev:
                all_px.append(lev)
        rng = max(all_px) - min(all_px)
        pad = max(rng * 0.08, 0.10)
        ax.set_ylim(min(all_px) - pad, max(all_px) + pad)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"${x:.2f}"))

        # ── Legend ────────────────────────────────────────────────────────────
        ax.legend(loc="upper left", fontsize=7, framealpha=0.25,
                  facecolor=_BG, edgecolor=_GRID, labelcolor=_TEXT,
                  handlelength=1.4, borderpad=0.5)

        # ── Title ─────────────────────────────────────────────────────────────
        ax.set_title(
            f"{signal.ticker}  ·  {sig_type.replace('_', ' ')}  "
            f"·  ${signal.price:.2f}  ·  {sig_et.strftime('%H:%M ET')}",
            color="#e2e8f0", fontsize=12, fontweight="bold", pad=7, loc="left",
        )

        # ── Indicator panel ────────────────────────────────────────────────────
        if ax2 is not None:
            _style(ax2)
            ax2.set_xlim(-0.5, n - 0.5)
            ax2.set_xticks(tick_xs)
            ax2.set_xticklabels(tick_lbl, color=_TEXT, fontsize=7)
            ax2.axvline(sig_idx, color=sig_color, linewidth=2.0, alpha=0.9, zorder=6)

            if sig_type in ("VWAP_CROSS_UP", "VWAP_CROSS_DOWN",
                            "VWAP_RETEST", "VWAP_BOUNCE"):
                # Distance from daily VWAP (%)
                dxs, dys = [], []
                for i, b in enumerate(session):
                    v = b.get("vwap")
                    if v and v > 0:
                        dxs.append(i)
                        dys.append((b["close"] - v) / v * 100)
                if dxs:
                    ax2.plot(dxs, dys, color="#a78bfa", linewidth=1.3, zorder=3)
                    pos = [y if y > 0 else 0 for y in dys]
                    neg = [y if y <= 0 else 0 for y in dys]
                    ax2.fill_between(dxs, pos, 0, alpha=0.18, color=_UP)
                    ax2.fill_between(dxs, neg, 0, alpha=0.18, color=_DOWN)
                # VWAP = 0 line
                ax2.axhline(0,    color=_VWAP,    linewidth=1.0, alpha=0.8)
                ax2.axhline( 0.5, color="#475569", linewidth=0.5, linestyle=":",
                             label="+0.5%")
                ax2.axhline(-0.5, color="#475569", linewidth=0.5, linestyle=":",
                             label="-0.5%")
                ax2.axhline( 0.20, color="#374151", linewidth=0.4, linestyle="--")
                ax2.axhline(-0.20, color="#374151", linewidth=0.4, linestyle="--")
                ax2.set_ylabel("vs VWAP %", color=_TEXT, fontsize=7)
                ax2.legend(fontsize=6, framealpha=0.2, facecolor=_BG,
                           edgecolor=_GRID, labelcolor=_TEXT, loc="upper left")

            elif sig_type in ("WVWAP_CROSS_UP", "WVWAP_CROSS_DOWN"):
                # Distance from weekly VWAP (%)
                dxs, dys = [], []
                for i, b in enumerate(session):
                    w = b.get("wvwap")
                    if w and w > 0:
                        dxs.append(i)
                        dys.append((b["close"] - w) / w * 100)
                if dxs:
                    ax2.plot(dxs, dys, color=_WVWAP, linewidth=1.3, zorder=3)
                    ax2.fill_between(dxs, dys, 0, alpha=0.15, color=_WVWAP)
                ax2.axhline(0, color=_WVWAP, linewidth=1.0, alpha=0.8)
                ax2.set_ylabel("vs W.VWAP %", color=_TEXT, fontsize=7)

            elif sig_type in ("MACD_CROSS_BULL", "MACD_CROSS_BEAR"):
                # Proxy MACD: EMA12 − EMA26 of session closes
                closes = [b["close"] for b in session]
                if len(closes) >= 26:
                    hist   = [a - b for a, b in
                              zip(_ema(closes, 12), _ema(closes, 26))]
                    colors = [_UP if h >= 0 else _DOWN for h in hist]
                    ax2.bar(xs, hist, color=colors, width=0.65, alpha=0.8, zorder=3)
                    ax2.axhline(0, color="#475569", linewidth=0.7)
                    # Annotate with actual weekly values
                    macd_v = extra.get("macd", 0)
                    sig_v  = extra.get("signal", 0)
                    ax2.annotate(
                        f"Weekly MACD: {macd_v:+.4f}  Signal line: {sig_v:.4f}",
                        xy=(0.01, 0.88), xycoords="axes fraction",
                        color=sig_color, fontsize=7, fontweight="bold",
                    )
                else:
                    ax2.text(0.5, 0.5, "Insufficient bars for indicator panel",
                             ha="center", va="center",
                             transform=ax2.transAxes, color=_TEXT, fontsize=8)
                ax2.set_ylabel("MACD (EMA12−26)", color=_TEXT, fontsize=6)

        # ── Watermark ─────────────────────────────────────────────────────────
        fig.text(0.99, 0.005, "StockNotify", ha="right", va="bottom",
                 color="#1a2234", fontsize=8, fontweight="bold")

        # ── Save to bytes ──────────────────────────────────────────────────────
        plt.tight_layout(pad=0.6)
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=130, facecolor=_BG, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    except Exception as exc:
        log.error(
            f"Chart generation failed [{signal.signal_type} {signal.ticker}]: {exc}",
            exc_info=True,
        )
        try:
            plt.close("all")
        except Exception:
            pass
        return None
