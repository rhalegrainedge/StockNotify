"""
StockNotify — Telegram alert sender + in-memory alert log

On every signal fire:
  1. Generate a candlestick chart PNG (chartgen.generate_signal_chart)
  2. Send chart as Telegram photo with signal text as caption (sendPhoto)
  3. Log the alert to the in-memory circular buffer
"""

import os
import json
import logging
import threading
import urllib.request
from collections import deque
from datetime import datetime

import pytz

log = logging.getLogger("sn_alerts")
ET  = pytz.timezone("America/New_York")


class AlertLog:
    """Thread-safe in-memory circular log of recent alerts."""

    def __init__(self, maxlen: int = 500):
        self._log:  deque = deque(maxlen=maxlen)
        self._lock: threading.Lock = threading.Lock()

    def add(self, ticker: str, signal_type: str, message: str, price: float, tg_sent: bool = False):
        entry = {
            "ts":      datetime.now(ET).isoformat(),
            "ticker":  ticker,
            "type":    signal_type,
            "price":   price,
            "message": message,
            "tg_sent": tg_sent,
        }
        with self._lock:
            self._log.appendleft(entry)

    def recent(self, n: int = 50) -> list:
        with self._lock:
            return list(self._log)[:n]


class TelegramAlerter:
    """Sends chart photos + text captions to all configured Telegram chat IDs."""

    def __init__(self, config):
        self.config    = config
        self.alert_log = AlertLog(maxlen=config.MAX_ALERTS_STORED)

    def send(self, signal):
        """Send a Signal to all configured Telegram chats and log it."""
        text = signal.to_telegram()

        # Pop bars snapshot attached by AnalysisEngine (used for chart, not logged)
        bars = signal.extra.pop("_bars", [])

        bot_token = os.environ.get("SN_TG_TOKEN") or self.config.TELEGRAM_BOT_TOKEN
        tg_sent   = False

        if bot_token and not bot_token.startswith("YOUR") and self.config.TELEGRAM_CHATS:
            # Generate chart PNG once, reuse across all chat destinations
            png = self._make_chart(signal, bars)

            for chat_id in self.config.TELEGRAM_CHATS:
                if png:
                    ok = self._send_photo(bot_token, str(chat_id), png, caption=text)
                else:
                    ok = self._send_text(bot_token, str(chat_id), text)
                if ok:
                    tg_sent = True
        else:
            log.debug("Telegram not configured — alert logged only")

        self.alert_log.add(signal.ticker, signal.signal_type, text, signal.price, tg_sent=tg_sent)

    # ── Chart generation ──────────────────────────────────────────────────────

    def _make_chart(self, signal, bars: list):
        """Return PNG bytes or None if chart generation fails/unavailable."""
        if not bars:
            return None
        try:
            from stocknotify.chartgen import generate_signal_chart
            return generate_signal_chart(signal, bars)
        except Exception as exc:
            log.warning(f"Chart generation skipped: {exc}")
            return None

    # ── Telegram delivery ─────────────────────────────────────────────────────

    def _send_photo(self, bot_token: str, chat_id: str, png: bytes, caption: str) -> bool:
        """Send PNG via Telegram sendPhoto (multipart/form-data using requests)."""
        try:
            import requests as _req
            url  = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
            cap  = caption[:1020] + "…" if len(caption) > 1024 else caption
            resp = _req.post(
                url,
                data={"chat_id": chat_id, "caption": cap, "parse_mode": "Markdown"},
                files={"photo": ("signal_chart.png", png, "image/png")},
                timeout=30,
            )
            if resp.ok:
                log.info(f"Telegram photo sent to {chat_id} [{len(png)//1024}KB]")
                return True
            log.error(f"Telegram sendPhoto failed ({chat_id}): {resp.status_code} {resp.text[:120]}")
            return False
        except Exception as exc:
            log.error(f"Telegram sendPhoto error ({chat_id}): {exc}")
            return self._send_text(bot_token, chat_id, caption)

    def _send_text(self, bot_token: str, chat_id: str, text: str) -> bool:
        """Plain-text Telegram message fallback (no chart)."""
        url     = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = json.dumps({
            "chat_id":    chat_id,
            "text":       text,
            "parse_mode": "Markdown",
        }).encode()
        try:
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            log.info(f"Telegram text sent to {chat_id}: {text[:60]}")
            return True
        except Exception as exc:
            log.error(f"Telegram send failed ({chat_id}): {exc}")
            return False

    _send_one = _send_text
