"""
StockNotify — 24/7 Watchdog (standalone edition)

Responsibilities:
  1. Health-check http://127.0.0.1:8436/health every 60 s
  2. If 2+ consecutive failures → restart via subprocess + Telegram alert
  3. Alert when service recovers
  4. Daily at 16:35 ET → trigger incremental history pull for all tickers
  5. Weekly on Sunday at 02:00 ET → pull full history for all tickers
  6. Log everything to ./logs/watchdog.log

Usage:
    python -m stocknotify.watchdog           # run forever
    python -m stocknotify.watchdog --once    # single health check then exit
"""

import argparse
import json
import logging
import os
import pathlib
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import pytz

# ── Paths ─────────────────────────────────────────────────────────────────────
THIS_DIR     = pathlib.Path(__file__).parent
PROJECT_ROOT = THIS_DIR.parent
LOG_DIR      = PROJECT_ROOT / "logs"
LOG_FILE     = LOG_DIR / "watchdog.log"

# ── Config ────────────────────────────────────────────────────────────────────
SN_PORT          = int(os.environ.get("SN_PORT", "8436"))
SN_HEALTH_URL    = f"http://127.0.0.1:{SN_PORT}/health"
SN_HIST_PULL_URL = f"http://127.0.0.1:{SN_PORT}/api/history/{{ticker}}/pull"

CHECK_INTERVAL_S  = 60
FAIL_THRESHOLD    = 2
DAILY_PULL_HOUR   = 16
DAILY_PULL_MINUTE = 35
WEEKLY_PULL_DOW   = 6   # Sunday (0=Mon…6=Sun)
WEEKLY_PULL_HOUR  = 2

ET = pytz.timezone("America/New_York")

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [watchdog] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
    ],
)
log = logging.getLogger("sn_watchdog")


# ── Telegram ──────────────────────────────────────────────────────────────────

def _load_config():
    """Import stocknotify.config."""
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        import importlib
        spec = importlib.util.spec_from_file_location(
            "stocknotify.config",
            str(THIS_DIR / "config.py"),
        )
        cfg = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cfg)
        return cfg
    except Exception as exc:
        log.error(f"Could not load config: {exc}")
        return None


def _send_telegram(text: str, cfg=None):
    if cfg is None:
        cfg = _load_config()
    if not cfg:
        return
    bot_token = getattr(cfg, "TELEGRAM_BOT_TOKEN", "") or ""
    chats     = getattr(cfg, "TELEGRAM_CHATS",     {}) or {}
    if not bot_token or bot_token.startswith("YOUR") or not chats:
        log.warning("Telegram not configured — alert not sent")
        return
    for chat_id, label in chats.items():
        url     = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = json.dumps({"chat_id": str(chat_id), "text": text, "parse_mode": "Markdown"}).encode()
        try:
            req = urllib.request.Request(url, data=payload,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
            log.info(f"Telegram alert sent to {label} ({chat_id})")
        except Exception as exc:
            log.error(f"Telegram send failed to {label} ({chat_id}): {exc}")


# ── Health check ──────────────────────────────────────────────────────────────

def _check_health() -> bool:
    try:
        with urllib.request.urlopen(SN_HEALTH_URL, timeout=8) as resp:
            return resp.status == 200
    except Exception:
        return False


# ── Restart via subprocess ────────────────────────────────────────────────────

def _restart_service() -> bool:
    """Restart StockNotify by launching main.py as a new subprocess."""
    main_py = str(PROJECT_ROOT / "main.py")
    if not os.path.exists(main_py):
        log.error(f"main.py not found at {main_py}")
        return False
    try:
        proc = subprocess.Popen(
            [sys.executable, main_py],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=str(PROJECT_ROOT),
        )
        log.info(f"StockNotify restarted — pid {proc.pid}")
        return True
    except Exception as exc:
        log.error(f"Subprocess restart failed: {exc}")
        return False


# ── History pull ──────────────────────────────────────────────────────────────

def _pull_history_all():
    """Trigger incremental history pull for all tickers via the SN dashboard API."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{SN_PORT}/api/coverage", timeout=10) as resp:
            data = json.loads(resp.read())
        tickers = list(data.keys())
    except Exception as exc:
        log.error(f"Could not fetch ticker list for history pull: {exc}")
        return []

    pulled = []
    for tk in tickers:
        try:
            url = SN_HIST_PULL_URL.format(ticker=tk)
            with urllib.request.urlopen(url, timeout=15) as resp:
                r = json.loads(resp.read())
            log.info(f"History pull started for {tk}: {r.get('status','?')}")
            pulled.append(tk)
        except Exception as exc:
            log.error(f"History pull failed for {tk}: {exc}")

    if pulled:
        log.info(f"Incremental history pull triggered for: {', '.join(pulled)}")
    return pulled


# ── Scheduler ─────────────────────────────────────────────────────────────────

class _Scheduler:
    def __init__(self):
        self._daily_pull_done_date  = None
        self._weekly_pull_done_week = None

    def tick(self, now_et: datetime, cfg=None):
        today    = now_et.date()
        iso_week = now_et.isocalendar()[:2]
        h, m, dow = now_et.hour, now_et.minute, now_et.weekday()

        if (h == DAILY_PULL_HOUR and m >= DAILY_PULL_MINUTE
                and self._daily_pull_done_date != today):
            log.info("=== Daily post-market history pull triggered ===")
            pulled = _pull_history_all()
            self._daily_pull_done_date = today
            if pulled:
                _send_telegram(
                    f"📥 *StockNotify — Daily History Pull*\n"
                    f"Incremental pull triggered for {len(pulled)} tickers after market close.\n"
                    f"Time: {now_et.strftime('%I:%M %p ET')}",
                    cfg=cfg,
                )

        if (dow == WEEKLY_PULL_DOW and h == WEEKLY_PULL_HOUR
                and self._weekly_pull_done_week != iso_week):
            log.info("=== Weekly full history pull triggered ===")
            pulled = _pull_history_all()
            self._weekly_pull_done_week = iso_week
            if pulled:
                _send_telegram(
                    f"🗓 *StockNotify — Weekly History Sync*\n"
                    f"Full incremental pull for {len(pulled)} tickers completed.\n"
                    f"Time: {now_et.strftime('%a %I:%M %p ET')}",
                    cfg=cfg,
                )


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_forever():
    log.info("=" * 60)
    log.info("StockNotify Watchdog started")
    log.info(f"  Health URL : {SN_HEALTH_URL}")
    log.info(f"  Log file   : {LOG_FILE}")
    log.info("=" * 60)

    cfg       = _load_config()
    scheduler = _Scheduler()

    fail_count   = 0
    service_up   = True
    last_restart = None
    check_count  = 0

    while True:
        now_et  = datetime.now(ET)
        healthy = _check_health()
        check_count += 1

        if healthy:
            if not service_up:
                msg = (
                    f"✅ *StockNotify — Service Recovered*\n"
                    f"Service is back up at {now_et.strftime('%I:%M %p ET')}\n"
                    f"Consecutive failures before recovery: {fail_count}"
                )
                log.info(msg)
                _send_telegram(msg, cfg=cfg)
                service_up = True
            fail_count = 0
            if check_count % 10 == 0:
                log.info(f"Heartbeat #{check_count} — service UP — {now_et.strftime('%H:%M:%S ET')}")

        else:
            fail_count += 1
            log.warning(f"Health check FAILED (consecutive: {fail_count})")

            if fail_count >= FAIL_THRESHOLD:
                now_utc = datetime.now(timezone.utc)
                if last_restart and (now_utc - last_restart).total_seconds() < 300:
                    log.info("Restart cooldown active — skipping restart this cycle")
                else:
                    service_up = False
                    alert_msg  = (
                        f"🚨 *StockNotify — Service DOWN*\n"
                        f"Health check failed {fail_count}× at {now_et.strftime('%I:%M %p ET')}\n"
                        f"Attempting restart…"
                    )
                    log.error(alert_msg.replace("*", ""))
                    _send_telegram(alert_msg, cfg=cfg)

                    ok = _restart_service()
                    last_restart = datetime.now(timezone.utc)

                    if ok:
                        log.info("Restart issued — waiting 20s for service to come up")
                        time.sleep(20)
                        if _check_health():
                            recover_msg = (
                                f"✅ *StockNotify — Restart Successful*\n"
                                f"Service came back up after restart at {now_et.strftime('%I:%M %p ET')}"
                            )
                            log.info("Service recovered after restart")
                            _send_telegram(recover_msg, cfg=cfg)
                            fail_count = 0
                            service_up = True
                        else:
                            log.error("Service still down after restart")
                    else:
                        log.error("Restart command failed")

        scheduler.tick(now_et, cfg=cfg)
        time.sleep(CHECK_INTERVAL_S)


def run_once():
    healthy = _check_health()
    status  = "UP" if healthy else "DOWN"
    log.info(f"StockNotify health check: {status}")
    return 0 if healthy else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StockNotify 24/7 watchdog")
    parser.add_argument("--once", action="store_true", help="Single check then exit")
    args = parser.parse_args()

    if args.once:
        sys.exit(run_once())
    else:
        try:
            run_forever()
        except KeyboardInterrupt:
            log.info("Watchdog stopped by user")
