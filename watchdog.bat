@echo off
REM StockNotify — Watchdog (24/7 auto-restart monitor)
REM Run in a separate window alongside start.bat

echo Starting StockNotify Watchdog...
echo Monitoring http://localhost:8436/health every 60s
echo Press Ctrl+C to stop.
echo.

if exist venv\Scripts\python.exe (
    venv\Scripts\python -m stocknotify.watchdog %*
) else (
    python -m stocknotify.watchdog %*
)
