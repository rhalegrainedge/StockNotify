@echo off
REM StockNotify — Dashboard only (no live stream — for testing/offline use)

echo Starting StockNotify (dashboard-only mode — no Databento stream)...
echo Dashboard will be available at: http://localhost:8436
echo.

if exist venv\Scripts\python.exe (
    venv\Scripts\python main.py --no-stream %*
) else (
    python main.py --no-stream %*
)
