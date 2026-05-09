@echo off
REM StockNotify — Start the dashboard + live stream
REM Double-click this file to launch StockNotify.

echo Starting StockNotify...
echo Dashboard will be available at: http://localhost:8436
echo Press Ctrl+C to stop.
echo.

REM Use venv if it exists
if exist venv\Scripts\python.exe (
    venv\Scripts\python main.py %*
) else (
    python main.py %*
)
