@echo off
REM StockNotify — Windows setup script
REM Run this once on a new machine to install dependencies.

echo ============================================
echo  StockNotify Setup
echo ============================================
echo.

REM Check Python
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python not found. Install Python 3.9+ from https://python.org
    pause
    exit /b 1
)

echo [1/3] Creating virtual environment...
python -m venv venv
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to create venv
    pause
    exit /b 1
)

echo [2/3] Installing dependencies...
call venv\Scripts\pip install -r requirements.txt
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: pip install failed
    pause
    exit /b 1
)

echo [3/3] Setting up config...
if not exist .env (
    copy .env.example .env
    echo.
    echo IMPORTANT: Edit .env and add your API keys before running!
    echo   - SN_DB_KEY: Your Databento API key
    echo   - SN_TG_TOKEN: Your Telegram bot token
    echo   - SN_TG_CHATS: Your Telegram chat IDs
    echo.
) else (
    echo .env already exists — skipping
)

echo.
echo ============================================
echo  Setup complete!
echo  Next: edit .env then run start.bat
echo ============================================
pause
