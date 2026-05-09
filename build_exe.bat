@echo off
REM StockNotify — Build standalone EXE using PyInstaller
REM Run after setup.bat completes.

echo Building StockNotify.exe...
echo.

REM Install PyInstaller if needed
if exist venv\Scripts\python.exe (
    set PY=venv\Scripts\python
) else (
    set PY=python
)

%PY% -m pip install pyinstaller -q

REM Clean previous builds
if exist dist\StockNotify rmdir /s /q dist\StockNotify
if exist build rmdir /s /q build

REM Build
%PY% -m PyInstaller StockNotify.spec --clean

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ============================================
    echo  Build successful!
    echo  EXE: dist\StockNotify.exe
    echo.
    echo  To distribute: copy dist\StockNotify.exe + .env.example
    echo  The user must create .env with their API keys.
    echo ============================================
) else (
    echo.
    echo BUILD FAILED — check the output above for errors.
)

pause
