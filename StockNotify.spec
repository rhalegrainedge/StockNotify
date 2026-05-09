# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec file for StockNotify standalone EXE
# Build: pyinstaller StockNotify.spec

import os
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# Collect data files
datas = [
    ('stocknotify/tickers.json',        'stocknotify'),
    ('stocknotify/matrix_tickers.json', 'stocknotify'),
    ('.env.example',                     '.'),
]

# Collect all databento data files if present
try:
    datas += collect_data_files('databento')
except Exception:
    pass

# Hidden imports that PyInstaller might miss
hiddenimports = [
    'uvicorn.logging',
    'uvicorn.loops',
    'uvicorn.loops.auto',
    'uvicorn.protocols',
    'uvicorn.protocols.http',
    'uvicorn.protocols.http.auto',
    'uvicorn.protocols.websockets',
    'uvicorn.protocols.websockets.auto',
    'uvicorn.lifespan',
    'uvicorn.lifespan.on',
    'fastapi',
    'starlette',
    'anyio',
    'anyio._backends._asyncio',
    'pytz',
    'pandas',
    'pyarrow',
    'matplotlib',
    'matplotlib.backends.backend_agg',
    'databento',
    'requests',
]

a = Analysis(
    ['main.py'],
    pathex=['.'],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['PySide6', 'pyqtgraph', 'PyQt5', 'tkinter', 'scipy', 'sklearn'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='StockNotify',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,   # set to False for no console window (windowed mode)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,       # add path to .ico file here for custom icon
)
