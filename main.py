"""
StockNotify — main entry point

Run:
    python main.py                  # start with live stream
    python main.py --no-stream      # dashboard only (no Databento)
    python main.py --port 8436      # custom port
"""

import sys
import os

# Add project root to path so `stocknotify` package is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from stocknotify.runner import main

if __name__ == "__main__":
    main()
