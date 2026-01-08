#!/usr/bin/env python3
"""
DB Studio
=========
Aplikasi GUI untuk mengeksplorasi dan mengekstrak data dari PostgreSQL.

Jalankan dengan: python run.py
"""

import sys
import os

# Tambahkan root directory ke path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.app import main

if __name__ == "__main__":
    main()
