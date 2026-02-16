"""PyInstaller entry point for RushTI.

This is a minimal entry point used by PyInstaller to build executables.
It avoids the path manipulation in rushti.py which causes conflicts.
"""
import sys
from rushti.cli import main

if __name__ == "__main__":
    sys.exit(main())
