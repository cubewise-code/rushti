# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for RushTI

Build command:
    pyinstaller rushti.spec

This creates a single-file executable with utils.py bundled.
"""

import re
from pathlib import Path

# Extract version from rushti.py
version = "0.0.0"
rushti_path = Path("rushti.py")
if rushti_path.exists():
    content = rushti_path.read_text()
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
    if match:
        version = match.group(1)

a = Analysis(
    ['rushti.py'],
    pathex=[],
    binaries=[],
    datas=[('utils.py', '.')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='rushti',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
