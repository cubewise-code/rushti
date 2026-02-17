# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for RushTI

Build command:
    pyinstaller rushti.spec --clean

This creates a one-directory distribution with ALL features:
- TM1 integration and asset management
- Database admin utilities
- DAG visualization (interactive HTML with vis.js, no external dependencies)
- All optional components

Using --onedir mode (COLLECT) instead of --onefile for faster cold start times.
The single-file mode extracts to a temp directory on each run, causing ~20s delays
on Windows servers. The one-directory mode eliminates this extraction overhead.

TM1 object definitions (dimensions, cube, process, sample data) are embedded
as Python constants in tm1_objects.py — no external asset files are needed.

After building, copy the following to the distribution directory:
- config/config.ini.template -> config.ini (edit with your TM1 settings)
- config/logging_config.ini -> logging_config.ini (or config/logging_config.ini)
- config/settings.ini.template -> settings.ini (optional, for custom defaults)

The executable looks for config files in order:
1. CLI argument (e.g., --settings for settings.ini)
2. RUSHTI_DIR environment variable ({RUSHTI_DIR}/config/{filename})
3. ./{filename} (current directory - legacy)
4. ./config/{filename} (config subdirectory - recommended)

RUSHTI_DIR also controls the root for all app data (logs, stats db, checkpoints, visualizations).
"""

import re
from pathlib import Path
from PyInstaller.utils.hooks import collect_submodules

# Extract version from src/rushti/__init__.py
version = "0.0.0"
init_path = Path("src/rushti/__init__.py")
if init_path.exists():
    content = init_path.read_text()
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
    if match:
        version = match.group(1)

print(f"Building RushTI version {version}")

# Package directory - use absolute path
src_dir = Path("src").resolve()

# Collect data files
# Templates directory contains HTML templates for DAG visualization
import os
templates_dir = str(src_dir / "rushti" / "templates")
datas = [
    (templates_dir, os.path.join("rushti", "templates")),
]

# Collect all rushti submodules
rushti_imports = collect_submodules('rushti')
print(f"Collected rushti submodules: {rushti_imports}")

a = Analysis(
    ['__main__.py'],
    pathex=[str(src_dir)],  # Include src/ directory so PyInstaller can find the rushti package
    binaries=[],
    datas=datas,
    hiddenimports=rushti_imports + [
        # TM1py submodules that PyInstaller might miss
        'TM1py.Objects',
        'TM1py.Objects.Element',
        'TM1py.Objects.Cube',
        'TM1py.Objects.Dimension',
        'TM1py.Objects.Process',
        'TM1py.Objects.Subset',
        'TM1py.Objects.NativeView',
        'TM1py.Objects.MDXView',
        'TM1py.Exceptions',
        # Third-party dependencies
        'pandas',
        'chardet',
        'keyring',
        # Windows timezone support (required for TM1 security mode 3 with delegated auth)
        'win32timezone',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Exclude dev dependencies
        'pytest',
        'pytest-asyncio',
        'pytest-cov',
        'black',
        'isort',
        'flake8',
        'mypy',
        # Graphviz no longer used (HTML visualization only)
        'graphviz',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

# Using --onedir mode for faster cold start times
# The single-file mode extracts to a temp directory on each run, causing ~20s delays
# on Windows servers. The one-directory mode eliminates this extraction overhead.
exe = EXE(
    pyz,
    a.scripts,
    [],  # Don't include binaries/datas in EXE for onedir mode
    exclude_binaries=True,  # Required for onedir mode
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

# Collect all files into a directory
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='rushti',
)
