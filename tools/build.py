"""
DB Studio - Build Script
========================
Script untuk membuat executable dari DB Studio.
Menggunakan PyInstaller.

Cara penggunaan:
1. Install dependencies: pip install -r requirements.txt
2. Jalankan: python tools/build.py
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

# Pastikan working directory adalah root project
ROOT_DIR = Path(__file__).parent.parent
os.chdir(ROOT_DIR)


def check_dependencies():
    """Check if required packages are installed"""
    required = ['PyInstaller', 'psycopg2', 'pandas', 'openpyxl']
    missing = []

    for pkg in required:
        try:
            __import__(pkg.lower().replace('-', '_'))
        except ImportError:
            missing.append(pkg)

    if missing:
        print(f"Missing packages: {', '.join(missing)}")
        print("Installing missing packages...")
        for pkg in missing:
            subprocess.run([sys.executable, '-m', 'pip', 'install', pkg], check=True)
        print("Dependencies installed!")

    return True


def create_spec_file():
    """Create PyInstaller spec file"""
    spec_content = '''# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['run.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('databases.py', '.'),
        ('mappings.py', '.'),
        ('src', 'src'),
    ],
    hiddenimports=[
        'psycopg2',
        'pandas',
        'openpyxl',
        'tkinter',
        'tkinter.ttk',
        'tkinter.messagebox',
        'tkinter.filedialog',
        'tkinter.scrolledtext',
        'src.app',
        'src.config',
        'src.database',
        'src.parser',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib',
        'numpy.testing',
        'scipy',
        'PIL',
        'cv2',
    ],
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
    name='DBStudio',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='icon.ico' if os.path.exists('icon.ico') else None,
)
'''

    with open('db_studio.spec', 'w') as f:
        f.write(spec_content)

    print("Created: db_studio.spec")


def build_executable():
    """Build the executable using PyInstaller"""
    print("\n" + "="*50)
    print("Building DB Studio Executable")
    print("="*50 + "\n")

    # Clean previous builds
    for folder in ['build', 'dist']:
        if os.path.exists(folder):
            print(f"Cleaning {folder}/...")
            shutil.rmtree(folder)

    # Run PyInstaller
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--onefile',
        '--windowed',
        '--name', 'DBStudio',
        '--clean',
        '--noconfirm',
        '--add-data', 'databases.py;.',
        '--add-data', 'mappings.py;.',
        '--add-data', 'src;src',
    ]

    # Add icon if exists
    if os.path.exists('icon.ico'):
        cmd.extend(['--icon', 'icon.ico'])

    # Hidden imports
    hidden_imports = [
        'psycopg2',
        'pandas',
        'openpyxl',
        'openpyxl.cell._writer',
        'src.app',
        'src.config',
        'src.database',
        'src.parser',
    ]
    for imp in hidden_imports:
        cmd.extend(['--hidden-import', imp])

    cmd.append('run.py')

    print("Running PyInstaller...")
    print(f"Command: {' '.join(cmd)}\n")

    result = subprocess.run(cmd)

    if result.returncode == 0:
        print("\n" + "="*50)
        print("BUILD SUCCESSFUL!")
        print("="*50)

        exe_path = Path('dist/DBStudio.exe')
        if exe_path.exists():
            size_mb = exe_path.stat().st_size / (1024 * 1024)
            print(f"\nExecutable: {exe_path.absolute()}")
            print(f"Size: {size_mb:.1f} MB")
        else:
            exe_path = Path('dist/DBStudio')
            if exe_path.exists():
                size_mb = exe_path.stat().st_size / (1024 * 1024)
                print(f"\nExecutable: {exe_path.absolute()}")
                print(f"Size: {size_mb:.1f} MB")

        print("\nYou can now distribute the executable from the 'dist' folder.")
    else:
        print("\n" + "="*50)
        print("BUILD FAILED!")
        print("="*50)
        print("Check the output above for errors.")

    return result.returncode


def main():
    print("="*50)
    print("DB Studio - Build Script")
    print("="*50)
    print()

    # Check we're in the right directory
    if not os.path.exists('run.py'):
        print("Error: run.py not found!")
        print("Please run this script from the project root directory")
        return 1

    # Menu
    print("Options:")
    print("  1. Build executable (one-file)")
    print("  2. Create spec file only")
    print("  3. Full build (all steps)")
    print("  0. Exit")
    print()

    choice = input("Select option [3]: ").strip() or '3'

    if choice == '0':
        return 0

    if choice in ['1', '3']:
        check_dependencies()

    if choice == '2':
        create_spec_file()
    elif choice == '1':
        return build_executable()
    elif choice == '3':
        create_spec_file()
        return build_executable()
    else:
        print("Invalid option")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
