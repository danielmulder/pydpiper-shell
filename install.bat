@echo off
SETLOCAL
ECHO --- PydPiper Shell Setup (Windows) ---
python --version

IF NOT EXIST ".venv" (
    ECHO 📦 Creating virtual environment...
    python -m venv .venv
)

CALL .venv\Scripts\activate.bat

ECHO 🚀 Upgrading core packaging tools (pip, setuptools, wheel)...
python -m pip install --no-cache-dir --upgrade pip setuptools wheel

ECHO 🛠️ Installing PydPiper Shell and dependencies in editable mode...
pip install -e . --no-warn-script-location

ECHO.
ECHO ✅ Installation successful!
ECHO.
ECHO To start the shell, run:
ECHO     .venv\Scripts\activate
ECHO     python -m pydpiper_shell.app
ECHO.
PAUSE
ENDLOCAL
EXIT /B 0