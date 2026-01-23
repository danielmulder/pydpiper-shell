#!/bin/bash
set -e

echo "--- PydPiper Shell Setup (Linux/macOS) ---"
python3 --version

# Create venv if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate
echo "🔄 Activating virtual environment..."
source .venv/bin/activate

# Upgrade packaging stack
echo "🚀 Upgrading core packaging tools (pip, setuptools, wheel)..."
pip install --no-cache-dir --upgrade pip setuptools wheel

# Install package
echo "🛠️ Installing PydPiper Shell in editable mode..."
pip install -e . --no-warn-script-location

echo ""
echo "✅ Installation successful!"
echo ""
echo "To start the shell, run:"
echo "    source .venv/bin/activate"
echo "    python3 -m pydpiper_shell.app"
echo ""