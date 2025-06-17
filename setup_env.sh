#!/bin/bash

VENV_DIR="migrator_venv"

# Check if virtual environment directory exists
if [ -d "$VENV_DIR" ]; then
    echo "Virtual environment already exists. Activating..."
    source "$VENV_DIR/bin/activate"
else
    echo "Creating new virtual environment..."
    python3 -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"
    pip install --upgrade pip
    pip install wheel
    pip install setuptools
    echo "Installing required libraries..."
    pip install -r requirements.txt
fi

echo "Environment setup complete."
