#!/bin/bash
echo "🚀 Setting up the Python environment for ADF JSON Processor..."

# Install core dependencies
pip install -r requirements.txt

# Install dev dependencies (optional)
if [ "$1" == "--dev" ]; then
    echo "🔹 Installing development tools..."
    pip install -e .[dev]
fi

# Install pre-commit hooks
pre-commit install

echo "✅ Environment setup completed successfully!"