#!/bin/bash
echo "🚀 Deploying ADF JSON Processor..."

# Ensure Python & pip are installed
if ! command -v python3 &> /dev/null || ! command -v pip &> /dev/null
then
    echo "❌ Python3 or pip not found. Please install Python before running this script."
    exit 1
fi

# Navigate to the project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Ensure the environment is set up
if [ ! -d "venv" ]; then
    echo "🔹 Virtual environment not found. Running setup_env.sh..."
    chmod +x scripts/setup_env.sh
    scripts/setup_env.sh
fi

# Activate the virtual environment
source venv/bin/activate

# Ensure dependencies are installed
if ! pip freeze | grep -q "databricks-cli"; then
    echo "🔹 Installing required dependencies..."
    pip install -r requirements.txt
fi

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null
then
    echo "❌ Databricks CLI not found. Installing..."
    pip install databricks-cli
fi

# Authenticate with Databricks if required
if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ]; then
    echo "❌ Databricks authentication is missing."
    echo "🔹 Please set the following environment variables:"
    echo "   export DATABRICKS_HOST='your-databricks-instance-url'"
    echo "   export DATABRICKS_TOKEN='your-databricks-token'"
    exit 1
fi

# Deploy notebooks to Databricks workspace
echo "🔹 Deploying notebooks to Databricks..."
databricks workspace import_dir notebooks /Workspace/adf-json-processor --overwrite

echo "✅ Deployment completed successfully!"