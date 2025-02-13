#!/bin/bash

# ==============================
# 🚀 ADF JSON Processor Execution Script
# ==============================

# ✅ Step 1: Check for Python Installation
echo "🔹 Checking Python installation..."
if ! command -v python &> /dev/null; then
    echo "❌ Python not found! Please install Python and try again."
    exit 1
fi
echo "✅ Python is installed."

# ✅ Step 2: Set Environment Variables
read -p "Enter Azure Storage Account Name (default: your-storage-account): " USER_STORAGE
export STORAGE_ACCOUNT=${USER_STORAGE:-"your-storage-account"}

read -p "Enter Azure DevOps PAT Token: " -s USER_PAT
export DEVOPS_PAT=${USER_PAT}
echo ""

if [[ -z "$DEVOPS_PAT" ]]; then
    echo "❌ DevOps PAT Token is required!"
    exit 1
fi

echo "✅ Environment variables set."

# ✅ Step 3: Run Processor
echo "🚀 Running ADF JSON Processor..."
if python -m adf_json_processor.processing.file_processor; then
    echo "✅ Processing completed successfully."
else
    echo "❌ Processing failed. Please check logs and try again."
    exit 1
fi

echo "🎉 ADF JSON Processor execution completed!"