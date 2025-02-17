# 🛠 **Setup & Installation Guide**

## 1️⃣ Introduction

This guide explains how to install, configure, and run **ADF JSON Processor**, a framework for processing **Azure Data Factory (ADF) JSON files** and storing them in **Delta tables** using **Databricks & Azure Data Lake**.

---

## 2️⃣ Prerequisites

Before installing, ensure you have the following dependencies:

### 🔹 **System Requirements**
| Requirement              | Version  |
|--------------------------|----------|
| **Python**               | 3.8+     |
| **Databricks Runtime**   | 10.4+ (or later) |
| **Azure DevOps Access**  | PAT Token for authentication |
| **Azure Data Lake Storage** | Required for Delta tables |

### 🔹 **Required Python Packages**
- `pyspark`
- `requests`
- `setuptools`
- `pytest` (for testing)
- `flake8` (for linting)

### 🔹 **Required Accounts**
✅ **Azure Services Required:**
- **Azure Data Lake** for data storage
- **Azure DevOps** repository (PAT token required)
- **Databricks Workspace** (with cluster access)

---

## 3️⃣ Installation

### 🔹 **1. Clone the Repository**
```bash
git clone https://github.com/xazms/adf-json-processor.git
cd adf-json-processor