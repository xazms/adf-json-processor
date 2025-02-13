# 🏗 Architecture Overview

## 1️⃣ Introduction

The **ADF JSON Processor** is a Python-based framework designed to process **Azure Data Factory (ADF) JSON files**. It converts them into structured **PySpark DataFrames** and stores them as **Delta tables** in **Azure Data Lake Storage**.

### 🔹 Key Features:
- **Standardizes ADF JSON files** for easier processing.
- **Modular structure** for flexibility and maintainability.
- **Supports Delta Lake storage** for efficient data querying.
- **Handles dependencies** between ADF activities and pipelines.

---

## 2️⃣ System Architecture

The system consists of **six core components**, each handling a specific aspect of the processing workflow.

### 🔹 **Core Components**
| Component         | Description                                      |
|------------------|--------------------------------------------------|
| **1️⃣ Authentication**  | Authenticates with Azure DevOps via PAT.  |
| **2️⃣ Configuration**   | Manages settings and environment variables. |
| **3️⃣ File Handling**   | Retrieves and reads ADF JSON files. |
| **4️⃣ Processing**      | Parses JSON, extracts pipelines, activities, and dependencies. |
| **5️⃣ Storage**         | Creates or merges Delta tables in ADLS. |
| **6️⃣ Utilities**       | Provides logging, debugging, and helper functions. |

---

## 3️⃣ Workflow Process

The system follows a **6-step process** to handle ADF JSON data efficiently.

### 🔹 **Processing Steps**
1. **Fetch JSON files** from Azure DevOps.
2. **Parse JSON** and extract relevant pipeline data.
3. **Convert JSON to PySpark DataFrames**.
4. **Validate and clean data**.
5. **Create or Merge Delta tables** in Azure Data Lake.
6. **Log results and errors** for monitoring.

---

## 4️⃣ Components in Detail

### 🔹 **1. Authentication (`auth/`)**
- Supports **Personal Access Token (PAT)** authentication for **Azure DevOps**.
- Can be extended for **OAuth2 authentication**.

### 🔹 **2. Configuration (`config/`)**
- Loads settings from `config.yaml`.
- Manages **Databricks workspace details** and **storage credentials**.

### 🔹 **3. File Handling (`file_handling/`)**
- Retrieves ADF JSON files using **Azure DevOps API**.
- Filters and organizes ADF files for processing.

### 🔹 **4. Processing Module (`processing/`)**
- **Parses JSON files** into structured DataFrames.
- Extracts **pipelines, activities, dependencies**.
- Supports **incremental processing** for efficiency.

### 🔹 **5. Storage Management (`storage/`)**
- Creates **Delta tables** if they do not exist.
- Supports **MERGE operations** for **incremental updates**.
- Ensures **data consistency** by handling duplicates.

### 🔹 **6. Utilities (`utils/`)**
- **Logging**: Debugging, error handling, and execution logs.
- **Helper functions**: JSON parsing, hash generation, and formatting.
- **SQL query generation** for Delta table operations.

---

## 5️⃣ Data Flow Diagram

```plaintext
Step 1️⃣: Fetch JSON Files (Azure DevOps)
    ⬇
Step 2️⃣: Parse JSON into Structured Format
    ⬇
Step 3️⃣: Convert to PySpark DataFrames
    ⬇
Step 4️⃣: Validate & Clean Data
    ⬇
Step 5️⃣: Create or Merge Delta Tables (Azure Data Lake)
    ⬇
Step 6️⃣: Log Results & Monitor Errors