# 📌 **ADF JSON Processor**

The **ADF JSON Processor** is a **Python-based framework** for **processing Azure Data Factory (ADF) JSON files**, transforming them into structured **DataFrames**, and storing them as **Delta tables** in **Azure Data Lake Storage**.

This framework is designed for **data engineers and developers** working with **Azure Synapse, Databricks, and ADF**, enabling **automated JSON processing and Delta Lake integration**.

---

## 🚀 **Key Features**
| Feature            | Description |
|-------------------|-------------|
| **Automated Parsing** | Extracts and structures **ADF JSON files** efficiently. |
| **Data Transformation** | Converts raw JSON into a structured **Delta table format**. |
| **Table Management** | Supports **creating, merging, and validating Delta tables**. |
| **Optimized Performance** | Utilizes **PySpark** for large-scale processing. |
| **Modular Design** | Built with a **flexible architecture** for easy maintenance. |
| **Azure Integration** | Supports **Azure Data Lake, Synapse, and DevOps**. |

---

## 📂 **Project Structure**

```plaintext
adf-json-processor/
│── src/                                # Source code
│   ├── adf_json_processor/             # Main package
│   │   ├── auth/                       # Authentication strategies
│   │   ├── config/                     # Configuration management
│   │   ├── file_handling/              # Handles file retrieval and processing
│   │   ├── processing/                 # Processes JSON data into structured DataFrames
│   │   ├── storage/                    # Manages Delta table creation and merging
│   │   ├── utils/                      # Helper functions, logging, and utilities
│
│── tests/                              # Unit tests
│── scripts/                            # Deployment and setup scripts
│── docs/                               # Project documentation
│── setup.py                            # Installation script
│── requirements.txt                    # Dependencies
│── config.yaml                         # Configuration file
│── README.md                           # Project introduction and setup guide
│── LICENSE                             # License information
│── .gitignore                          # Git ignore file