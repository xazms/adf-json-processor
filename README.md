# adf-json-processor
A Python project for processing ADF JSON files and building hierarchical structures.

## Overview

This project is designed to process Azure Data Factory (ADF) JSON files, standardize their structure, and store them as Delta files in a specified Azure Data Lake Storage account. The project is modular, with separate components for configuration, authentication, file handling, and processing. The goal is to streamline the handling of ADF files in a robust and scalable way.

## Features

- **Configuration Management**: Easily manage and retrieve configurations using the `Config` class.
- **Authentication**: Support for both Personal Access Token (PAT) and OAuth2 authentication methods.
- **File Handling**: Efficiently handle ADF JSON files, including retrieval, filtering, and error logging.
- **Processing**: Standardize the structure of ADF pipelines and save the output to Azure Data Lake Storage.
- **Modularity**: The project is structured into multiple modules, making it easy to maintain and extend.

## Project Structure

```plaintext
my-github-project/
│
├── src/                        # Source code folder
│   ├── config/                 # Subfolder for configuration-related code
│   ├── auth/                   # Subfolder for authentication-related code
│   ├── file_handling/          # Subfolder for file handling code
│   ├── processing/             # Subfolder for processing code
│   └── utils/                  # Utility functions or common helpers
│
├── notebooks/                  # Jupyter or Databricks notebooks
│   └── main_notebook.ipynb     # Your main notebook with the code and documentation
│
├── tests/                      # Test folder for unit tests
│   └── test_file_handler.py    # Example: Unit tests for FileHandler
│
├── LICENSE                     # License file
├── README.md                   # Project description and usage guide
└── setup.py                    # Setup script for installing the package
