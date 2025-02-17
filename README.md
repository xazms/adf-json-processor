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
adf-json-processor/
│
├── src/                                # Source code folder
│   └── adf_json_processor/             # Main package
│       ├── auth/                       # Subfolder for authentication-related code
│       │   ├── __init__.py
│       │   └── auth_manager.py
│       ├── config/                     # Subfolder for configuration-related code
│       │   ├── __init__.py
│       │   └── config_manager.py
│       ├── file_handling/              # Subfolder for file handling code
│       │   ├── __init__.py
│       │   └── file_handler.py
│       ├── processing/                 # Subfolder for processing code
│       │   ├── __init__.py
│       │   └── file_processor.py
│       ├── storage/                    # Subfolder for processing code
│       │   ├── __init__.py
│       │   └── connector.py
│       │   └── reader.py
│       │   └── table_manager.py
│       │   └── writer.py
│       └── utils/                      # Utility functions or common helpers
│           ├── __init__.py
│           ├── helper.py
│           ├── logger.py
│           └── widget_manager.py       # Script for installing the package itself
│
├── tests/                              # Test folder for unit tests
│   ├── __init__.py
│   └── test_file_handler.py            # Unit tests for FileHandler
│
├── .gitignore                          # Git ignore file
├── LICENSE                             # License file
├── README.md                           # Project description and usage guide
├── requirements.txtmd                  # Installation requirements
└── setup.py                            # Setup script for installing the package