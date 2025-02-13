from setuptools import setup, find_packages

# Read the README file for the long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="adf-json-processor",
    version="0.1.2",  # Updated version for improved structure
    author="Azmir Salihovic",
    author_email="azmir.salihovic@twoday.com",
    description="A Python package for processing Azure Data Factory (ADF) JSON files and storing them as Delta tables in Azure Data Lake Storage",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/xazms/adf-json-processor",
    packages=find_packages(where="src"),  # Ensures correct package discovery
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
    ],
    python_requires=">=3.7",  # Minimum version requirement updated
    install_requires=[
        "requests>=2.26",  # Ensures compatibility with API requests
        "pyspark>=3.2",  # Required for Spark DataFrame processing
        "sqlparse",  # Required for SQL formatting
        "pygments",  # Required for syntax highlighting in logs
    ],
    extras_require={
        "dev": ["pytest", "flake8", "black", "pre-commit"],  # Added tools for development & formatting
        "test": ["pytest", "pytest-cov"],  # Ensures test coverage
    },
    entry_points={
        "console_scripts": [
            "adf-json-processor=adf_json_processor.processing.file_processor:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,  # Ensures package data is properly included
)