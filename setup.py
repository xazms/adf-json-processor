from setuptools import setup, find_packages

setup(
    name="adf-json-processor",
    version="0.1.0",
    author="Azmir Salihovic",
    author_email="azmir.salihovic@twoday.com",
    description="A project for processing ADF JSON files and storing them as Delta files in Azure Data Lake Storage",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/xazms/adf-json-processor",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "requests",
        "pyspark",
        "setuptools",
    ],
    extras_require={
        "dev": ["pytest", "flake8"],
    },
)
