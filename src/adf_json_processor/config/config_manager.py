import json
import os
import datetime
from adf_json_processor.utils.logger import Logger

class ConfigManager:
    """
    Manages configuration for Azure Data Factory (ADF) pipelines by setting up Databricks widgets
    and managing directory paths for logging and outputs. Provides utility functions to handle
    directory structure and configuration validation.
    """

    def __init__(self, dbutils, logger=None):
        """
        Initializes the ConfigManager with dbutils for widget access and an optional logger.
        
        Args:
            dbutils: Databricks utility object for accessing widgets.
            logger (Logger): Optional logger instance for structured logging.
        """
        if not dbutils:
            raise ValueError("dbutils is required to manage widgets in this environment.")
        
        self.dbutils = dbutils
        self.config = {}
        self.logger = logger or Logger()
        # Expose log and output paths as attributes
        self.log_path = self._generate_log_path()
        self.output_path = self._generate_output_path()

    def initialize_widgets(self):
        """
        Sets up Databricks widgets for ADF pipeline configuration and other storage-related parameters.
        """
        self.logger.log_start("initialize_widgets")
        self.dbutils.widgets.text("adfConfig", '["energinet", "DataPlatform_v3.0", "data-factory", "main", "pipeline"]', "ADF Configuration")
        self.dbutils.widgets.text("sourceStorageAccount", "dplandingstoragetest", "Source Storage Account")
        self.dbutils.widgets.text("destinationStorageAccount", "dpuniformstoragetest", "Destination Storage Account")
        self.dbutils.widgets.text("datasetIdentifier", "data_quality__adf", "Dataset Identifier")
        self.dbutils.widgets.text("sourceFileName", "*", "Source File Name")
        self.logger.log_end("initialize_widgets")

    def load_config(self):
        """
        Loads and validates widget values, stores them in the config attribute, logs the configuration settings,
        ensures directories exist, and logs optional configuration paths.
        """
        self.logger.log_start("load_config")
        try:
            # Load and validate configuration
            self._load_main_config()
            self.logger.log_block("Loaded Configuration", [f"{k}: {v}" for k, v in self.config.items()])

            # Log optional paths right after main configuration
            self._log_optional_paths()

            # Ensure directories exist
            self.ensure_directories_exist()
        except (ValueError, json.JSONDecodeError) as e:
            self.logger.log_error(f"Configuration loading error: {e}")
            raise
        except Exception as e:
            self.logger.log_error(f"Unexpected error during configuration loading: {e}")
            raise ValueError("Unexpected error during configuration loading.")
        self.logger.log_end("load_config")

    def _load_main_config(self):
        """
        Retrieves and parses main configuration values from widgets and stores them in `self.config`.
        """
        adf_config_str = self.dbutils.widgets.get("adfConfig")
        self.config["adfConfig"] = self._parse_and_validate_adf_config(adf_config_str)
        self.config["sourceStorageAccount"] = self.dbutils.widgets.get("sourceStorageAccount")
        self.config["destinationStorageAccount"] = self.dbutils.widgets.get("destinationStorageAccount")
        self.config["datasetIdentifier"] = self.dbutils.widgets.get("datasetIdentifier")
        self.config["sourceFileName"] = self.dbutils.widgets.get("sourceFileName")

    def _parse_and_validate_adf_config(self, adf_config_str):
        """
        Parses and validates the adfConfig JSON string.
        
        Args:
            adf_config_str (str): JSON string containing ADF configuration.
        
        Returns:
            list: Parsed configuration as a list if valid.
        
        Raises:
            ValueError: If the configuration is invalid.
        """
        adf_config = json.loads(adf_config_str)
        if len(adf_config) < 5:
            self.logger.log_error("ADF Configuration requires 5 elements.")
            raise ValueError("ADF Configuration requires 5 elements: organization, project, repository, branch, and folder path.")
        return adf_config

    def _generate_log_path(self):
        """
        Generates a log path based on the current timestamp.
        
        Returns:
            str: Path to the log file.
        """
        date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"/mnt/{self.config.get('sourceStorageAccount', 'dplandingstoragetest')}/{self.config.get('datasetIdentifier', 'data_quality__adf')}/log/error_log_{date_str}.json"

    def _generate_output_path(self):
        """
        Generates an output path for the processed file.
        
        Returns:
            str: Path to the output file.
        """
        return f"/dbfs/mnt/{self.config.get('sourceStorageAccount', 'dplandingstoragetest')}/{self.config.get('datasetIdentifier', 'data_quality__adf')}/combined_hierarchical_pipeline_structure_filtered.json"

    def ensure_directories_exist(self):
        """
        Ensures that required directories exist in the file system.
        Logs whether each directory already exists or was created.
        """
        self.logger.log_start("ensure_directories_exist")
        log_dir, output_dir = os.path.dirname(self.log_path), os.path.dirname(self.output_path)
        dir_statuses = self._check_and_create_directories(log_dir, output_dir)
        self.logger.log_block("Directory Statuses", dir_statuses)
        self.logger.log_end("ensure_directories_exist")

    def _check_and_create_directories(self, log_dir, output_dir):
        """
        Helper method to check and create directories as needed.
        
        Args:
            log_dir (str): Path for the log directory.
            output_dir (str): Path for the output directory.
        
        Returns:
            list: Status messages indicating whether directories existed or were created.
        """
        dir_statuses = []
        if 'dbutils' in globals():  # Databricks environment
            for dir_path, dir_type in [(log_dir, "Log"), (output_dir, "Output")]:
                try:
                    self.dbutils.fs.ls(dir_path)
                    dir_statuses.append(f"{dir_type} directory already exists in DBFS: {dir_path}")
                except Exception:
                    self.dbutils.fs.mkdirs(dir_path)
                    dir_statuses.append(f"{dir_type} directory created in DBFS: {dir_path}")
        else:  # Local environment
            for dir_path, dir_type in [(log_dir, "Log"), (output_dir, "Output")]:
                if os.path.exists(dir_path):
                    dir_statuses.append(f"{dir_type} directory already exists locally: {dir_path}")
                else:
                    os.makedirs(dir_path)
                    dir_statuses.append(f"{dir_type} directory created locally: {dir_path}")
        return dir_statuses

    def _log_optional_paths(self):
        """
        Logs additional configuration paths for optional outputs.
        """
        self.logger.log_block("Optional Configuration", [
            f"log_path: {self.log_path}",
            f"output_path: {self.output_path}"
        ])

    def get_output_path(self) -> str:
        """
        Returns the output path for saving processed data.
        
        Returns:
            str: Path to the output file.
        """
        return self.output_path

    def get_config(self):
        """
        Returns the configuration dictionary.
        
        Returns:
            dict: Configuration dictionary.
        """
        return self.config

    def unpack(self, scope):
        """
        Unpacks configuration variables into the given scope.

        Args:
            scope (dict): The scope to unpack configuration variables into, like `globals()`.
        """
        for key, value in self.config.items():
            scope[key] = value
        scope['log_path'] = self.log_path
        scope['output_path'] = self.output_path