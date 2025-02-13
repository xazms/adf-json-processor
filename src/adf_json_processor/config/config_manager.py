import os
import datetime
import ast
from adf_json_processor.utils.logger import Logger

class Config:
    """
    Configuration class for managing and initializing settings, paths, and parameters.
    Uses an Authenticator instance for authentication.
    """

    def __init__(self, dbutils=None, authenticator=None, debug=False, logger=None):
        """
        Initialize the Config object.

        Args:
            dbutils (object): Databricks utilities object.
            authenticator (Authenticator): Instance of the Authenticator class for managing authentication.
            debug (bool): Enable detailed logging if True.
            logger (Logger, optional): Custom logger instance. If not provided, a new Logger is created.
        """
        # Use provided logger or create a new one
        self.logger = logger if logger is not None else Logger(debug=debug)
        self.dbutils = dbutils or globals().get("dbutils")  # Use global dbutils if available
        self.authenticator = authenticator
        self.debug = debug

        if not self.dbutils:
            self.logger.log_error("dbutils is required to retrieve secrets or widgets.")
            raise ValueError("dbutils is required to retrieve secrets or widgets.")

        if not self.authenticator:
            self.logger.log_error("Authenticator instance is required.")
            raise ValueError("Authenticator instance is required.")

        self.logger.log_info("Initializing Config...")

        try:
            self._initialize_config()
            self._validate_config()
            self.logger.log_info("Config initialized successfully.")
        except Exception as e:
            self.logger.log_error(f"Failed to initialize Config: {e}")
            raise

    def _get_widget_value(self, widget_name):
        """
        Retrieve a widget value safely.

        Args:
            widget_name (str): The widget name.

        Returns:
            str: The retrieved widget value.
        """
        try:
            value = self.dbutils.widgets.get(widget_name)
            if self.debug:
                self.logger.log_debug(f"Retrieved widget '{widget_name}': {value}")
            return value
        except Exception as e:
            self.logger.log_error(f"Error retrieving widget '{widget_name}': {e}")
            raise

    def _initialize_config(self):
        """
        Internal method to initialize configuration settings and paths.
        """
        self.logger.log_debug("Parsing ADF configuration...")

        # Parse ADF configuration from widgets
        adf_config_str = self._get_widget_value("ADFConfig")
        adf_config = ast.literal_eval(adf_config_str)

        (
            self.organization,
            self.project,
            self.repository,
            self.branch,
            self.folder_path,
        ) = adf_config

        self.source_storage_account = self._get_widget_value("SourceStorageAccount")
        self.destination_storage_account = self._get_widget_value("DestinationStorageAccount")
        self.datasetidentifier = self._get_widget_value("Datasetidentifier")
        self.source_filename = self._get_widget_value("SourceFileName")

        # Store ADF details in a structured dictionary
        self.adf_details = {
            "Organization": self.organization,
            "Project": self.project,
            "Repository": self.repository,
            "Branch": self.branch,
            "Folder Path": self.folder_path,
        }

        # Generate paths
        self.log_path = self._get_directory_path("log", f"error_log_{self._get_timestamp()}.json")
        self.output_path = self._get_directory_path("output", "combined_hierarchical_pipeline_structure_filtered.json")

        # Ensure directories exist
        self._ensure_directories_exist()

    def _get_timestamp(self):
        """
        Generate a timestamp for file paths.

        Returns:
            str: Current timestamp as a string.
        """
        return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    def _get_directory_path(self, directory_type, filename):
        """
        Construct a full directory path.

        Args:
            directory_type (str): The directory type ('log' or 'output').
            filename (str): The filename.

        Returns:
            str: Full path for the file.
        """
        base_path = f"/dbfs/mnt/{self.source_storage_account}/{self.datasetidentifier}"
        return os.path.join(base_path, directory_type, filename)

    def _validate_config(self):
        """
        Validate configuration settings to ensure they are correctly initialized.
        """
        required_fields = {
            "organization": self.organization,
            "project": self.project,
            "repository": self.repository,
            "branch": self.branch,
            "folder_path": self.folder_path,
            "source_storage_account": self.source_storage_account,
            "destination_storage_account": self.destination_storage_account,
            "datasetidentifier": self.datasetidentifier,
        }

        missing_fields = [key for key, value in required_fields.items() if not value]
        if missing_fields:
            self.logger.log_error(f"Missing required configuration fields: {', '.join(missing_fields)}")
            raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")

        self.logger.log_debug("Configuration validated successfully.")

    def _ensure_directories_exist(self):
        """
        Ensure that required directories exist in the file system.
        """
        try:
            for path in [self.log_path, self.output_path]:
                dir_path = os.path.dirname(path)
                # If running in Databricks, use dbutils.fs.mkdirs; otherwise, use os.makedirs.
                if "dbutils" in globals():
                    self.dbutils.fs.mkdirs(dir_path)
                else:
                    os.makedirs(dir_path, exist_ok=True)
            self.logger.log_info("Required directories validated successfully.")
        except Exception as e:
            self.logger.log_error(f"Failed to create directories: {e}")
            raise

    def print_configuration(self):
        """
        Print all configuration parameters in a structured format.
        """
        self.logger.log_block("ADF Configuration", [f"{key}: {value}" for key, value in self.adf_details.items()], level="info")
        self.logger.log_block("Storage Configuration", [
            f"Source Storage Account: {self.source_storage_account}",
            f"Destination Storage Account: {self.destination_storage_account}",
            f"Dataset Identifier: {self.datasetidentifier}",
            f"Source Filename: {self.source_filename}",
        ], level="info")

    @staticmethod
    def initialize(dbutils=None, authenticator=None, debug=False, logger=None):
        """
        Factory method to initialize the Config class.

        Args:
            dbutils (object): Databricks utilities object.
            authenticator (Authenticator): Instance of the Authenticator class.
            debug (bool): Enable detailed logging if True.
            logger (Logger, optional): Custom logger instance.

        Returns:
            Config: An initialized Config instance.
        """
        temp_logger = logger if logger is not None else Logger(debug=debug)

        if not dbutils:
            dbutils = globals().get("dbutils")

        if not dbutils:
            temp_logger.log_error("dbutils is not available. Ensure it is properly initialized.")
            raise ValueError("dbutils is required.")

        try:
            return Config(dbutils=dbutils, authenticator=authenticator, debug=debug, logger=logger)
        except Exception as e:
            temp_logger.log_error(f"Failed to initialize Config: {e}")
            raise

    def test_config(self):
        """
        Simple built-in test for initializing Config and verifying functionality.
        """
        try:
            print("🔹 Running Config Test...")
            # Check if required fields are set
            assert self.organization, "❌ Organization not set!"
            assert self.source_storage_account, "❌ Source Storage Account not set!"
            assert self.datasetidentifier, "❌ Dataset Identifier not set!"
            print("✅ Config Test Passed Successfully!")
        except Exception as e:
            print(f"❌ Config Test Failed: {e}")