from adf_json_processor.utils.logger import Logger
from adf_json_processor.config.config_manager import ConfigManager
from adf_json_processor.auth.auth_manager import authenticate, AuthStrategy
from adf_json_processor.file_handling.file_handler import FileHandler
from adf_json_processor.processing.file_processor import FileProcessor
from adf_json_processor.utils.helper import Helper
from pyspark.sql import SparkSession

class Initializer:
    """
    Initializer class for setting up all necessary components, including Spark session, 
    Logger, ConfigManager, Helper, AuthStrategy, FileHandler, and FileProcessor, 
    for standardized setup in the ADF processing pipeline.
    """

    def __init__(self, dbutils, debug: bool = True, auth_method: str = "PAT"):
        """
        Initialize the Initializer with dbutils, debug mode, and authentication method.

        Args:
            dbutils: Databricks utility object for widget and secret management.
            debug (bool): Enable debug mode for detailed logging.
            auth_method (str): The authentication method, either "PAT" or "OAuth2".
        """
        # Save initialization parameters
        self.dbutils = dbutils
        self.debug = debug
        self.auth_method = auth_method
        
        # Step 1: Start Spark session
        self.spark = self._initialize_spark()

        # Step 2: Initialize Logger for structured and detailed logging
        self.logger = self._initialize_logger()

        # Step 3: Initialize Helper for utility functions like creating temp views
        self.helper = self._initialize_helper()

        # Step 4: Set up ConfigManager to manage and load configuration
        self.config_manager = self._initialize_config_manager()

        # Step 5: Initialize authentication strategy (PAT or OAuth2) based on configuration
        self.auth_strategy = self._initialize_auth_strategy()

        # Step 6: Set up FileHandler with ConfigManager and AuthStrategy for file operations
        self.file_handler = self._initialize_file_handler()

        # Step 7: Initialize FileProcessor for processing files and converting to DataFrames
        self.file_processor = self._initialize_file_processor()

    def _initialize_spark(self) -> SparkSession:
        """
        Initialize and return a Spark session for distributed data processing.

        Returns:
            SparkSession: The initialized Spark session.
        """
        return SparkSession.builder.appName("ADF Processing Pipeline").getOrCreate()

    def _initialize_logger(self) -> Logger:
        """
        Initialize and return a Logger instance with debugging enabled if specified.

        Returns:
            Logger: Configured logger instance with debug level settings.
        """
        return Logger(debug=self.debug)

    def _initialize_helper(self) -> Helper:
        """
        Initialize and return a Helper instance to support utility functions.

        Returns:
            Helper: Configured helper instance for utility functions.
        """
        return Helper(logger=self.logger, debug=self.debug)

    def _initialize_config_manager(self) -> ConfigManager:
        """
        Initialize and configure ConfigManager for the environment.
        Sets up widgets and loads configuration values.

        Returns:
            ConfigManager: Initialized ConfigManager instance with loaded configurations.
        """
        config_manager = ConfigManager(self.dbutils, logger=self.logger)
        config_manager.initialize_widgets()  # Initialize widgets for user input
        config_manager.load_config()         # Load configurations
        return config_manager

    def _initialize_auth_strategy(self) -> AuthStrategy:
        """
        Initialize authentication strategy using specified method (PAT or OAuth2).
        
        Returns:
            AuthStrategy: Initialized authentication strategy instance.
        """
        return authenticate(auth_method=self.auth_method, dbutils=self.dbutils, logger=self.logger)

    def _initialize_file_handler(self) -> FileHandler:
        """
        Initialize and return a FileHandler instance to manage file operations.
        
        Returns:
            FileHandler: Configured file handler instance for managing file operations.
        """
        return FileHandler(config=self.config_manager, auth_strategy=self.auth_strategy, logger=self.logger)

    def _initialize_file_processor(self) -> FileProcessor:
        """
        Initialize and return a FileProcessor instance to handle JSON files 
        and convert them to structured DataFrames.
        
        Returns:
            FileProcessor: Configured file processor instance for processing JSON files.
        """
        return FileProcessor(file_handler=self.file_handler, spark=self.spark, config=self.config_manager, logger=self.logger)

    def get_spark(self) -> SparkSession:
        """Return the initialized Spark session."""
        return self.spark

    def get_logger(self) -> Logger:
        """Return the initialized Logger instance."""
        return self.logger

    def get_helper(self) -> Helper:
        """Return the initialized Helper instance."""
        return self.helper

    def get_config_manager(self) -> ConfigManager:
        """Return the initialized ConfigManager instance."""
        return self.config_manager

    def get_auth_strategy(self) -> AuthStrategy:
        """Return the initialized AuthStrategy instance."""
        return self.auth_strategy

    def get_file_handler(self) -> FileHandler:
        """Return the initialized FileHandler instance."""
        return self.file_handler

    def get_file_processor(self) -> FileProcessor:
        """Return the initialized FileProcessor instance."""
        return self.file_processor