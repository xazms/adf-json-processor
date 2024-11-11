from utils.logger import Logger
from config.config_manager import ConfigManager
from auth.auth_strategy import authenticate
from auth.auth_strategy import AuthStrategy  # For type hinting

class Initializer:
    """
    Initializer class for setting up Logger, ConfigManager, and AuthStrategy
    instances to standardize the environment setup for ADF pipeline processing.
    """

    def __init__(self, dbutils, debug: bool = True, auth_method: str = "PAT"):
        """
        Initialize the Initializer with dbutils, debug mode, and authentication method.

        Args:
            dbutils: Databricks utility object for widget and secret management.
            debug (bool): Enable debug mode for detailed logging.
            auth_method (str): The authentication method, either "PAT" or "OAuth2".
        """
        self.dbutils = dbutils
        self.debug = debug
        self.auth_method = auth_method
        self.logger = self._initialize_logger()
        self.config_manager = self._initialize_config_manager()
        self.auth_strategy = self._initialize_auth_strategy()

    def _initialize_logger(self) -> Logger:
        """
        Initialize and return a Logger instance.

        Returns:
            Logger: Configured logger instance with debug level settings.
        """
        return Logger(debug=self.debug)

    def _initialize_config_manager(self) -> ConfigManager:
        """
        Initialize and configure ConfigManager for the environment.

        Returns:
            ConfigManager: Initialized ConfigManager instance with loaded configurations.
        """
        config_manager = ConfigManager(self.dbutils, logger=self.logger)
        config_manager.initialize_widgets()
        config_manager.load_config()
        return config_manager

    def _initialize_auth_strategy(self) -> AuthStrategy:
        """
        Initialize authentication strategy using specified method.

        Returns:
            AuthStrategy: Initialized authentication strategy instance.
        """
        return authenticate(auth_method=self.auth_method, dbutils=self.dbutils, logger=self.logger)

    def get_logger(self) -> Logger:
        """Return the initialized Logger instance."""
        return self.logger

    def get_config_manager(self) -> ConfigManager:
        """Return the initialized ConfigManager instance."""
        return self.config_manager

    def get_auth_strategy(self) -> AuthStrategy:
        """Return the initialized AuthStrategy instance."""
        return self.auth_strategy