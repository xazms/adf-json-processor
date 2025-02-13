import requests
from requests.auth import HTTPBasicAuth
from adf_json_processor.utils.logger import Logger

class Authenticator:
    """
    Centralized authentication class to manage and execute different authentication methods.
    Supports 'PAT' (Personal Access Token) and 'OAuth2'.
    """

    def __init__(self, dbutils=None, auth_method="PAT", debug=False, logger=None):
        """
        Initialize the Authenticator.

        Args:
            dbutils (object, optional): Databricks utilities object for secrets and widgets.
            auth_method (str): The authentication method to use ('PAT' or 'OAuth2').
            debug (bool): Enable debug-level logging.
            logger (Logger, optional): Custom logger instance. If not provided, a new instance is created.
        """
        # Use provided logger or initialize one with the given debug setting.
        self.logger = logger if logger is not None else Logger(debug=debug)
        # Use the provided dbutils or grab it from globals.
        self.dbutils = dbutils or globals().get("dbutils")
        self.auth_method = auth_method.upper()
        self.debug = debug
        self.session = None  # For caching the authenticated session

        if not self.dbutils:
            error_message = "dbutils is required to retrieve secrets or widgets."
            self.logger.log_message(error_message, level="error")
            raise ValueError(error_message)

        self.logger.log_start("Authenticator Initialization")
        try:
            self.token = self._retrieve_token()
            self.logger.log_info(f"Authenticator initialized successfully with method '{self.auth_method}'.")
        except Exception as e:
            self.logger.log_message(f"Failed to initialize Authenticator: {e}", level="error")
            raise
        finally:
            self.logger.log_end("Authenticator Initialization")

    def _get_secret(self, scope, key):
        """
        Retrieve a secret from Databricks KeyVault.

        Args:
            scope (str): The secret scope.
            key (str): The key name.

        Returns:
            str: The retrieved secret.
        """
        try:
            self.logger.log_debug(f"Retrieving secret '{key}' from scope '{scope}'...")
            secret = self.dbutils.secrets.get(scope=scope, key=key)
            if not secret:
                raise ValueError(f"Secret '{key}' is missing in the scope '{scope}'.")
            return secret
        except Exception as e:
            self.logger.log_error(f"Error retrieving secret '{key}': {e}")
            raise

    def _retrieve_token(self):
        """
        Retrieve the token based on the selected authentication method.
        """
        if self.auth_method == "PAT":
            return self._retrieve_pat_token()
        elif self.auth_method == "OAUTH2":
            return self._retrieve_oauth_token()
        else:
            error_message = f"Unsupported authentication method: {self.auth_method}"
            self.logger.log_message(error_message, level="error")
            raise ValueError(error_message)

    def _retrieve_pat_token(self):
        """
        Retrieve a Personal Access Token (PAT) from KeyVault.

        Returns:
            str: The retrieved PAT.
        """
        # Retrieve the widget value that indicates the key name for the PAT
        pat_widget_value = self.dbutils.widgets.get("PersonalAccessTokenKeyVaultName")
        return self._get_secret("shared-key-vault", pat_widget_value)

    def _retrieve_oauth_token(self):
        """
        Retrieve an OAuth2 token using client credentials.

        Returns:
            str: The retrieved OAuth2 token.

        Raises:
            ValueError: If the OAuth2 token cannot be retrieved.
        """
        try:
            client_id = self._get_secret("shared-key-vault", "OAuth2ClientId")
            client_secret = self._get_secret("shared-key-vault", "OAuth2ClientSecret")
            token_url = self._get_secret("shared-key-vault", "OAuth2TokenUrl")

            payload = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
            self.logger.log_debug("Requesting OAuth2 token...")
            response = requests.post(token_url, data=payload, timeout=10)

            if response.status_code == 200:
                token = response.json().get("access_token")
                if not token:
                    raise ValueError("OAuth2 token is missing in the response.")
                return token
            else:
                raise ValueError(f"Failed to retrieve OAuth2 token: {response.text}")
        except Exception as e:
            self.logger.log_error(f"Error retrieving OAuth2 token: {e}")
            raise

    def get_session(self):
        """
        Returns an authenticated session for making requests.

        Returns:
            requests.Session: A session with authentication headers.
        """
        if self.session:
            return self.session  # Reuse existing session

        self.logger.log_debug("Creating a new authenticated session...")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "ADF-Authenticator"})

        if self.auth_method == "PAT":
            self.session.auth = HTTPBasicAuth("", self.token)
        elif self.auth_method == "OAUTH2":
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

        return self.session

    @staticmethod
    def initialize(dbutils=None, auth_method="PAT", debug=False, logger=None):
        """
        Factory method to initialize the Authenticator class.

        Args:
            dbutils: Databricks utilities for retrieving secrets.
            auth_method (str): Authentication method ('PAT' or 'OAuth2').
            debug (bool): Enable debug-level logging.
            logger (Logger, optional): A custom logger instance.

        Returns:
            Authenticator: An initialized Authenticator instance.
        """
        temp_logger = logger if logger is not None else Logger(debug=debug)

        if not dbutils:
            dbutils = globals().get("dbutils")  # Use existing dbutils if available

        if not dbutils:
            error_message = "dbutils is not available. Ensure it is properly initialized in your environment."
            temp_logger.log_error(error_message)
            raise ValueError(error_message)

        try:
            return Authenticator(dbutils=dbutils, auth_method=auth_method, debug=debug, logger=logger)
        except Exception as e:
            temp_logger.log_error(f"Failed to initialize Authenticator: {e}")
            raise

    def test_authentication(self):
        """
        Simple built-in test for initializing the authenticator and verifying functionality.
        """
        try:
            print("🔹 Running Authenticator Test...")
            # Test if token is retrieved
            if self.token:
                print("✅ Authenticator initialized successfully.")
            else:
                print("❌ Authenticator failed: No token retrieved.")
            # Test session creation
            session = self.get_session()
            if session:
                print("✅ Authenticated session created successfully.")
            else:
                print("❌ Failed to create an authenticated session.")
        except Exception as e:
            print(f"❌ Test failed with error: {e}")