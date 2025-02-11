from abc import ABC, abstractmethod
import requests
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from adf_json_processor.utils.logger import Logger

class AuthStrategy(ABC):
    """
    Abstract base class for different authentication strategies for accessing DevOps resources.
    Provides a common interface for authentication and repository validation.
    """

    def __init__(self, logger=None):
        """
        Initialize the AuthStrategy with a logger.

        Args:
            logger (Logger): Optional logger instance for logging authentication actions.
        """
        self.logger = logger or Logger()

    @abstractmethod
    def authenticate(self, url):
        """Authenticate with the given URL."""
        pass

    @abstractmethod
    def validate_repository(self, adf_config):
        """Validates the DevOps repository based on ADF configuration."""
        pass

class PATAuthStrategy(AuthStrategy):
    """Implements authentication using a Personal Access Token (PAT)."""

    def __init__(self, pat, logger=None):
        """
        Initialize the PATAuthStrategy with a Personal Access Token.

        Args:
            pat (str): Personal Access Token for authentication.
            logger (Logger): Optional logger instance.
        """
        super().__init__(logger)
        self._pat = pat
        self._log_authentication_details()

    def _log_authentication_details(self):
        """Logs masked details of the authentication method for security purposes."""
        masked_pat = '*' * (len(self._pat) - 4) + self._pat[-4:]
        self.logger.log_block("Authentication Details", [
            "Authentication Method: PATAuthStrategy",
            f"Personal Access Token: {masked_pat}"
        ])

    def authenticate(self, url):
        """Performs authentication using PAT with the given URL."""
        try:
            response = requests.get(url, auth=HTTPBasicAuth('', self._pat))
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.logger.log_error(f"Authentication failed. Error: {e}")
            raise

    def validate_repository(self, adf_config):
        """
        Validates the DevOps repository by constructing a URL from the ADF configuration.

        Args:
            adf_config (list): Configuration list with organization, project, and repository details.
        """
        organization, project, repository, *_ = adf_config
        url = f"https://dev.azure.com/{organization}/{project}/_git/{repository}"

        try:
            response = self.authenticate(url)
            if response.ok:
                self.logger.log_block("DevOps Repository Validation", [
                    f"URL: {url}",
                    f"Status Code: {response.status_code}",
                    "Repository validation: Successful"
                ])
            else:
                self.logger.log_error(f"Failed to validate repository. Status Code: {response.status_code}")
        except Exception as e:
            self.logger.log_error(f"Repository validation error: {e}")

class OAuth2AuthStrategy(AuthStrategy):
    """Implements OAuth2 authentication for accessing DevOps."""

    def __init__(self, client_id, client_secret, token_url, logger=None, scope=None):
        """
        Initialize the OAuth2AuthStrategy with client credentials.

        Args:
            client_id (str): OAuth2 client ID.
            client_secret (str): OAuth2 client secret.
            token_url (str): URL to obtain the OAuth2 token.
            logger (Logger): Optional logger instance.
            scope (list): Scope of access required.
        """
        super().__init__(logger)
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_url = token_url
        self._scope = scope or []
        self._session = self._initialize_oauth_session()
        self._log_authentication_details()

    def _initialize_oauth_session(self):
        """Creates an OAuth2 session using client credentials and retrieves the access token."""
        client = BackendApplicationClient(client_id=self._client_id)
        session = OAuth2Session(client=client, scope=self._scope)

        try:
            session.fetch_token(
                token_url=self._token_url,
                client_id=self._client_id,
                client_secret=self._client_secret
            )
        except requests.RequestException as e:
            self.logger.log_error(f"OAuth2 token retrieval failed. Error: {e}")
            raise
        return session

    def _log_authentication_details(self):
        """Logs details of the OAuth2 authentication for debugging and verification purposes."""
        self.logger.log_block("Authentication Details", [
            "Authentication Method: OAuth2AuthStrategy",
            f"Client ID: {self._client_id[:4]}{'*' * (len(self._client_id) - 4)}"
        ])

    def authenticate(self, url):
        """Performs OAuth2 authentication with the provided URL."""
        try:
            response = self._session.get(url)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.logger.log_error(f"OAuth2 authentication failed. Error: {e}")
            raise

    def validate_repository(self, adf_config):
        """
        Validates the DevOps repository by constructing a URL from the ADF configuration.

        Args:
            adf_config (list): Configuration list with organization, project, and repository details.
        """
        organization, project, repository, *_ = adf_config
        url = f"https://dev.azure.com/{organization}/{project}/_git/{repository}"

        try:
            response = self.authenticate(url)
            if response.ok:
                self.logger.log_block("DevOps Repository Validation", [
                    f"URL: {url}",
                    f"Status Code: {response.status_code}",
                    "Repository validation: Successful"
                ])
            else:
                self.logger.log_error(f"Failed to validate repository. Status Code: {response.status_code}")
        except Exception as e:
            self.logger.log_error(f"Repository validation error: {e}")

def authenticate(auth_method="PAT", dbutils=None, logger=None, tenant_id=None, scope=None):
    """
    Factory function to initialize an appropriate authentication strategy.

    Args:
        auth_method (str): The authentication method, either 'PAT' or 'OAuth2'.
        dbutils: Databricks utility object for retrieving secrets.
        logger (Logger): Logger instance for logging authentication details.
        tenant_id (str): Tenant ID for OAuth2, required if using 'OAuth2'.
        scope (list): List of scopes required for OAuth2 access.

    Returns:
        AuthStrategy: An instance of the selected authentication strategy.
    """
    if not dbutils:
        raise ValueError("dbutils is required to retrieve secrets or widgets in this environment.")

    logger = logger or Logger()

    try:
        if auth_method == "PAT":
            pat = dbutils.secrets.get(scope="shared-key-vault", key="token-devops-PAT-data-quality")
            if not pat:
                raise ValueError("Personal Access Token (PAT) is required but not provided.")
            return PATAuthStrategy(pat, logger=logger)
        elif auth_method == "OAuth2":
            client_id = dbutils.secrets.get(scope="shared-key-vault", key="client-id-devops")
            client_secret = dbutils.secrets.get(scope="shared-key-vault", key="client-secret-devops")
            if not tenant_id:
                raise ValueError("Tenant ID is required for OAuth2 authentication.")
            token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            return OAuth2AuthStrategy(client_id, client_secret, token_url, logger=logger, scope=scope)
        else:
            logger.log_error(f"Unknown authentication method: {auth_method}. Supported methods are 'PAT' and 'OAuth2'.")
            raise ValueError(f"Unknown authentication method: {auth_method}. Supported methods are 'PAT' and 'OAuth2'.")
    except Exception as e:
        logger.log_error(f"Failed to initialize authentication strategy. Error: {e}")
        raise