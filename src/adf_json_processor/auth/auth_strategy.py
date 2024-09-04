from abc import ABC, abstractmethod
import requests
from requests.auth import HTTPBasicAuth

class AuthStrategy(ABC):
    """Abstract base class for different authentication strategies."""
    
    @abstractmethod
    def authenticate(self, url):
        """
        Abstract method to be implemented by child classes for specific authentication.
        Args:
            url (str): The URL to authenticate against.
        """
        pass

class PATAuthStrategy(AuthStrategy):
    """Authentication strategy using Personal Access Token (PAT)."""
    
    def __init__(self, pat):
        """
        Initialize the PAT authentication strategy with the provided token.
        Args:
            pat (str): Personal Access Token for authentication.
        """
        self.pat = pat

    def authenticate(self, url):
        """
        Authenticate using the provided PAT and return the response.
        Args:
            url (str): The URL to authenticate against.
        Returns:
            Response object if successful, None otherwise.
        """
        try:
            # Perform the request with PAT-based authentication
            response = requests.get(url, auth=HTTPBasicAuth('', self.pat))
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed using PAT. Error: {e}")
            return None

class OAuth2AuthStrategy(AuthStrategy):
    """Authentication strategy using OAuth2 token."""
    
    def __init__(self, token):
        """
        Initialize the OAuth2 authentication strategy with the provided token.
        Args:
            token (str): OAuth2 token for authentication.
        """
        self.token = token

    def authenticate(self, url):
        """
        Authenticate using the provided OAuth2 token and return the response.
        Args:
            url (str): The URL to authenticate against.
        Returns:
            Response object if successful, None otherwise.
        """
        headers = {'Authorization': f'Bearer {self.token}'}
        try:
            # Perform the request with OAuth2 token-based authentication
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed using OAuth2. Error: {e}")
            return None

def authenticate(auth_method="PAT", dbutils=None):
    """
    Authenticate based on the selected method and return the appropriate authentication strategy.
    Args:
        auth_method (str): The method to use for authentication ('PAT' or 'OAuth2'). Default is 'PAT'.
        dbutils (object): Databricks utility object to access widgets or secrets.
    Returns:
        An instance of the appropriate authentication strategy.
    """
    if dbutils is None:
        raise ValueError("dbutils is required to retrieve secrets or widgets in this environment.")
    
    if auth_method == "PAT":
        # Retrieve the Personal Access Token (PAT) from the Databricks widget
        pat = dbutils.widgets.get("PersonalAccessToken")
        if not pat:
            raise ValueError("Personal Access Token (PAT) is required but not provided.")
        auth_strategy = PATAuthStrategy(pat)

    elif auth_method == "OAuth2":
        # Retrieve the OAuth2 token from the Databricks widget
        token = dbutils.widgets.get("OAuth2Token")
        if not token:
            raise ValueError("OAuth2 Token is required but not provided.")
        auth_strategy = OAuth2AuthStrategy(token)

    else:
        raise ValueError(f"Unknown authentication method: {auth_method}. Supported methods are 'PAT' and 'OAuth2'.")

    return auth_strategy