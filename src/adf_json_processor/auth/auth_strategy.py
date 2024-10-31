from abc import ABC, abstractmethod
import requests
from requests.auth import HTTPBasicAuth

class AuthStrategy(ABC):
    """Abstract base class for different authentication strategies."""

    @abstractmethod
    def authenticate(self, url):
        pass

class PATAuthStrategy(AuthStrategy):
    """
    Implements authentication using Personal Access Token (PAT).
    """
    def __init__(self, pat):
        self.pat = pat

    def authenticate(self, url):
        """
        Authenticate with the given URL using PAT.
        """
        try:
            response = requests.get(url, auth=HTTPBasicAuth('', self.pat))
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed using PAT. Error: {e}")
            return None

def authenticate(dbutils, auth_method="PAT"):
    """
    Factory function to select the authentication strategy.
    
    Args:
        auth_method (str): Either 'PAT' or 'OAuth2'.
    
    Returns:
        AuthStrategy: The authentication strategy.
    """
    if 'dbutils' not in globals() or dbutils is None:
        raise ValueError("dbutils is required to retrieve secrets or widgets in this environment.")
    
    if auth_method == "PAT":
        pat = dbutils.widgets.get("PersonalAccessToken")
        if not pat:
            raise ValueError("Personal Access Token (PAT) is required but not provided.")
        return PATAuthStrategy(pat)
    else:
        raise ValueError(f"Unknown authentication method: {auth_method}. Supported methods are 'PAT'.")