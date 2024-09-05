from abc import ABC, abstractmethod
import requests
from requests.auth import HTTPBasicAuth

class AuthStrategy(ABC):
    """Abstract base class for different authentication strategies."""
    
    @abstractmethod
    def authenticate(self, url):
        pass

class PATAuthStrategy(AuthStrategy):
    def __init__(self, pat):
        self.pat = pat

    def authenticate(self, url):
        try:
            response = requests.get(url, auth=HTTPBasicAuth('', self.pat))
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed using PAT. Error: {e}")
            return None

class OAuth2AuthStrategy(AuthStrategy):
    def __init__(self, token):
        self.token = token

    def authenticate(self, url):
        headers = {'Authorization': f'Bearer {self.token}'}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed using OAuth2. Error: {e}")
            return None

def authenticate(auth_method="PAT"):
    """
    Authenticate based on the selected method and return the appropriate authentication strategy.
    """
    # Check if dbutils is available in the global scope
    if 'dbutils' not in globals() or dbutils is None:
        raise ValueError("dbutils is required to retrieve secrets or widgets in this environment.")
    
    if auth_method == "PAT":
        # Retrieve the Personal Access Token (PAT) from the Databricks widget
        pat = dbutils.widgets.get("PersonalAccessToken")
        if not pat:
            raise ValueError("Personal Access Token (PAT) is required but not provided.")
        return PATAuthStrategy(pat)

    elif auth_method == "OAuth2":
        token = dbutils.widgets.get("OAuth2Token")
        if not token:
            raise ValueError("OAuth2 Token is required but not provided.")
        return OAuth2AuthStrategy(token)

    else:
        raise ValueError(f"Unknown authentication method: {auth_method}. Supported methods are 'PAT' and 'OAuth2'.")