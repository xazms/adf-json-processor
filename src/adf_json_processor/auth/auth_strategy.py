from abc import ABC, abstractmethod
import requests
from requests.auth import HTTPBasicAuth
from pyspark.dbutils import DBUtils

class AuthStrategy(ABC):
    """Abstract base class for different authentication strategies."""
    
    @abstractmethod
    def authenticate(self, url):
        pass

class PATAuthStrategy(AuthStrategy):
    """Authentication strategy using Personal Access Token (PAT)."""
    
    def __init__(self, pat):
        self.pat = pat

    def authenticate(self, url):
        """Authenticate using a PAT and return the response."""
        try:
            response = requests.get(url, auth=HTTPBasicAuth('', self.pat))
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed using PAT. Error: {e}")
            return None

class OAuth2AuthStrategy(AuthStrategy):
    """Authentication strategy using OAuth2 token."""
    
    def __init__(self, token):
        self.token = token

    def authenticate(self, url):
        """Authenticate using an OAuth2 token and return the response."""
        headers = {'Authorization': f'Bearer {self.token}'}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed using OAuth2. Error: {e}")
            return None

# Authenticate

def authenticate(auth_method="PAT"):
    """
    Authenticate based on the selected method and return the authentication strategy.
    """
    if auth_method == "PAT":
        pat = dbutils.widgets.get("PersonalAccessToken")
        if not pat:
            raise ValueError("Personal Access Token (PAT) is required but not provided.")
        auth_strategy = PATAuthStrategy(pat)
        # print("Using PAT Authentication Strategy.")  # Debug message

    elif auth_method == "OAuth2":
        token = dbutils.widgets.get("OAuth2Token")
        if not token:
            raise ValueError("OAuth2 Token is required but not provided.")
        auth_strategy = OAuth2AuthStrategy(token)
        # print("Using OAuth2 Authentication Strategy.")  # Debug message

    else:
        raise ValueError(f"Unknown authentication method: {auth_method}. Supported methods are 'PAT' and 'OAuth2'.")

    return auth_strategy