import unittest
from unittest.mock import MagicMock, patch
from adf_json_processor.auth.auth_manager import Authenticator

class TestAuthenticator(unittest.TestCase):
    """Unit tests for the Authenticator class."""

    def setUp(self):
        """Setup common mocks before each test."""
        self.mock_dbutils = MagicMock()
        self.mock_dbutils.secrets.get.side_effect = lambda scope, key: "mocked_token"
        self.mock_dbutils.widgets.get.side_effect = lambda key: "mocked_key"

    @patch("requests.post")
    def test_initialize_authenticator_pat(self, mock_post):
        """Test initialization with PAT authentication."""
        authenticator = Authenticator(dbutils=self.mock_dbutils, auth_method="PAT", debug=True)
        self.assertEqual(authenticator.token, "mocked_token")

    @patch("requests.post")
    def test_initialize_authenticator_oauth2(self, mock_post):
        """Test initialization with OAuth2 authentication."""
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"access_token": "mocked_oauth_token"}
        
        authenticator = Authenticator(dbutils=self.mock_dbutils, auth_method="OAUTH2", debug=True)
        self.assertEqual(authenticator.token, "mocked_oauth_token")

    def test_get_session(self):
        """Test authenticated session creation."""
        authenticator = Authenticator(dbutils=self.mock_dbutils, auth_method="PAT", debug=True)
        session = authenticator.get_session()
        self.assertIsNotNone(session)

    def test_invalid_auth_method(self):
        """Test invalid authentication method handling."""
        with self.assertRaises(ValueError):
            Authenticator(dbutils=self.mock_dbutils, auth_method="INVALID", debug=True)

if __name__ == "__main__":
    unittest.main()
