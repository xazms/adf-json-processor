import unittest
from unittest.mock import MagicMock
from adf_json_processor.config.config_manager import Config

class TestConfig(unittest.TestCase):
    """Unit tests for the Config class."""

    def setUp(self):
        """Setup common mocks before each test."""
        self.mock_dbutils = MagicMock()
        self.mock_dbutils.widgets.get.side_effect = lambda key: {
            "ADFConfig": '["org", "project", "repo", "main", "folder"]',
            "SourceStorageAccount": "source_account",
            "DestinationStorageAccount": "dest_account",
            "Datasetidentifier": "dataset_id",
            "SourceFileName": "*"
        }.get(key, "")

        self.mock_authenticator = MagicMock()

    def test_initialize_config(self):
        """Test Config initialization with mocked dbutils."""
        config = Config(dbutils=self.mock_dbutils, authenticator=self.mock_authenticator, debug=True)
        self.assertEqual(config.organization, "org")
        self.assertEqual(config.source_storage_account, "source_account")

    def test_missing_dbutils(self):
        """Test that missing dbutils raises an error."""
        with self.assertRaises(ValueError):
            Config(dbutils=None, authenticator=self.mock_authenticator, debug=True)

    def test_validate_config(self):
        """Test configuration validation with valid settings."""
        config = Config(dbutils=self.mock_dbutils, authenticator=self.mock_authenticator, debug=True)
        self.assertIsNotNone(config.organization)

    def test_invalid_adf_config(self):
        """Test handling of invalid ADFConfig format."""
        self.mock_dbutils.widgets.get.side_effect = lambda key: "invalid_string" if key == "ADFConfig" else ""
        with self.assertRaises(ValueError):
            Config(dbutils=self.mock_dbutils, authenticator=self.mock_authenticator, debug=True)

if __name__ == "__main__":
    unittest.main()