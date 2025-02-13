import unittest
import json
import os
from unittest.mock import MagicMock, patch
from adf_json_processor.utils.helper import Helper

class TestHelper(unittest.TestCase):
    """Unit tests for the Helper class."""

    def setUp(self):
        """Setup common dependencies before each test."""
        self.mock_logger = MagicMock()
        self.helper = Helper(logger=self.mock_logger)

    def test_generate_hash_key(self):
        """Test generating a hash key."""
        hash_key = self.helper.generate_hash_key("Test", "123")
        self.assertIsInstance(hash_key, str)
        self.assertEqual(len(hash_key), 64, "SHA256 hash should be 64 characters long.")

    def test_generate_unique_id(self):
        """Test generating a unique UUID."""
        unique_id = self.helper.generate_unique_id()
        self.assertIsInstance(unique_id, str)
        self.assertEqual(len(unique_id), 36, "UUID should be 36 characters long.")

    def test_extract_last_part(self):
        """Test extracting the last part of a file path."""
        path = "/dbfs/mnt/storage/file.json"
        last_part = self.helper.extract_last_part(path)
        self.assertEqual(last_part, "file.json", "Should return last part of path.")

    def test_parse_json(self):
        """Test parsing a JSON string."""
        json_str = '{"key": "value"}'
        parsed_data = self.helper.parse_json(json_str)
        self.assertIsInstance(parsed_data, dict)
        self.assertEqual(parsed_data["key"], "value")

    @patch("builtins.open", create=True)
    def test_save_json_to_file(self, mock_open):
        """Test saving JSON to a file."""
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        test_data = {"name": "Test"}
        file_path = "/tmp/test_output.json"
        self.helper.save_json_to_file(test_data, file_path)

        mock_open.assert_called_once_with(file_path, "w")
        mock_file.write.assert_called_once()

if __name__ == "__main__":
    unittest.main()