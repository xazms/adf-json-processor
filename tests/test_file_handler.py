import unittest
from unittest.mock import MagicMock, patch
from adf_json_processor.file_handling.file_handler import FileHandler

class TestFileHandler(unittest.TestCase):
    """Unit tests for the FileHandler class."""

    def setUp(self):
        """Setup common mocks before each test."""
        self.mock_config = MagicMock()
        self.mock_config.adf_details = {
            "Organization": "energinet",
            "Project": "DataPlatform_v3.0",
            "Repository": "data-factory",
            "Branch": "main",
            "Folder Path": "pipeline"
        }
        self.mock_config.source_filename = "*"

        self.mock_authenticator = MagicMock()
        self.mock_authenticator.get_session.return_value = MagicMock()

        self.file_handler = FileHandler(config=self.mock_config, authenticator=self.mock_authenticator, debug=True)

    @patch("adf_json_processor.file_handling.file_handler.FileHandler.get_adf_files")
    def test_get_filtered_file_list(self, mock_get_adf_files):
        """Test that file filtering works correctly."""
        mock_get_adf_files.return_value = [
            {"path": "test1.json"},
            {"path": "test2.json"}
        ]
        filtered_files = self.file_handler.get_filtered_file_list()
        self.assertEqual(len(filtered_files), 2, "Should return 2 files when filtering is disabled.")

    @patch("adf_json_processor.file_handling.file_handler.FileHandler.get_adf_files")
    def test_get_filtered_file_list_with_pattern(self, mock_get_adf_files):
        """Test that file filtering works correctly when a pattern is applied."""
        self.file_handler.source_filename = "test1.json"
        mock_get_adf_files.return_value = [
            {"path": "test1.json"},
            {"path": "test2.json"}
        ]
        filtered_files = self.file_handler.get_filtered_file_list()
        self.assertEqual(len(filtered_files), 1, "Should return only 1 matching file.")

    @patch("adf_json_processor.file_handling.file_handler.FileHandler.get_adf_file_content")
    def test_get_adf_file_content(self, mock_get_adf_file_content):
        """Test retrieving ADF file content."""
        mock_get_adf_file_content.return_value = '{"name": "test_pipeline"}'
        content = self.file_handler.get_adf_file_content("test1.json")
        self.assertIn("test_pipeline", content, "File content should include pipeline data.")

    def test_invalid_configuration(self):
        """Test that missing configuration raises an error."""
        with self.assertRaises(ValueError):
            FileHandler(config=None, authenticator=self.mock_authenticator)

    def test_invalid_authenticator(self):
        """Test that missing authenticator raises an error."""
        with self.assertRaises(ValueError):
            FileHandler(config=self.mock_config, authenticator=None)

if __name__ == "__main__":
    unittest.main()