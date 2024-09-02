import unittest
from unittest.mock import patch, MagicMock
from file_handling.file_handler import FileHandler
from config.config import Config

class TestFileHandler(unittest.TestCase):

    def setUp(self):
        # Mock dbutils and auth strategy
        self.mock_dbutils = MagicMock()
        self.mock_auth_strategy = MagicMock()
        
        # Set up a basic configuration
        self.mock_config = Config(self.mock_dbutils, self.mock_auth_strategy, debug=True)

        # Initialize the FileHandler with the mock config
        self.file_handler = FileHandler(self.mock_config)

    @patch('file_handling.file_handler.requests.get')
    def test_get_adf_files_success(self, mock_get):
        # Mock a successful API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'value': [
                {'path': '/path/to/file1.json', 'isFolder': False},
                {'path': '/path/to/file2.json', 'isFolder': False}
            ]
        }
        mock_get.return_value = mock_response

        # Call the method
        result = self.file_handler.get_adf_files()

        # Assertions
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['path'], '/path/to/file1.json')
        self.assertEqual(result[1]['path'], '/path/to/file2.json')

    @patch('file_handling.file_handler.requests.get')
    def test_get_adf_files_failure(self, mock_get):
        # Mock a failed API response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        # Assert an exception is raised
        with self.assertRaises(Exception) as context:
            self.file_handler.get_adf_files()

        self.assertTrue('Failed to list items in the repository' in str(context.exception))

    @patch('file_handling.file_handler.requests.get')
    def test_get_adf_file_content_success(self, mock_get):
        # Mock a successful file content retrieval
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"key": "value"}'
        mock_get.return_value = mock_response

        # Call the method
        result = self.file_handler.get_adf_file_content('/path/to/file.json')

        # Assertions
        self.assertEqual(result, '{"key": "value"}')

    @patch('file_handling.file_handler.requests.get')
    def test_get_adf_file_content_failure(self, mock_get):
        # Mock a failed file content retrieval
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        # Assert an exception is raised
        with self.assertRaises(Exception) as context:
            self.file_handler.get_adf_file_content('/path/to/file.json')

        self.assertTrue('Failed to retrieve file' in str(context.exception))

    def test_get_filtered_file_list(self):
        # Mock the response of get_adf_files
        self.file_handler.get_adf_files = MagicMock(return_value=[
            {'path': '/path/to/file1.json'},
            {'path': '/path/to/file2.json'},
            {'path': '/path/to/file3.json'}
        ])
        
        # Set the source_filename to a pattern
        self.file_handler.config.source_filename = "file1.json"

        # Call the method
        result = self.file_handler.get_filtered_file_list()

        # Assertions
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['path'], '/path/to/file1.json')

    def test_log_errors(self):
        # Mock the open function
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            error_log = [
                {"file": "/path/to/file.json", "error": "KeyError", "details": "Missing key"}
            ]

            # Call the method
            self.file_handler.log_errors(error_log)

            # Assert the file was opened in write mode
            mock_file.assert_called_once_with(self.file_handler.config.log_path, "w")

            # Assert that json.dump was called
            mock_file().write.assert_called()

if __name__ == '__main__':
    unittest.main()