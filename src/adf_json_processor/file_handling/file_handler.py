import json
import fnmatch  # Necessary for file name pattern matching
from adf_json_processor.config.config import Config
from adf_json_processor.auth.auth_strategy import AuthStrategy

class FileHandler:
    def __init__(self, config):
        """
        Initialize the FileHandler with the provided configuration.
        
        Args:
            config (Config): Configuration object containing ADF details, paths, and authentication strategy.
        """
        self.config = config
        self.error_log_path = config.log_path
        self.output_file_path = config.output_path

    def update_source_filename(self, source_filename):
        """
        Update the source filename in the configuration.
        
        Args:
            source_filename (str): The new source file name or pattern.
        """
        self.config.update_source_filename(source_filename)

    def get_adf_files(self):
        """
        Retrieve ADF JSON files from the specified Azure DevOps repository.
        
        Returns:
            list: A list of JSON files found in the repository.
        
        Raises:
            Exception: If the request to retrieve files fails.
        """
        # Construct the Azure DevOps API URL
        url = (f'https://dev.azure.com/{self.config.adf_details["Organization"]}/'
               f'{self.config.adf_details["Project"]}/_apis/git/repositories/'
               f'{self.config.adf_details["Repository"]}/items?scopePath='
               f'{self.config.adf_details["Folder Path"]}&recursionLevel=Full&'
               f'versionDescriptor.version={self.config.adf_details["Branch"]}&api-version=6.0')

        # Authenticate and retrieve files
        response = self.config.auth_strategy.authenticate(url)
        
        if response.status_code == 200:
            adf_files = response.json().get('value', [])
            # Filter for JSON files that are not folders
            json_files = [file for file in adf_files if not file.get('isFolder') and file['path'].endswith('.json')]
            return json_files
        else:
            raise Exception(f"Failed to list items in the repository: {response.status_code}")

    def get_filtered_file_list(self):
        """
        Filter the list of ADF JSON files based on the provided source filename pattern.
        
        Returns:
            list: A filtered list of JSON files.
        """
        # Retrieve all ADF JSON files
        json_files = self.get_adf_files()
        # Filter based on the provided filename pattern or return all files
        if self.config.source_filename == "*":
            return json_files
        else:
            return [file for file in json_files if fnmatch.fnmatch(file['path'], f"*{self.config.source_filename}")]

    def get_adf_file_content(self, file_path):
        """
        Retrieve the content of a specific ADF JSON file from Azure DevOps.
        
        Args:
            file_path (str): The path of the file to retrieve.
        
        Returns:
            str: The raw content of the JSON file.
        
        Raises:
            Exception: If the request to retrieve the file fails.
        """
        # Construct the Azure DevOps API URL to retrieve the file content
        file_url = (f"https://dev.azure.com/{self.config.adf_details['Organization']}/"
                    f"{self.config.adf_details['Project']}/_apis/git/repositories/"
                    f"{self.config.adf_details['Repository']}/items?path={file_path}&"
                    f"versionDescriptor.version={self.config.adf_details['Branch']}&"
                    f"api-version=6.0&$format=octetStream")

        # Authenticate and retrieve file content
        file_response = self.config.auth_strategy.authenticate(file_url)
        
        if file_response.status_code == 200:
            return file_response.text  # Return the raw content of the JSON file
        else:
            raise Exception(f"Failed to retrieve file: {file_path} - {file_response.status_code}")

    def log_errors(self, error_log):
        """
        Log any errors that occurred during the process to a file.
        
        Args:
            error_log (dict): A dictionary containing the errors to be logged.
        """
        if error_log:
            # Ensure the directory for logs exists and log the errors
            # self.config.ensure_directories_exist()
            with open(self.error_log_path, "a") as log_file:
                json.dump(error_log, log_file, indent=4)
            print(f"Errors logged to {self.error_log_path}")