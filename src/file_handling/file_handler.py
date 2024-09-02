import json
import requests
import fnmatch

class FileHandler:
    def __init__(self, config):
        self.config = config
        self.error_log_path = config.log_path
        self.output_file_path = config.output_path

    def update_source_filename(self, source_filename):
        """Update the source filename in the configuration."""
        self.config.update_source_filename(source_filename)

    def get_adf_files(self):
        """Retrieve ADF JSON files from the specified Azure DevOps repository."""
        url = f'https://dev.azure.com/{self.config.adf_details["Organization"]}/{self.config.adf_details["Project"]}/_apis/git/repositories/{self.config.adf_details["Repository"]}/items?scopePath={self.config.adf_details["Folder Path"]}&recursionLevel=Full&versionDescriptor.version={self.config.adf_details["Branch"]}&api-version=6.0'

        response = self.config.auth_strategy.authenticate(url)
        
        if response.status_code == 200:
            adf_files = response.json()['value']
            json_files = [file for file in adf_files if not file.get('isFolder') and file['path'].endswith('.json')]
            return json_files
        else:
            raise Exception(f"Failed to list items in the repository: {response.status_code}")

    def get_filtered_file_list(self):
        """Return the filtered list of files based on the configuration."""
        json_files = self.get_adf_files()
        if self.config.source_filename == "*":
            return json_files
        else:
            # Use fnmatch for wildcard pattern matching
            filtered_files = [file for file in json_files if fnmatch.fnmatch(file['path'], f"*{self.config.source_filename}")]
            return filtered_files

    def get_adf_file_content(self, file_path):
        """Retrieve the content of a specific ADF JSON file from Azure DevOps."""
        file_url = f"https://dev.azure.com/{self.config.adf_details['Organization']}/{self.config.adf_details['Project']}/_apis/git/repositories/{self.config.adf_details['Repository']}/items?path={file_path}&versionDescriptor.version={self.config.adf_details['Branch']}&api-version=6.0&$format=octetStream"

        file_response = self.config.auth_strategy.authenticate(file_url)
        
        if file_response.status_code == 200:
            return file_response.text  # Return the raw content of the JSON file
        else:
            raise Exception(f"Failed to retrieve file: {file_path} - {file_response.status_code}")

    def log_errors(self, error_log):
        """Log errors to a file if any occurred."""
        if error_log:
            self.config.create_directory_if_not_exists(self.error_log_path)
            with open(self.error_log_path, "w") as log_file:
                json.dump(error_log, log_file, indent=4)
            print(f"Errors logged to {self.error_log_path}")