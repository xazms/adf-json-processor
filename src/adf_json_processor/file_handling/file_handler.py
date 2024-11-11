import json
import fnmatch
from utils.logger import Logger

class FileHandler:
    """
    Manages file operations, including retrieving and filtering ADF JSON files from an Azure DevOps repository.
    """

    def __init__(self, config, auth_strategy, logger=None):
        """
        Initializes the FileHandler with configuration, authentication, and logging.

        Args:
            config (ConfigManager): Instance for retrieving configuration values.
            auth_strategy (AuthStrategy): Authentication strategy for accessing the DevOps repository.
            logger (Logger): Optional logger instance for structured logging.
        """
        self.config = config
        self.auth_strategy = auth_strategy
        self.logger = logger or Logger()

    def get_adf_file_content(self, file_path):
        """
        Retrieves the content of a specific ADF JSON file from Azure DevOps.

        Args:
            file_path (str): The path of the file to retrieve.

        Returns:
            str: Raw content of the JSON file if retrieval is successful.

        Raises:
            Exception: If the file retrieval fails with a non-200 status code.
        """
        file_url = self._construct_file_url(file_path)
        response = self.auth_strategy.authenticate(file_url)

        if response.status_code == 200:
            return response.text
        else:
            self.logger.log_error(f"Failed to retrieve file: {file_path} - Status Code: {response.status_code}")
            raise Exception(f"Failed to retrieve file: {file_path} - Status Code: {response.status_code}")

    def get_adf_files(self):
        """
        Retrieves all ADF JSON files from the Azure DevOps repository without logging.

        Returns:
            list: List of JSON files in the repository.
        """
        repo_url = self._construct_repo_url()
        response = self.auth_strategy.authenticate(repo_url)

        if response.status_code != 200:
            self.logger.log_error(f"Failed to list items in the repository: Status Code {response.status_code}")
            raise Exception(f"Failed to list items in the repository: {response.status_code}")

        adf_files = response.json().get('value', [])
        return [file for file in adf_files if not file.get('isFolder') and file['path'].endswith('.json')]

    def get_filtered_file_list(self, show_all=False, top_n=10, debug=False):
        """
        Filters the ADF files based on the source filename pattern from the configuration.

        Args:
            show_all (bool, optional): If True, logs all filtered file paths, ignoring top_n.
            top_n (int, optional): Number of files to display if show_all is False. Default is 10.
            debug (bool, optional): If True, logs both "Total files" and "Total files after filtering".

        Returns:
            list: Filtered list of JSON files.
        """
        json_files = self.get_adf_files()
        total_files = len(json_files)

        if debug:
            self._log_total_files(total_files)

        source_filename = self.config.get_config()["sourceFileName"]

        # Apply filename filter based on the source filename pattern
        if source_filename == "*" or source_filename == "*.json":
            filtered_files = json_files
        else:
            filtered_files = [file for file in json_files if fnmatch.fnmatch(file['path'], f"*{source_filename}")]

        total_filtered = len(filtered_files)
        
        # Determine files to display based on show_all and top_n
        display_files = filtered_files if show_all else filtered_files[:top_n]

        # Log filtered file summary if top_n is specified or show_all is True, and debug is True
        if (show_all or top_n) and debug:
            self._log_filtered_file_summary(total_files, total_filtered, source_filename, display_files, show_all, top_n)

        return display_files

    def _construct_file_url(self, file_path):
        """
        Constructs the URL for retrieving a specific file from Azure DevOps.

        Args:
            file_path (str): The path of the file.

        Returns:
            str: The constructed URL.
        """
        adf_config = self.config.get_config()["adfConfig"]
        return (
            f"https://dev.azure.com/{adf_config[0]}/{adf_config[1]}/_apis/git/repositories/"
            f"{adf_config[2]}/items?path={file_path}&versionDescriptor.version={adf_config[3]}"
            f"&api-version=6.0&$format=octetStream"
        )

    def _construct_repo_url(self):
        """
        Constructs the URL for listing all files in the repository from Azure DevOps.

        Returns:
            str: The constructed URL for the repository.
        """
        adf_config = self.config.get_config()["adfConfig"]
        return (
            f"https://dev.azure.com/{adf_config[0]}/{adf_config[1]}/_apis/git/repositories/"
            f"{adf_config[2]}/items?scopePath={adf_config[4]}&recursionLevel=Full&"
            f"versionDescriptor.version={adf_config[3]}&api-version=6.0"
        )

    def _log_total_files(self, total_files):
        """
        Logs the total number of files in the repository.

        Args:
            total_files (int): Total number of files.
        """
        self.logger.log_block("Total Files in Repository", [f"Total files: {total_files}"])

    def _log_filtered_file_summary(self, total_files, total_filtered, source_filename, display_files, show_all, top_n):
        """
        Logs a summary of filtered files based on the source filename pattern.

        Args:
            total_files (int): Total number of files before filtering.
            total_filtered (int): Total number of files after filtering.
            source_filename (str): Filename pattern used for filtering.
            display_files (list): List of files to display in the log.
            show_all (bool): If True, log all filtered files.
            top_n (int): Number of filtered files to log if show_all is False.
        """
        summary_message = [
            f"Total files before filtering: {total_files}",
            f"Total files after filtering: {total_filtered}",
            f"Filter pattern: {source_filename}",
            f"Displaying {'all' if show_all else f'top {top_n}'} matching files:"
        ] + [file['path'] for file in display_files]

        self.logger.log_block("Filtered ADF Files Summary", summary_message)