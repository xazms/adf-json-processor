import json
import fnmatch
from adf_json_processor.utils.logger import Logger
from adf_json_processor.config.config_manager import Config

class FileHandler:
    """
    Handles file operations such as retrieving and reading ADF JSON files from Azure DevOps.
    """

    def __init__(self, config, authenticator, debug=False, logger=None):
        """
        Initialize the FileHandler with configuration and authentication.

        Args:
            config (Config): The configuration object with all required settings.
            authenticator (Authenticator): Instance of the Authenticator class for managing authentication.
            debug (bool): Enable debug-level logging if True.
            logger (Logger, optional): A custom logger instance. If not provided, a new one is created.
        """
        self.logger = logger if logger is not None else Logger(debug=debug)
        self.config = config
        self.authenticator = authenticator
        self.debug = debug

        if not self.config:
            self.logger.log_error("Config instance is required to initialize FileHandler.")
            raise ValueError("Config instance is required to initialize FileHandler.")

        if not self.authenticator:
            self.logger.log_error("Authenticator instance is required to initialize FileHandler.")
            raise ValueError("Authenticator instance is required to initialize FileHandler.")

        # Authenticate once and store the session.
        self.session = self.authenticator.get_session()

        self.source_filename = self._ensure_json_extension(self.config.source_filename)

        self.logger.log_info("FileHandler initialized successfully.")

    def _ensure_json_extension(self, filename):
        """
        Ensure the source filename ends with `.json`.

        Args:
            filename (str): The source filename.

        Returns:
            str: Updated filename with `.json` extension if missing.
        """
        if not filename.endswith(".json"):
            updated_filename = f"{filename}.json"
            self.logger.log_debug(f"Updated source filename to include .json: {updated_filename}")
            return updated_filename
        return filename

    def _get_adf_api_url(self, path=None):
        """
        Construct an Azure DevOps API URL for fetching ADF JSON files.

        Args:
            path (str, optional): The specific file path for retrieval.

        Returns:
            str: The constructed API URL.
        """
        base_url = (
            f"https://dev.azure.com/{self.config.adf_details['Organization']}/"
            f"{self.config.adf_details['Project']}/_apis/git/repositories/"
            f"{self.config.adf_details['Repository']}/items"
        )

        if path:
            return (f"{base_url}?path={path}&versionDescriptor.version="
                    f"{self.config.adf_details['Branch']}&api-version=6.0&$format=octetStream")
        return (f"{base_url}?scopePath={self.config.adf_details['Folder Path']}&recursionLevel=Full&"
                f"versionDescriptor.version={self.config.adf_details['Branch']}&api-version=6.0")

    def get_adf_files(self):
        """
        Retrieve all ADF JSON files from the Azure DevOps repository.
        Returns:
            list: List of JSON files in the repository.
        """
        try:
            url = self._get_adf_api_url()
            self.logger.log_message(f"Fetching ADF files from {url}...")
            response = self.session.get(url)
            self.logger.log_debug(f"HTTP GET to {url} returned status code: {response.status_code}")

            if response.status_code == 200:
                adf_files = response.json().get("value", [])
                for file in adf_files:
                    self.logger.log_debug(f"File dict: {json.dumps(file, indent=4)}")
                    # Try alternative keys if they exist
                    file_size = file.get("contentLength", "N/A")
                    last_modified = file.get("lastModifiedDate", "N/A")
                    self.logger.log_debug(f"File Metadata - Path: {file.get('path')}, Size: {file_size}, Last Modified: {last_modified}")
                json_files = [file for file in adf_files if not file.get("isFolder") and file["path"].endswith(".json")]
                self.logger.log_debug(f"Total JSON files retrieved: {len(json_files)}")
                return json_files
            else:
                self.logger.log_error(f"Failed to list items in the repository. Status Code: {response.status_code}")
                return []
        except Exception as e:
            self.logger.log_error(f"Error while retrieving ADF files: {e}")
            raise

    def get_filtered_file_list(self):
        """
        Filter the ADF files based on the provided source filename pattern.

        Returns:
            list: Filtered list of JSON files.
        """
        try:
            json_files = self.get_adf_files()

            if self.source_filename in {"*", "*.json"}:
                self.logger.log_debug(f"Returning all {len(json_files)} JSON files (no filter applied).")
                return json_files

            filtered_files = [file for file in json_files if fnmatch.fnmatch(file["path"], f"*{self.source_filename}")]
            self.logger.log_debug(f"Filtered {len(filtered_files)} files based on pattern: {self.source_filename}")
            return filtered_files
        except Exception as e:
            self.logger.log_error(f"Error while filtering files: {e}")
            raise

    def get_adf_file_content(self, file_path):
        """
        Retrieve the content of a specific ADF JSON file from Azure DevOps.
        
        Args:
            file_path (str): The path of the file to retrieve.
        
        Returns:
            str: The raw content of the JSON file.
        """
        try:
            file_url = self._get_adf_api_url(file_path)
            self.logger.log_debug(f"Fetching content for: {file_path} using URL: {file_url}")
            
            # Attempt a HEAD request to get headers
            head_response = self.session.head(file_url)
            content_length = head_response.headers.get("Content-Length", "N/A")
            self.logger.log_debug(f"HEAD request returned Content-Length: {content_length}")
            
            # Measure the GET request time
            import time
            start_time = time.time()
            response = self.session.get(file_url)
            elapsed_time = time.time() - start_time
            
            if response.status_code == 200:
                content = response.text
                inferred_size = len(content)
                self.logger.log_debug(
                    f"Successfully retrieved content for: {file_path} "
                    f"(length: {inferred_size} characters, elapsed time: {elapsed_time:.2f} sec)"
                )
                snippet = content[:200] if inferred_size > 200 else content
                self.logger.log_debug(f"Content snippet: {snippet}")
                return content
            else:
                self.logger.log_error(f"Failed to retrieve file: {file_path}. Status Code: {response.status_code}")
                return None
        except Exception as e:
            self.logger.log_error(f"Error retrieving file content: {e}")
            raise

    @staticmethod
    def initialize(config, authenticator, debug=False, logger=None):
        """
        Factory method to initialize the FileHandler class.

        Args:
            config (Config): The configuration object with all required settings.
            authenticator (Authenticator): Instance of the Authenticator class for managing authentication.
            debug (bool): Enable debug-level logging if True.
            logger (Logger, optional): A custom logger instance.

        Returns:
            FileHandler: An initialized FileHandler instance.
        """
        temp_logger = logger if logger is not None else Logger(debug=debug)
        temp_logger.log_info("Initializing FileHandler...")
        try:
            return FileHandler(config=config, authenticator=authenticator, debug=debug, logger=logger)
        except Exception as e:
            temp_logger.log_error(f"Failed to initialize FileHandler: {e}")
            raise

    def test_file_handler(self):
        """
        Simple built-in test for verifying file retrieval and content reading.
        """
        try:
            print("🔹 Running FileHandler Test...")

            # Test file retrieval
            files = self.get_filtered_file_list()
            if files:
                print(f"✅ Retrieved {len(files)} files successfully.")
            else:
                print("❌ No files retrieved.")

            # Test reading a single file with additional debug information
            if files:
                file_path = files[0]["path"]
                file_content = self.get_adf_file_content(file_path)
                if file_content:
                    print(f"✅ Successfully read content from {file_path}.")
                    print(f"Content Length: {len(file_content)} characters")
                    print("✅ Content snippet has been logged at debug level.")
                else:
                    print(f"❌ Failed to read content from {file_path}.")

            print("✅ FileHandler Test Passed Successfully!")

        except Exception as e:
            print(f"❌ FileHandler Test Failed: {e}")
