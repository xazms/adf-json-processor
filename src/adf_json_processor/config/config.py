import os
import datetime
import ast

class Config:
    """
    Handles configuration and paths for processing ADF data.
    """
    def __init__(self, auth_strategy, debug=False):
        self.debug = debug

        if 'dbutils' not in globals() or dbutils is None:
            raise ValueError("dbutils is required to retrieve secrets or widgets in this environment.")

        # Parse ADF configuration
        adf_config_str = dbutils.widgets.get("ADFConfig")
        adf_config = ast.literal_eval(adf_config_str)
        self.organization, self.project, self.repository, self.branch, self.folder_path = adf_config

        self.source_storage_account = dbutils.widgets.get("SourceStorageAccount")
        self.destination_storage_account = dbutils.widgets.get("DestinationStorageAccount")
        self.datasetidentifier = dbutils.widgets.get("Datasetidentifier")
        self.source_filename = dbutils.widgets.get("SourceFileName")

        self.auth_strategy = auth_strategy

        self.adf_details = {
            "Organization": self.organization,
            "Project": self.project,
            "Repository": self.repository,
            "Branch": self.branch,
            "Folder Path": self.folder_path
        }

        self.log_path = self.generate_log_path()
        self.output_path = self.generate_output_path()

    def update_source_filename(self, source_filename):
        """
        Ensure the source filename ends with .json.
        """
        if not source_filename.endswith(".json"):
            source_filename += ".json"
        self.source_filename = source_filename

    def generate_log_path(self):
        """
        Generate log path based on the current timestamp.
        """
        date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"/dbfs/mnt/{self.source_storage_account}/{self.datasetidentifier}/log/error_log_{date_str}.json"

    def generate_output_path(self):
        """
        Generate output path for the processed file.
        """
        return f"/dbfs/mnt/{self.source_storage_account}/{self.datasetidentifier}/combined_hierarchical_pipeline_structure_filtered.json"

    def print_params(self):
        """
        Print configuration parameters if debug is enabled.
        """
        if not self.debug:
            return

        print("\n=== ADF Configuration ===")
        for key, value in self.adf_details.items():
            print(f"{key}: {value}")

        print("\n=== Storage Configuration ===")
        print(f"Source Storage Account: {self.source_storage_account}")
        print(f"Destination Storage Account: {self.destination_storage_account}")
        print(f"Dataset Identifier: {self.datasetidentifier}")
        print(f"Source Filename: {self.source_filename}")

        print("\n=== Authentication ===")
        print(f"Authentication Method: {self.auth_strategy.__class__.__name__}")
        if isinstance(self.auth_strategy, PATAuthStrategy):
            print(f"Personal Access Token: {'*' * len(self.auth_strategy.pat)}")

    def ensure_directories_exist(self):
        """
        Ensure the required directories exist in the file system.
        """
        log_dir = os.path.dirname(self.log_path)
        output_dir = os.path.dirname(self.output_path)
        
        if 'dbutils' in globals():
            try:
                dbutils.fs.mkdirs(log_dir)
                dbutils.fs.mkdirs(output_dir)
            except Exception as e:
                print(f"An error occurred while creating directories in DBFS: {e}")
        else:
            try:
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
            except Exception as e:
                print(f"An error occurred while creating directories locally: {e}")