import os
import datetime
import ast
from adf_json_processor.auth.auth_strategy import PATAuthStrategy

class Config:
    def __init__(self, dbutils, auth_strategy, debug=False):
        self.debug = debug
        adf_config_str = dbutils.widgets.get("ADFConfig")
        adf_config = ast.literal_eval(adf_config_str)
        self.organization, self.project, self.repository, self.branch, self.folder_path = adf_config

        self.destination_storage_account = dbutils.widgets.get("DestinationStorageAccount")
        self.datasetidentifier = dbutils.widgets.get("Datasetidentifier")

        self.auth_strategy = auth_strategy
        self.source_filename = dbutils.widgets.get("SourceFileName")

        self.adf_details = {
            "Organization": self.organization,
            "Project": self.project,
            "Repository": self.repository,
            "Branch": self.branch,
            "Folder Path": self.folder_path
        }

        # Generate log and output paths
        self.log_path = self.generate_log_path()
        self.output_path = self.generate_output_path()

        # Ensure directories exist
        self.ensure_directories_exist()

    def update_source_filename(self, source_filename):
        """Update the source filename if a new one is provided."""
        self.source_filename = source_filename

    def generate_log_path(self):
        """Generate log path based on the current date and time."""
        date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"/dbfs/mnt/{self.destination_storage_account}/{self.datasetidentifier}/log/error_log_{date_str}.json"

    def generate_output_path(self):
        """Generate output file path."""
        return f"/dbfs/mnt/{self.destination_storage_account}/{self.datasetidentifier}/combined_hierarchical_pipeline_structure_filtered.json"

    def ensure_directories_exist(self):
        """Ensure that the required directories exist."""
        log_dir = os.path.dirname(self.log_path)
        output_dir = os.path.dirname(self.output_path)
        dbutils.fs.mkdirs(log_dir)
        dbutils.fs.mkdirs(output_dir)

    def print_params(self):
        """Print configuration parameters in a well-organized format."""
        if not self.debug:
            return  # Exit if debug is not enabled

        print("\n=== ADF Configuration ===")
        for key, value in self.adf_details.items():
            print(f"{key}: {value}")

        print("\n=== Authentication ===")
        print(f"Authentication Method: {self.auth_strategy.__class__.__name__}")
        if isinstance(self.auth_strategy, PATAuthStrategy):
            print(f"Personal Access Token: {'*' * len(self.auth_strategy.pat)}")

        print("\n=== Storage Configuration ===")
        print(f"Destination Storage Account: {self.destination_storage_account}")
        print(f"Dataset Identifier: {self.datasetidentifier}")
        print(f"Source Filename: {self.source_filename}")

        print("\n=== Paths ===")
        print(f"Log Path: {self.log_path}")
        print(f"Output File Path: {self.output_path}")

def initialize_config(auth_strategy, dbutils, debug=False):
    """
    Initialize the configuration with the provided authentication strategy and optional debug flag.
    """
    config = Config(dbutils=dbutils, auth_strategy=auth_strategy, debug=debug)
    config.print_params()  # Print configuration parameters for verification

    return config