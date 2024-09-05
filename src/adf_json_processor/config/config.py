import os
import datetime
import ast
from adf_json_processor.auth.auth_strategy import PATAuthStrategy

class Config:
    def __init__(self, auth_strategy, debug=False):
        """
        Initialize the configuration with the provided parameters.
        
        Args:
            auth_strategy (object): Authentication strategy (PAT or OAuth).
            debug (bool): Flag for enabling debug mode.
        """
        self.debug = debug

        # Ensure dbutils is available globally
        if 'dbutils' not in globals() or dbutils is None:
            raise ValueError("dbutils is required to retrieve secrets or widgets in this environment.")

        # Parse the ADF configuration (organization, project, repository, branch, folder path)
        adf_config_str = dbutils.widgets.get("ADFConfig")
        adf_config = ast.literal_eval(adf_config_str)
        self.organization, self.project, self.repository, self.branch, self.folder_path = adf_config

        # Set storage accounts for source and destination
        self.source_storage_account = dbutils.widgets.get("SourceStorageAccount")
        self.destination_storage_account = dbutils.widgets.get("DestinationStorageAccount")

        # Set dataset identifier and source filename
        self.datasetidentifier = dbutils.widgets.get("Datasetidentifier")
        self.source_filename = dbutils.widgets.get("SourceFileName")

        # Set catalog table names for nodes and links
        self.catalog_table_name_nodes = dbutils.widgets.get("CatalogTableNameNodes")
        self.catalog_table_name_links = dbutils.widgets.get("CatalogTableNameLinks")

        # Authentication strategy (e.g., PAT)
        self.auth_strategy = auth_strategy

        # ADF details used for debugging and logging
        self.adf_details = {
            "Organization": self.organization,
            "Project": self.project,
            "Repository": self.repository,
            "Branch": self.branch,
            "Folder Path": self.folder_path
        }

        # Generate log and output paths based on storage account and dataset
        self.log_path = self.generate_log_path()
        self.output_path = self.generate_output_path()

        # Ensure required directories exist
        # self.ensure_directories_exist()

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
        
        if 'dbutils' in globals():
            try:
                # Create directories in DBFS using dbutils.fs.mkdirs
                dbutils.fs.mkdirs(log_dir)
                dbutils.fs.mkdirs(output_dir)
            except Exception as e:
                print(f"An error occurred while creating directories in DBFS: {e}")
        else:
            # Create directories locally using os.makedirs
            try:
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
            except Exception as e:
                print(f"An error occurred while creating directories locally: {e}")

    def print_params(self):
        """Print configuration parameters in a well-organized format."""
        if not self.debug:
            return  # Exit if debug is not enabled

        # Print ADF configuration details
        print("\n=== ADF Configuration ===")
        for key, value in self.adf_details.items():
            print(f"{key}: {value}")

        # Print authentication details
        print("\n=== Authentication ===")
        print(f"Authentication Method: {self.auth_strategy.__class__.__name__}")
        if isinstance(self.auth_strategy, PATAuthStrategy):
            print(f"Personal Access Token: {'*' * len(self.auth_strategy.pat)}")

        # Print storage and dataset configuration
        print("\n=== Storage Configuration ===")
        print(f"Source Storage Account: {self.source_storage_account}")
        print(f"Destination Storage Account: {self.destination_storage_account}")
        print(f"Dataset Identifier: {self.datasetidentifier}")
        print(f"Source Filename: {self.source_filename}")

        # Print catalog table names
        print("\n=== Unity Catalog Configuration ===")
        print(f"Catalog Table Name - Nodes: {self.catalog_table_name_nodes}")
        print(f"Catalog Table Name - Links: {self.catalog_table_name_links}")

        # Print log and output paths
        print("\n=== Paths ===")
        print(f"Log Path: {self.log_path}")
        print(f"Output File Path: {self.output_path}")

def initialize_config(auth_strategy, debug=False):
    """
    Initialize the configuration with the provided authentication strategy and optional debug flag.
    
    Args:
        auth_strategy (object): The authentication strategy (PAT or OAuth).
        debug (bool): Enable debug printing if True.
        
    Returns:
        Config: A configuration object with all the initialized settings.
    """
    config = Config(auth_strategy=auth_strategy, debug=debug)
    config.print_params()  # Print configuration parameters for verification
    return config