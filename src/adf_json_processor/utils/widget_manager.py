import logging
from adf_json_processor.utils.logger import Logger

class WidgetManager:
    """
    Manages the initialization and configuration of Databricks widgets.
    """

    def __init__(self, dbutils, logger: Logger = None):
        """
        Initializes the WidgetManager.

        Args:
            dbutils: Databricks utilities for managing widgets.
            logger (Logger, optional): Custom logger instance.
        """
        self.dbutils = dbutils
        self.logger = logger or Logger(debug=False)  # Default logger if none is provided

    def clear_widgets(self):
        """
        Clears all existing widgets to ensure a fresh start.
        """
        try:
            self.dbutils.widgets.removeAll()
            self.logger.log_info("All existing widgets removed successfully.")
        except Exception as e:
            self.logger.log_error(f"Error during widget cleanup: {str(e)}")
            raise RuntimeError(f"Failed to clear widgets: {str(e)}")

    def initialize_widgets(self):
        """
        Initializes and configures the required Databricks widgets.
        """
        self.logger.log_start("Widget Initialization")

        try:
            # Remove any pre-existing widgets
            self.clear_widgets()

            # Define default widget values
            default_values = {
                "ADFConfig": '["energinet", "DataPlatform_v3.0", "data-factory", "main", "pipeline"]',
                "PersonalAccessTokenKeyVaultName": "token-devops-PAT-data-quality",
                "SourceStorageAccount": "dplandingstoragetest",
                "DestinationStorageAccount": "dpuniformstoragetest",
                "Datasetidentifier": "data_quality__adf",
                "SourceFileName": "*"
            }

            # Create widgets dynamically from the dictionary
            for widget_name, default_value in default_values.items():
                self.dbutils.widgets.text(widget_name, default_value, widget_name.replace("_", " "))

            self.logger.log_info("Widgets initialized and configured successfully.")

        except Exception as e:
            self.logger.log_error(f"Widget initialization failed: {e}")
            raise

        self.logger.log_end("Widget Initialization")
