import logging

def initialize_widgets():
    """
    Clears all existing widgets and initializes the required widgets.
    If no logger is provided, a new Logger instance is created.
    """
    # Initialize a logger for package installation.
    logger = logging.getLogger("WidgetInstaller")
    logger.setLevel(logging.DEBUG)
    
    # Clear any existing handlers to avoid duplicate messages.
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create a console handler with a simple formatter.
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('[%(levelname)s] - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    logger.info("Starting widget installation...")

    # Clear all widgets.
    try:
        dbutils.widgets.removeAll()
        logger.info("All existing widgets removed successfully.")
    except Exception as e:
        logger.log_error(f"Error during widget cleanup: {str(e)}")
        raise RuntimeError(f"Failed to clear widgets: {str(e)}")

    # Widget initialization and configuration.
    dbutils.widgets.text("ADFConfig", '["energinet", "DataPlatform_v3.0", "data-factory", "main", "pipeline"]', "ADF Configuration")
    dbutils.widgets.text("PersonalAccessTokenKeyVaultName", "token-devops-PAT-data-quality", "Personal Access Token Key Vault Name")
    dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest", "Source Storage Account")
    dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest", "Destination Storage Account")
    dbutils.widgets.text("Datasetidentifier", "data_quality__adf", "Dataset Identifier")
    dbutils.widgets.text("SourceFileName", "*", "Source File Name")

    logger.info("Widgets initialized and configured successfully.")