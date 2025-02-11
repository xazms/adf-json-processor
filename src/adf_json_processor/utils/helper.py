import json
import uuid
import hashlib
import os
from pyspark.sql import DataFrame
from adf_json_processor.utils.logger import Logger

class Helper:
    """
    Helper class for utility functions including DataFrame view creation, hash key generation, 
    JSON handling, and file operations.
    """

    def __init__(self, spark, logger=None, debug=False):
        """
        Initializes the Helper with Spark session, optional debug mode, and a logger.

        Args:
            spark (SparkSession): Spark session for executing SQL queries.
            logger (Logger): Logger instance for structured logging.
            debug (bool): Enable debug-level output if True.
        """
        self.spark = spark
        self.logger = logger
        self.debug = debug

    def create_temp_views(self, dataframes: dict = None, pipelines_df=None, activities_df=None, dependencies_df=None, preview_rows: int = 5):
        """
        Create temporary views for each DataFrame in the given dictionary or directly passed DataFrames
        and display a preview.

        Args:
            dataframes (dict): Dictionary of DataFrames to create views from.
            pipelines_df (DataFrame): Optional individual DataFrame for pipelines.
            activities_df (DataFrame): Optional individual DataFrame for activities.
            dependencies_df (DataFrame): Optional individual DataFrame for dependencies.
            preview_rows (int): Number of rows to display in the preview of each DataFrame.
        """
        # Initialize the dataframes dictionary if not provided
        if dataframes is None:
            dataframes = {}

        # Add individual DataFrames to the dictionary if provided
        if pipelines_df is not None:
            dataframes["pipelines_df"] = pipelines_df
        if activities_df is not None:
            dataframes["activities_df"] = activities_df
        if dependencies_df is not None:
            dataframes["dependencies_df"] = dependencies_df

        created_views = []
        previews = []

        # Create views and store previews in a list
        for df_name, df in dataframes.items():
            if isinstance(df, DataFrame):  # Ensure the value is a DataFrame
                view_name = f"view_{df_name}"
                df.createOrReplaceTempView(view_name)
                created_views.append(f"{view_name} created successfully")

                # Collect the preview data
                previews.append((view_name, df.limit(preview_rows)))
            else:
                created_views.append(f"Warning: {df_name} is not a DataFrame. Skipping view creation.")

        # Log the block with the created views if debug is enabled
        if self.debug and self.logger:
            self.logger.log_block("Temporary Views Creation Summary", created_views)
        elif self.debug:
            print("Temporary Views Creation Summary:")
            for view in created_views:
                print(view)

        # Display previews for each view with actual data rows
        for view_name, preview_df in previews:
            print(f"\n=== Preview of {view_name} ===")
            if preview_df.count() > 0:
                try:
                    # Try to use display if available (Databricks)
                    if "display" in globals():
                        display(preview_df)
                    else:
                        preview_df.show(truncate=False)
                except Exception as e:
                    print(f"Failed to display preview for {view_name}: {e}")
            else:
                print(f"No data available in {view_name}.")

    def _generate_hash_key(self, *args):
        """
        Generate a unique hash key using SHA-256 based on provided arguments.

        Args:
            *args: Variable length argument list to include in the hash.

        Returns:
            str: A hexadecimal hash string.
        """
        combined_string = '|'.join(str(arg) for arg in args if arg)
        return hashlib.sha256(combined_string.encode()).hexdigest()

    def _generate_unique_id(self):
        """
        Generate a unique UUID (Universally Unique Identifier).

        Returns:
            str: A UUID string.
        """
        return str(uuid.uuid4())

    def _extract_last_part(self, path):
        """
        Extract the last part of a file path.

        Args:
            path (str): File path as a string.

        Returns:
            str: The last part of the path (e.g., filename).
        """
        return os.path.basename(path) if path else None

    def print_json_structure(self, json_data, title="Flattened JSON Structure"):
        """
        Print the JSON structure in a human-readable format.

        Args:
            json_data (dict): JSON data to print.
            title (str): Title to display above the JSON structure.
        """
        try:
            formatted_json = json.dumps(json_data, indent=4)
            if self.debug:
                print(f"\n=== {title} ===\n")
            print("\n" + formatted_json)
        except Exception as e:
            print(f"Error printing JSON structure: {e}")