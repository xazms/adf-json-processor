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

    def create_temp_views(self, dataframes: dict, preview_rows: int = 5):
        """
        Create temporary views for each DataFrame in the given dictionary and include a preview in the log.

        Args:
            dataframes (dict): Dictionary of DataFrames to create views from.
            preview_rows (int): Number of rows to display in the preview of each DataFrame.
        """
        created_views = []
        previews = []

        # Create views and store previews in a list
        for df_name, df in dataframes.items():
            if isinstance(df, DataFrame):  # Ensure the value is a DataFrame
                view_name = f"view_{df_name}"
                df.createOrReplaceTempView(view_name)
                created_views.append(f"{view_name} created successfully")

                # Store SQL query for preview display at the end
                previews.append((view_name, f"SELECT * FROM {view_name} LIMIT {preview_rows}"))
            else:
                created_views.append(f"Warning: {df_name} is not a DataFrame. Skipping view creation.")

        # Log the block with the created views if debug is enabled
        if self.debug and self.logger:
            self.logger.log_block("Temporary Views Creation Summary", created_views)
        elif self.debug:
            print("Temporary Views Creation Summary:")
            for view in created_views:
                print(view)

        # Display previews for each view at the end
        for view_name, query in previews:
            print(f"\n=== Preview of {view_name} ===")
            self.spark.sql(query).show(truncate=False)

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

    def save_json_to_file(self, json_data, output_path):
        """
        Save the provided JSON data to a specified output path. If the output path is a directory,
        a default filename is appended.

        Args:
            json_data (dict): JSON data to save.
            output_path (str): Path to the file or directory where JSON should be saved.
        """
        # Append default filename if output_path is a directory
        if os.path.isdir(output_path):
            output_path = os.path.join(output_path, "combined_structure.json")

        try:
            with open(output_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=4)
            if self.debug:
                print(f"JSON successfully saved to {output_path}")
        except Exception as e:
            print(f"Error saving JSON to {output_path}: {e}")