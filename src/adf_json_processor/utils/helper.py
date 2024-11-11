import json
import uuid
import hashlib
import os
from pyspark.sql import DataFrame

class Helper:
    """
    Helper class for utility functions including DataFrame view creation, hash key generation, 
    JSON handling, and file operations.
    """

    def __init__(self, debug=False):
        """
        Initializes the Helper with optional debug mode.
        
        Args:
            debug (bool): Enable debug-level output if True.
        """
        self.debug = debug

    def create_temp_views(self, dataframes: dict):
        """
        Create temporary views for each DataFrame in the given dictionary.

        Args:
            dataframes (dict): Dictionary of DataFrames to create views from.
        """
        for df_name, df in dataframes.items():
            if isinstance(df, DataFrame):  # Ensure the value is a DataFrame
                view_name = f"view_{df_name}"
                df.createOrReplaceTempView(view_name)
                if self.debug:
                    print(f"{view_name} created successfully")
            else:
                print(f"Warning: {df_name} is not a DataFrame. Skipping view creation.")

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