import json
import uuid
import hashlib
from adf_json_processor.utils.logger import Logger

class Helper:
    """
    Helper class for common operations, including creating temporary views,
    generating hash keys, saving JSON to files, parsing JSON, and other utility functions.
    """

    def __init__(self, logger: Logger = None):
        """
        Initialize the Helper with a logger instance.

        Args:
            logger (Logger, optional): Custom logger instance. If None, a default logger is created.
        """
        self.logger = logger if logger is not None else Logger(debug=False)
        self.logger.log_info("Helper initialized successfully.")

    @staticmethod
    def initialize(logger: Logger = None, debug=False):
        """
        Factory method to initialize the Helper class.

        Args:
            logger (Logger, optional): Logger instance for structured logging.
            debug (bool): Enable debug-level logging.

        Returns:
            Helper: An initialized Helper instance.
        """
        logger = logger if logger is not None else Logger(debug=debug)
        logger.log_info("Initializing Helper...")
        return Helper(logger=logger)

    def create_temp_views(self, dataframes: dict, debug: bool = False):
        """
        Create temporary views for each DataFrame in the given dictionary.

        Args:
            dataframes (dict): Dictionary containing DataFrames.
            debug (bool): Enable debug-level logging.
        """
        self.logger.log_start("create_temp_views")
        try:
            for df_name, df in dataframes.items():
                view_name = f"view_{df_name}"
                df.createOrReplaceTempView(view_name)
                self.logger.log_message(
                    f"Temporary view created: {view_name}",
                    level="info" if not debug else "debug"
                )
        except Exception as e:
            self.logger.log_error(f"Error creating temporary views: {e}")
            raise
        self.logger.log_end("create_temp_views")

    def generate_hash_key(self, *args):
        """
        Generate a unique hash-based ID using provided values.

        Args:
            *args: Variable length argument list to generate a hash from.

        Returns:
            str: A unique hash key in hexadecimal.
        """
        try:
            combined_string = '|'.join(str(arg) for arg in args if arg)
            hash_object = hashlib.sha256(combined_string.encode())
            hash_key = hash_object.hexdigest()
            self.logger.log_message(
                f"Generated hash key: {hash_key[:8]}... (truncated)",
                level="debug"
            )
            return hash_key
        except Exception as e:
            self.logger.log_error(f"Error generating hash key: {e}")
            raise

    def generate_unique_id(self):
        """
        Generate a unique UUID.

        Returns:
            str: A unique UUID string.
        """
        try:
            unique_id = str(uuid.uuid4())
            self.logger.log_message(
                f"Generated unique UUID: {unique_id}",
                level="debug"
            )
            return unique_id
        except Exception as e:
            self.logger.log_error(f"Error generating unique UUID: {e}")
            raise

    def extract_last_part(self, path):
        """
        Extract the last part of a file path.

        Args:
            path (str): A string representing the full path.

        Returns:
            str: The last part of the path after the last slash (/).
        """
        try:
            last_part = path.split('/')[-1] if path else None
            self.logger.log_message(
                f"Extracted last part of path: {last_part}",
                level="debug"
            )
            return last_part
        except Exception as e:
            self.logger.log_error(f"Error extracting last part of path: {e}")
            raise

    def parse_json(self, json_string: str):
        """
        Parse a JSON string into a Python dictionary.

        Args:
            json_string (str): The JSON string to parse.

        Returns:
            dict: The parsed JSON data as a Python dictionary.
        """
        try:
            result = json.loads(json_string)
            self.logger.log_message(
                f"Successfully parsed JSON with keys: {list(result.keys())}",
                level="debug"
            )
            return result
        except Exception as e:
            self.logger.log_error(f"Error parsing JSON: {e}")
            raise

    def print_json_structure(self, json_data, title="Flattened JSON Structure", debug=False):
        """
        Print the JSON structure in a human-readable format.

        Args:
            json_data (dict): JSON data to print.
            title (str): Title of the JSON structure.
            debug (bool): Enable debug-level logging.
        """
        try:
            formatted_json = json.dumps(json_data, indent=4)
            self.logger.log_block(
                title,
                [formatted_json] if debug else None,
                level="debug" if debug else "info"
            )
        except Exception as e:
            self.logger.log_error(f"Error printing JSON structure: {e}")

    def save_json_to_file(self, json_data, output_path, debug=False):
        """
        Save the provided JSON data to the specified output file path.

        Args:
            json_data (dict): The JSON data to be saved.
            output_path (str): The file path where the JSON should be saved.
            debug (bool): Enable debug-level logging.
        """
        self.logger.log_start("save_json_to_file")
        try:
            with open(output_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=4)
            self.logger.log_message(
                f"JSON successfully saved to {output_path}",
                level="info" if not debug else "debug"
            )
        except Exception as e:
            self.logger.log_error(f"Error saving JSON to {output_path}: {e}")
            raise
        self.logger.log_end("save_json_to_file")

    def test_helper(self):
        """
        Run a self-contained test to verify that Helper functions are working correctly.
        This test uses a sample JSON file defined by 'sample_path' and writes output to a temporary file.
        """
        try:
            print("🔹 Running Helper Test...")

            # Test generate_hash_key
            hash_key = self.generate_hash_key("Test", "Value")
            print(f"✅ generate_hash_key returned: {hash_key}")

            # Test generate_unique_id
            unique_id = self.generate_unique_id()
            print(f"✅ generate_unique_id returned: {unique_id}")

            # Test extract_last_part with a sample path
            sample_path = "/dbfs/mnt/dplandingstoragetest/data_quality__adf/combined_hierarchical_pipeline_structure_filtered.json"
            last_part = self.extract_last_part(sample_path)
            print(f"✅ extract_last_part returned: {last_part}")

            # Read JSON content from the sample file instead of using a hardcoded string.
            with open(sample_path, 'r') as f:
                sample_json_str = f.read()
            parsed_json = self.parse_json(sample_json_str)
            print(f"✅ parse_json returned: {parsed_json}")

            # Test print_json_structure (this logs a block with formatted JSON)
            self.print_json_structure(parsed_json, title="Test JSON Structure", debug=True)
            print("✅ print_json_structure executed (see debug log for details)")

            # Test save_json_to_file using a temporary file path.
            temp_output_path = "/dbfs/tmp/helper_test_output.json"
            self.save_json_to_file(parsed_json, temp_output_path, debug=True)
            print(f"✅ JSON successfully saved to {temp_output_path}")

            print("✅ Helper Test Passed Successfully!")
        except Exception as e:
            print(f"❌ Helper Test Failed: {e}")