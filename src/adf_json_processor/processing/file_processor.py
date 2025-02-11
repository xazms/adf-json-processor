import json
import os
from pyspark.sql import DataFrame, SparkSession
from typing import Tuple, Dict
from adf_json_processor.utils.helper import Helper
from adf_json_processor.processing.conversion import ADFDataConverter

class FileProcessor:
    def __init__(self, file_handler, spark, config, logger, dbutils, debug=False):
        """
        Initialize the FileProcessor with required dependencies.

        Args:
            file_handler: Instance to handle file operations.
            spark (SparkSession): Spark session for DataFrame operations.
            config: Configuration manager instance.
            logger: Logger instance for logging.
            dbutils: Databricks utility object for accessing file system operations.
            debug (bool): Enable debug mode if True.
        """
        self.file_handler = file_handler
        self.spark = spark
        self.config = config
        self.logger = logger
        self.dbutils = dbutils  # Pass dbutils for file handling
        self.debug = debug
        self.helper = Helper(spark=spark, logger=logger, debug=debug)
        self.converter = ADFDataConverter(spark=spark, debug=debug)

    def log_pipeline_summary(self, pipeline_name, activities_count, dependencies_count, file_path):
        """
        Logs a summary of the current pipeline's processing results.

        Args:
            pipeline_name (str): Name of the pipeline.
            activities_count (int): Number of activities in the pipeline.
            dependencies_count (int): Number of dependencies within the pipeline.
            file_path (str): The file path of the processed pipeline.
        """
        self.logger.log_info(f"File: {file_path}, Pipeline: {pipeline_name}, "
                             f"Activities Count: {activities_count}, Dependencies: {dependencies_count}")
        
    def save_json_to_file(self, json_data, output_path, debug=None, top_n=None):
        """
        Save the provided JSON data to the specified output path on DBFS and confirm the saved content.

        Args:
            json_data (dict): JSON data to save.
            output_path (str): Path to the file or directory where JSON should be saved.
            debug (bool): Enable debug mode if True.
            top_n (int): Number of items to show for each array in the preview. Shows all items if None.
        """
        effective_debug = debug if debug is not None else self.debug

        # Ensure output_path is valid and includes a filename
        if not output_path:
            self.logger.log_block("File Save Error", ["Error: output_path is empty or not provided."], skip_prefix_for_blank=True)
            return

        if output_path.endswith("/"):
            output_path = os.path.join(output_path, "combined_hierarchical_pipeline_structure_filtered.json")

        # Convert JSON data to a string format
        json_string = json.dumps(json_data, indent=4)

        # Use dbutils to save the JSON data directly to DBFS
        try:
            self.dbutils.fs.put(output_path, json_string, overwrite=True)
            
            # Limit preview to top_n items if specified, otherwise show all items
            preview_data = {
                "pipelines": json_data.get("pipelines", [])[:top_n] if top_n is not None else json_data.get("pipelines", []),
                "activities": json_data.get("activities", [])[:top_n] if top_n is not None else json_data.get("activities", []),
                "dependencies": json_data.get("dependencies", [])[:top_n] if top_n is not None else json_data.get("dependencies", [])
            }
            preview_string = json.dumps(preview_data, indent=4)

            if effective_debug:
                self.logger.log_block(
                    "File Save Confirmation", [
                        f"File successfully saved to: {output_path}",
                        "",  # Empty line without prefix
                        f"=== Preview of Saved Content (Top {top_n} Items) ===" if top_n is not None else "=== Full Content of Saved File ===",
                        preview_string
                    ],
                    skip_prefix_for_blank=True
                )

        except Exception as e:
            self.logger.log_block("File Save Error", [f"Error saving JSON to {output_path}: {e}"], skip_prefix_for_blank=True)

    def process_json_files(
        self, 
        include_types=None, 
        include_empty=False, 
        include_json=False, 
        debug=None, 
        save_to_file=False, 
        output_path=None,
        process_all=True,
        top_n=None
    ) -> Tuple[Dict[str, DataFrame], DataFrame, DataFrame, DataFrame]:
        """
        Process JSON files, extract hierarchical structure, and convert it to DataFrames.

        Args:
            include_types (list): List of activity types to include in processing.
            include_empty (bool): Include pipelines with no activities if True.
            include_json (bool): Print the JSON structure if True.
            debug (bool): Enable debug mode if True.
            save_to_file (bool): Save combined structure to file if True.
            output_path (str): Output path for saving JSON structure.
            process_all (bool): Process all files if True, otherwise limit to `top_n`.
            top_n (int): Number of files to display in log output if `process_all` is True.

        Returns:
            Tuple containing:
            - dataframes (dict): Dictionary of DataFrames for pipelines, activities, and dependencies.
            - pipelines_df (DataFrame): DataFrame for pipelines.
            - activities_df (DataFrame): DataFrame for activities.
            - dependencies_df (DataFrame): DataFrame for dependencies.
        """
        effective_debug = debug if debug is not None else self.debug
        if effective_debug:
            print("Debugging is enabled in process_json_files.")
        
        combined_structure = {"pipelines": [], "activities": [], "dependencies": []}

        # Set default top_n to 10 if top_n is None
        top_n = top_n if top_n is not None else 10

        # Fetch all files to process
        all_files = self.file_handler.get_filtered_file_list(show_all=True)

        # Set files_to_log for logging purposes and files_to_process for processing purposes
        files_to_log = all_files[:top_n] if process_all else all_files[:top_n]
        files_to_process = all_files if process_all else all_files[:top_n]

        # Log the processing mode with an information message
        processing_message = (
            f"Processing all {len(all_files)} files, displaying top {top_n} in log." if process_all
            else f"Processing only {top_n} files out of {len(all_files)} based on provided parameters."
        )
        self.logger.log_block("File Processing", [processing_message])

        # Process each file in files_to_process
        for idx, file in enumerate(files_to_process):
            try:
                # Process the file content and convert to hierarchical structure
                file_content = self.file_handler.get_adf_file_content(file['path'])
                adf_data = json.loads(file_content)
                hierarchical_structure, counts = self.converter.build_hierarchical_structure_with_counts(
                    adf_data, include_types, include_empty
                )

                # Extend the combined structure if hierarchical data is found
                if hierarchical_structure:
                    combined_structure["pipelines"].extend(hierarchical_structure.get("pipelines", []))
                    combined_structure["activities"].extend(hierarchical_structure.get("activities", []))
                    combined_structure["dependencies"].extend(hierarchical_structure.get("dependencies", []))

                    # Log details for this file's pipeline if needed
                    pipeline_name = hierarchical_structure['pipelines'][0].get('PipelineName', 'Unknown') if hierarchical_structure['pipelines'] else 'Unknown'
                    activities_count = len(hierarchical_structure.get("activities", []))
                    dependencies_count = len(hierarchical_structure.get("dependencies", []))

                    if idx < top_n:
                        self.logger.log_info(f"File: {file['path']}, Pipeline: {pipeline_name}, Activities Count: {activities_count}, Dependencies: {dependencies_count}")

            except json.JSONDecodeError as e:
                self.logger.log_error(f"JSONDecodeError in file {file['path']}: {e}")
            except Exception as e:
                self.logger.log_error(f"Unexpected error in file {file['path']}: {e}")

        # Print JSON structure if requested
        if include_json:
            self.helper.print_json_structure(combined_structure)

        # Save JSON structure to a file if requested
        if save_to_file:
            output_path = output_path or "combined_structure.json"
            self.save_json_to_file(combined_structure, output_path, debug=effective_debug, top_n=top_n)

        # Convert the combined structure to DataFrames
        dataframes, pipelines_df, activities_df, dependencies_df = self.converter.convert_to_dataframe(self.spark, combined_structure)

        return dataframes, pipelines_df, activities_df, dependencies_df