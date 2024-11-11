import json
from typing import Tuple, Dict
from pyspark.sql import SparkSession, DataFrame
from utils.helper import print_json_structure, save_json_to_file  # Ensure helper functions are imported
from processing.conversion import build_hierarchical_structure_with_counts, convert_to_dataframe

class FileProcessor:
    """
    Processes JSON files by retrieving, filtering, and structuring data from Azure Data Factory (ADF) pipelines.
    Converts structured data to DataFrames and logs summaries for each pipeline.
    """

    def __init__(self, file_handler, spark: SparkSession, config, logger):
        """
        Initializes the FileProcessor with the necessary components.

        Args:
            file_handler: Instance responsible for handling file operations.
            spark (SparkSession): Spark session for DataFrame creation.
            config: Configuration manager instance.
            logger (Logger): Logger instance for structured logging.
        """
        self.file_handler = file_handler
        self.spark = spark
        self.config = config
        self.logger = logger

    def log_pipeline_summary(self, combined_structure):
        """
        Logs a summary for each pipeline, including the count of activities and dependencies.

        Args:
            combined_structure (dict): Hierarchical structure containing pipelines, activities, and dependencies.
        """
        for pipeline in combined_structure['pipelines']:
            pipeline_id = pipeline.get('PipelineId')
            pipeline_name = pipeline.get('PipelineName', 'Unknown')

            # Count activities associated with this pipeline
            activities_count = sum(1 for activity in combined_structure['activities'] if activity['ParentId'] == pipeline_id)

            # Count dependencies within this pipeline
            pipeline_activity_ids = {activity['ActivityId'] for activity in combined_structure['activities'] if activity['ParentId'] == pipeline_id}
            dependencies_count = sum(
                1 for dependency in combined_structure['dependencies']
                if dependency['DependencySourceId'] in pipeline_activity_ids and dependency['DependencyTargetId'] in pipeline_activity_ids
            )

            # Log pipeline summary
            self.logger.log_info(f"Pipeline: {pipeline_name}, Activities Count: {activities_count}, Dependencies: {dependencies_count}")

    def process_json_files(
        self, include_types=None, include_empty=False, include_json=False, debug=False, save_to_file=False, output_path="combined_structure.json"
    ) -> Tuple[Dict[str, DataFrame], DataFrame, DataFrame, DataFrame]:
        """
        Process JSON files, extract hierarchical structure, and convert it to DataFrames.
        """
        combined_structure = {"pipelines": [], "activities": [], "dependencies": []}
        filtered_files = self.file_handler.get_filtered_file_list(show_all=False, top_n=5)

        for file in filtered_files:
            try:
                file_content = self.file_handler.get_adf_file_content(file['path'])
                adf_data = json.loads(file_content)
                hierarchical_structure, counts = build_hierarchical_structure_with_counts(adf_data, include_types, include_empty, debug)

                if hierarchical_structure:
                    combined_structure["pipelines"].extend(hierarchical_structure.get("pipelines", []))
                    combined_structure["activities"].extend(hierarchical_structure.get("activities", []))
                    combined_structure["dependencies"].extend(hierarchical_structure.get("dependencies", []))

            except json.JSONDecodeError as e:
                self.logger.log_error(f"JSONDecodeError in file {file['path']}: {e}")
            except Exception as e:
                self.logger.log_error(f"Unexpected error in file {file['path']}: {e}")

        if include_json:
            print_json_structure(combined_structure, debug)

        if save_to_file:
            save_json_to_file(combined_structure, output_path, debug)

        # Log pipeline summary
        #log_pipeline_summary(combined_structure, self.logger)

        # Convert the combined structure to DataFrames
        dataframes, pipelines_df, activities_df, dependencies_df = convert_to_dataframe(self.spark, combined_structure, debug)

        # Return all required values
        return dataframes, pipelines_df, activities_df, dependencies_df