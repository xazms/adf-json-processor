import json
from pyspark.sql import DataFrame, SparkSession
from typing import Tuple, Dict
from adf_json_processor.utils.helper import Helper
from adf_json_processor.processing.conversion import ADFDataConverter

class FileProcessor:
    """
    A class to process JSON files, convert them to structured DataFrames, and log summaries.
    """

    def __init__(self, file_handler, spark: SparkSession, config, logger, debug=False):
        """
        Initialize the FileProcessor with required dependencies.

        Args:
            file_handler: Instance to handle file operations.
            spark (SparkSession): Spark session for DataFrame operations.
            config: Configuration manager instance.
            logger: Logger instance for logging.
            debug (bool): Enable debug mode if True.
        """
        self.file_handler = file_handler
        self.spark = spark
        self.config = config
        self.logger = logger
        self.debug = debug
        self.helper = Helper(debug=debug)
        self.converter = ADFDataConverter(debug=debug)

    def log_pipeline_summary(self, combined_structure):
        """
        Logs a summary of the pipeline structure.
        
        Args:
            combined_structure (dict): Structure containing pipeline, activities, and dependencies data.
        """
        for pipeline in combined_structure['pipelines']:
            pipeline_id = pipeline.get('PipelineId')
            pipeline_name = pipeline.get('PipelineName', 'Unknown')

            # Count activities associated with this pipeline
            activities_count = sum(1 for activity in combined_structure['activities'] if activity['ParentId'] == pipeline_id)

            # Count dependencies where both source and target activities belong to this pipeline
            pipeline_activity_ids = {activity['ActivityId'] for activity in combined_structure['activities'] if activity['ParentId'] == pipeline_id}
            dependencies_count = sum(
                1 for dependency in combined_structure['dependencies']
                if dependency['DependencySourceId'] in pipeline_activity_ids and dependency['DependencyTargetId'] in pipeline_activity_ids
            )

            # Log pipeline summary
            self.logger.log_info(f"Pipeline: {pipeline_name}, Activities Count: {activities_count}, Dependencies: {dependencies_count}")

    def process_json_files(
        self, 
        include_types=None, 
        include_empty=False, 
        include_json=False, 
        debug=False, 
        save_to_file=False, 
        output_path=None,
        show_all=False,
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
            show_all (bool): Show all files in log output if True. If False, limits the log output.
            top_n (int, optional): Number of files to show in log output if `show_all` is False.

        Returns:
            Tuple containing:
            - dataframes (dict): Dictionary of DataFrames for pipelines, activities, and dependencies.
            - pipelines_df (DataFrame): DataFrame for pipelines.
            - activities_df (DataFrame): DataFrame for activities.
            - dependencies_df (DataFrame): DataFrame for dependencies.
        """
        combined_structure = {"pipelines": [], "activities": [], "dependencies": []}

        # Set default top_n to 10 if top_n is not provided and show_all is False
        top_n = top_n if top_n is not None else 10
        filtered_files = self.file_handler.get_filtered_file_list(show_all=show_all, top_n=top_n)

        # Add a block to group file processing logs
        self.logger.log_block("File Processing", [])

        for file in filtered_files:
            try:
                file_content = self.file_handler.get_adf_file_content(file['path'])
                adf_data = json.loads(file_content)
                hierarchical_structure, counts = self.converter.build_hierarchical_structure_with_counts(adf_data, include_types, include_empty)

                if hierarchical_structure:
                    combined_structure["pipelines"].extend(hierarchical_structure.get("pipelines", []))
                    combined_structure["activities"].extend(hierarchical_structure.get("activities", []))
                    combined_structure["dependencies"].extend(hierarchical_structure.get("dependencies", []))

                    # Log pipeline summary for the current file
                    pipeline_name = hierarchical_structure['pipelines'][0].get('PipelineName', 'Unknown') if hierarchical_structure['pipelines'] else 'Unknown'
                    activities_count = len(hierarchical_structure.get("activities", []))
                    dependencies_count = len(hierarchical_structure.get("dependencies", []))

                    # Log a single line of info for this file's pipeline
                    self.logger.log_info(f"File: {file['path']}, Pipeline: {pipeline_name}, Activities Count: {activities_count}, Dependencies: {dependencies_count}")

            except json.JSONDecodeError as e:
                self.logger.log_error(f"JSONDecodeError in file {file['path']}: {e}")
            except Exception as e:
                self.logger.log_error(f"Unexpected error in file {file['path']}: {e}")

        # Print JSON structure if requested
        if include_json:
            self.helper.print_json_structure(combined_structure, debug=debug)

        # Save JSON structure to a file if requested
        if save_to_file:
            output_path = output_path or "combined_structure.json"
            self.helper.save_json_to_file(combined_structure, output_path)

        # Convert the combined structure to DataFrames
        dataframes, pipelines_df, activities_df, dependencies_df = self.converter.convert_to_dataframe(self.spark, combined_structure)

        return dataframes, pipelines_df, activities_df, dependencies_df