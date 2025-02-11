import json
from typing import Tuple, List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql import functions as F
from adf_json_processor.utils.logger import Logger
from adf_json_processor.utils.helper import Helper
from adf_json_processor.file_handling.file_handler import FileHandler

class Processor:
    """
    Handles the processing of ADF JSON files, builds hierarchical structures,
    and converts them into Spark DataFrames.
    """

    def __init__(self, logger: Logger = None, helper: Helper = None, include_types: List[str] = None, debug: bool = False):
        """
        Initialize the Processor.

        Args:
            logger (Logger, optional): Logger instance.
            helper (Helper, optional): Helper instance for utility functions.
            include_types (list, optional): List of activity types to include.
            debug (bool): Enable debug-level logging.
        """
        self.logger = logger if logger is not None else Logger(debug=debug)
        self.debug = debug
        # Use provided helper or initialize one using its factory method.
        self.helper = helper if helper is not None else Helper.initialize(logger=self.logger, debug=self.debug)
        self.include_types = include_types or ["DatabricksNotebook", "ExecutePipeline", "ExecuteDataFlow"]
        self.logger.log_info("Processor initialized successfully.")
    
    @staticmethod
    def initialize(logger: Logger = None, helper: Helper = None, include_types=None, debug=False):
        """
        Factory method to initialize the Processor class.

        Args:
            logger (Logger, optional): Logger instance.
            helper (Helper, optional): Helper instance for utility functions.
            include_types (list, optional): List of activity types to include.
            debug (bool): Enable debug-level logging.

        Returns:
            Processor: An initialized Processor instance.
        """
        logger = logger if logger is not None else Logger(debug=debug)
        # Override logger's debug flag for consistency
        logger.debug = debug
        helper = helper if helper is not None else Helper.initialize(logger=logger, debug=debug)
        logger.log_info("Initializing Processor...")
        return Processor(logger=logger, helper=helper, include_types=include_types, debug=debug)

    def define_schemas(self) -> Tuple[StructType, StructType, StructType]:
        """
        Define the schemas for pipelines, activities, and dependencies DataFrames.

        Returns:
            tuple: Schemas for pipelines, activities, and dependencies.
        """
        self.logger.log_start("define_schemas")
        try:
            pipelines_schema = StructType() \
                .add("PipelineId", StringType()) \
                .add("PipelineName", StringType()) \
                .add("PipelineType", StringType()) \
                .add("PipelineLevel", StringType()) \
                .add("CreatedDate", TimestampType())

            activities_schema = StructType() \
                .add("ActivityId", StringType()) \
                .add("ParentId", StringType()) \
                .add("ActivityName", StringType()) \
                .add("ActivityType", StringType()) \
                .add("ActivityLevel", StringType()) \
                .add("ActivityTargetId", StringType()) \
                .add("ActivityTargetName", StringType()) \
                .add("ActivityTargetType", StringType()) \
                .add("CreatedDate", TimestampType())

            dependencies_schema = StructType() \
                .add("DependencySourceId", StringType()) \
                .add("DependencyTargetId", StringType()) \
                .add("DependencyName", StringType()) \
                .add("DependencySourceType", StringType()) \
                .add("DependencyTargetType", StringType()) \
                .add("DependencySourceName", StringType()) \
                .add("DependencyTargetName", StringType()) \
                .add("CreatedDate", TimestampType())

            self.logger.log_message("Schemas defined successfully.", level="debug")
            return pipelines_schema, activities_schema, dependencies_schema

        except Exception as e:
            self.logger.log_error(f"Error in define_schemas: {e}")
            raise
        finally:
            self.logger.log_end("define_schemas", success=True)

    def convert_to_dataframe(self, spark: SparkSession, combined_structure: dict):
        """
        Convert the combined hierarchical structure into Spark DataFrames.

        Args:
            spark (SparkSession): Active Spark session.
            combined_structure (dict): Combined hierarchical structure containing pipelines, activities, and dependencies.

        Returns:
            tuple: Dictionary of DataFrames and individual DataFrames for pipelines, activities, and dependencies.
        """
        self.logger.log_start("convert_to_dataframe")
        try:
            pipelines_schema, activities_schema, dependencies_schema = self.define_schemas()
            current_timestamp = F.current_timestamp()

            pipelines_df = spark.createDataFrame(
                combined_structure["pipelines"], schema=pipelines_schema
            ).withColumn("CreatedDate", current_timestamp)

            activities_df = spark.createDataFrame(
                combined_structure["activities"], schema=activities_schema
            ).withColumn("CreatedDate", current_timestamp)

            dependencies_df = spark.createDataFrame(
                combined_structure["dependencies"], schema=dependencies_schema
            ).withColumn("CreatedDate", current_timestamp)

            #self.logger.log_dataframe_summary(pipelines_df, "Pipelines", level="debug")
            #self.logger.log_dataframe_summary(activities_df, "Activities", level="debug")
            #self.logger.log_dataframe_summary(dependencies_df, "Dependencies", level="debug")

            dataframes = {
                "pipelines_df": pipelines_df,
                "activities_df": activities_df,
                "dependencies_df": dependencies_df
            }
            return dataframes, pipelines_df, activities_df, dependencies_df
        except Exception as e:
            self.logger.log_error(f"Error in convert_to_dataframe: {e}")
            raise
        finally:
            self.logger.log_end("convert_to_dataframe", success=True)
    
    def build_hierarchical_structure(self, adf_json, include_types=None):
        """
        Build a hierarchical structure from ADF JSON data, filtering both activities and dependencies.

        Args:
            adf_json (dict): JSON representation of an ADF pipeline.
            include_types (list, optional): List of activity types to include.

        Returns:
            tuple: (dict: Combined hierarchical structure, dict: Counts of pipelines, activities by type, and dependencies)
        """
        try:
            nodes, dependencies = [], []
            counts = {"pipelines": 0, "activities": {}, "dependencies": 0}

            pipeline_name = adf_json.get("name")
            if not pipeline_name:
                self.logger.log_error("Pipeline JSON is missing the 'name' field.")
                raise ValueError("Pipeline JSON is missing the 'name' field.")

            pipeline_id = self.helper.generate_hash_key(pipeline_name, "Pipeline")
            pipeline_data = {
                "PipelineId": pipeline_id,
                "PipelineName": pipeline_name,
                "PipelineType": "Pipeline",
                "PipelineLevel": 1
            }
            counts["pipelines"] += 1

            activities = adf_json.get("properties", {}).get("activities", [])
            activity_name_to_id = {}
            activity_name_to_type = {}
            node_name_to_target_name = {}
            unique_dependencies = set()

            # First pass: Process Activities
            for activity in activities:
                activity_name = activity.get("name")
                activity_type = activity.get("type")
                if not activity_name or not activity_type:
                    continue
                if include_types and activity_type not in include_types:
                    continue

                activity_id = self.helper.generate_hash_key(activity_name, activity_type, pipeline_id)
                activity_name_to_id[activity_name] = activity_id
                activity_name_to_type[activity_name] = activity_type
                counts["activities"].setdefault(activity_type, 0)
                counts["activities"][activity_type] += 1

                activity_node = {
                    "ActivityId": activity_id,
                    "ParentId": pipeline_id,
                    "ActivityName": activity_name,
                    "ActivityType": activity_type,
                    "ActivityLevel": 2,
                    "ActivityTargetId": None,
                    "ActivityTargetName": None,
                    "ActivityTargetType": None,
                }

                if activity_type == "ExecutePipeline":
                    referenced_pipeline = activity.get("typeProperties", {}).get("pipeline", {}).get("referenceName")
                    if referenced_pipeline:
                        referenced_pipeline_id = self.helper.generate_hash_key(referenced_pipeline, activity_type, pipeline_id)
                        activity_node["ActivityTargetId"] = referenced_pipeline_id
                        activity_node["ActivityTargetName"] = referenced_pipeline
                        activity_node["ActivityTargetType"] = "Pipeline"
                elif activity_type == "DatabricksNotebook":
                    notebook_path = activity.get("typeProperties", {}).get("notebookPath", "")
                    notebook_name = self.helper.extract_last_part(notebook_path)
                    notebook_target_id = self.helper.generate_hash_key(notebook_name, activity_type, pipeline_id)
                    activity_node["ActivityTargetId"] = notebook_target_id
                    activity_node["ActivityTargetName"] = notebook_name
                    activity_node["ActivityTargetType"] = activity_type
                elif activity_type == "ExecuteDataFlow":
                    dataflow_name = activity.get("typeProperties", {}).get("dataflow", {}).get("referenceName")
                    if dataflow_name:
                        dataflow_target_id = self.helper.generate_hash_key(dataflow_name, activity_type, pipeline_id)
                        activity_node["ActivityTargetId"] = dataflow_target_id
                        activity_node["ActivityTargetName"] = dataflow_name
                        activity_node["ActivityTargetType"] = "DataFlow"

                node_name_to_target_name[activity_name] = activity_node["ActivityTargetName"]
                nodes.append(activity_node)

            # Second pass: Process Dependencies
            for activity in activities:
                activity_name = activity.get("name")
                activity_id = activity_name_to_id.get(activity_name)
                activity_type = activity_name_to_type.get(activity_name)
                if not activity_name or not activity_id:
                    continue
                depends_on = activity.get("dependsOn", [])
                for dependency in depends_on:
                    dependency_name = dependency.get("activity")
                    if dependency_name in activity_name_to_id:
                        dependency_id = activity_name_to_id[dependency_name]
                        dependency_source_type = activity_name_to_type.get(dependency_name)
                        dependency_target_type = activity_type
                        if include_types and (dependency_source_type not in include_types or dependency_target_type not in include_types):
                            continue
                        dependency_key = (dependency_id, activity_id, dependency_name)
                        if dependency_key not in unique_dependencies:
                            unique_dependencies.add(dependency_key)
                            dependencies.append({
                                "DependencySourceId": dependency_id,
                                "DependencyTargetId": activity_id,
                                "DependencyName": dependency_name,
                                "DependencySourceType": dependency_source_type,
                                "DependencyTargetType": dependency_target_type,
                                "DependencySourceName": node_name_to_target_name.get(dependency_name),
                                "DependencyTargetName": node_name_to_target_name.get(activity_name),
                            })
                            counts["dependencies"] += 1

            self.logger.log_debug(
                f"Processed pipeline: {pipeline_name}. Activities: {len(nodes)} | Dependencies: {len(dependencies)}"
            )
            return {"pipelines": [pipeline_data], "activities": nodes, "dependencies": dependencies}, counts

        except Exception as e:
            self.logger.log_error(f"Error in build_hierarchical_structure_with_counts: {e}")
            raise

    def process_files(self, file_handler):
        """
        Process ADF JSON files and construct the combined hierarchical structure.

        Args:
            file_handler (FileHandler): File handler instance.

        Returns:
            tuple: Combined structure and counts.
        """
        self.logger.log_start("Processing files and building hierarchical structure")
        combined_structure = {"pipelines": [], "activities": [], "dependencies": []}
        total_counts = {"pipelines": 0, "activities": {}, "dependencies": 0}
        try:
            filtered_files = file_handler.get_filtered_file_list()
            for file in filtered_files:
                try:
                    file_path = file["path"]
                    if self.debug:
                        self.logger.log_debug(f"Processing file: {file_path}")
                    file_content = file_handler.get_adf_file_content(file_path)
                    adf_data = self.helper.parse_json(file_content)
                    hierarchical_structure, counts = self.build_hierarchical_structure(
                        adf_json=adf_data,
                        include_types=self.include_types
                    )
                    if self.debug:
                        self.logger.log_debug(f"Processed file: {file_path}. Counts: {counts}")
                    combined_structure["pipelines"].extend(hierarchical_structure["pipelines"])
                    combined_structure["activities"].extend(hierarchical_structure["activities"])
                    combined_structure["dependencies"].extend(hierarchical_structure["dependencies"])
                    total_counts["pipelines"] += counts["pipelines"]
                    total_counts["dependencies"] += counts["dependencies"]
                    for activity_type, count in counts["activities"].items():
                        total_counts["activities"][activity_type] = total_counts["activities"].get(activity_type, 0) + count
                except Exception as e:
                    self.logger.log_error(f"Error processing file {file_path}: {e}")
            
            # Log a summary block with one line per summary item at INFO level:
            summary_lines = [
                f"Pipelines: {total_counts['pipelines']}",
                f"Activities by Type: {total_counts['activities']}",
                f"Dependencies: {total_counts['dependencies']}"
            ]
            self.logger.log_block("Processing Summary", summary_lines, level="info")

        except Exception as e:
            self.logger.log_error(f"Error in process_files: {e}")
            raise
        finally:
            self.logger.log_end("Processing files and building hierarchical structure")
        return combined_structure, total_counts

    def test_processor(self, file_handler, spark: SparkSession):
        """
        Runs a self-contained test for the Processor using only ONE file.

        Args:
            file_handler (FileHandler): The file handler instance for fetching JSON files.
            spark (SparkSession): The active Spark session.

        Returns:
            None
        """
        try:
            print("\n🔹 **Running Processor Test (Single File Mode)...**\n")
            filtered_files = file_handler.get_filtered_file_list()
            if not filtered_files:
                print("\n❌ **No files found for testing. Aborting.**\n")
                return
            test_file = filtered_files[0]
            file_path = test_file["path"]
            print(f"\n🔹 **Processing Test File: {file_path}**\n")
            file_content = file_handler.get_adf_file_content(file_path)
            adf_data = self.helper.parse_json(file_content)
            combined_structure, total_counts = self.build_hierarchical_structure(
                adf_json=adf_data,
                include_types=self.include_types
            )
            dataframes, pipelines_df, activities_df, dependencies_df = self.convert_to_dataframe(spark, combined_structure)
            self.logger.log_block("Test DataFrame Overview", [
                f"✅ Pipelines: {pipelines_df.count()} rows",
                f"✅ Activities: {activities_df.count()} rows",
                f"✅ Dependencies: {dependencies_df.count()} rows"
            ], level="info")
            print("\n✅ **Processor Test Passed Successfully!** 🎉\n")
        except Exception as e:
            print(f"\n❌ **Processor Test Failed:** {e}\n")
