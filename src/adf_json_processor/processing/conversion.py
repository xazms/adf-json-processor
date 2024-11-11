import datetime
from typing import Tuple, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType
from adf_json_processor.utils.helper import _generate_hash_key, _extract_last_part

class ADFDataConverter:
    """
    ADFDataConverter class for handling Azure Data Factory (ADF) data structures, including
    hierarchical structures, schema definitions, and Spark DataFrame conversions.
    """

    def __init__(self, debug=False):
        """
        Initializes the ADFDataConverter with an optional debug mode.
        
        Args:
            debug (bool): If True, enable debug-level logging and outputs.
        """
        self.debug = debug

    def build_hierarchical_structure_with_counts(self, adf_json: dict, include_types=None, include_empty=False) -> Tuple[dict, dict]:
        """
        Build a hierarchical structure from ADF JSON data with counts for pipelines, activities, and dependencies.

        Args:
            adf_json (dict): JSON structure of the ADF pipeline.
            include_types (list, optional): List of activity types to include.
            include_empty (bool): If True, includes pipelines with no activities.

        Returns:
            dict: Combined structure with pipelines, activities, and dependencies.
            dict: Counts of pipelines, activities by type, and dependencies.
        """
        nodes, dependencies = [], []
        node_name_to_id = {}
        node_name_to_type = {}
        node_name_to_target_name = {}
        counts = {"pipelines": 0, "activities": {}, "dependencies": 0}

        pipeline_name = adf_json.get("name")
        if not pipeline_name:
            raise ValueError("Pipeline JSON is missing the 'name' field.")

        pipeline_id = self._generate_id(pipeline_name, "Pipeline")
        pipeline_data = {"PipelineId": pipeline_id, "PipelineName": pipeline_name, "PipelineType": "Pipeline", "PipelineLevel": 1}
        counts["pipelines"] += 1

        activities = adf_json.get("properties", {}).get("activities", [])
        if not activities and not include_empty:
            return None, counts

        for activity in activities:
            activity_name = activity.get("name")
            activity_type = activity.get("type")

            if not activity_name or not activity_type:
                continue
            if include_types and activity_type not in include_types:
                continue

            counts["activities"].setdefault(activity_type, 0)
            counts["activities"][activity_type] += 1

            activity_id = self._generate_id(activity_name, activity_type, pipeline_id)
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

            # Define targets based on activity type
            self._define_activity_target(activity, activity_node, activity_type, pipeline_id)

            node_name_to_id[activity_name] = activity_id
            node_name_to_type[activity_name] = activity_type
            node_name_to_target_name[activity_name] = activity_node["ActivityTargetName"]
            nodes.append(activity_node)

        # Define dependencies
        for activity in activities:
            self._define_dependencies(activity, node_name_to_id, node_name_to_type, node_name_to_target_name, dependencies, counts)

        return {"pipelines": [pipeline_data], "activities": nodes, "dependencies": dependencies}, counts

    def _define_activity_target(self, activity, activity_node, activity_type, pipeline_id):
        """
        Defines target details for an activity node based on the activity type.
        """
        if activity_type == "ExecutePipeline":
            referenced_pipeline = activity["typeProperties"].get("pipeline", {}).get("referenceName")
            if referenced_pipeline:
                referenced_pipeline_id = self._generate_id(referenced_pipeline, activity_type, pipeline_id)
                activity_node["ActivityTargetId"] = referenced_pipeline_id
                activity_node["ActivityTargetName"] = referenced_pipeline
                activity_node["ActivityTargetType"] = "Pipeline"

        elif activity_type == "DatabricksNotebook":
            notebook_path = activity.get("typeProperties", {}).get("notebookPath", "")
            notebook_name = extract_last_part(notebook_path)

            notebook_target_id = self._generate_id(notebook_name, activity_type, pipeline_id)
            activity_node["ActivityTargetId"] = notebook_target_id
            activity_node["ActivityTargetName"] = notebook_name
            activity_node["ActivityTargetType"] = activity_type

        elif activity_type == "ExecuteDataFlow":
            dataflow_name = activity["typeProperties"].get("dataflow", {}).get("referenceName")
            if dataflow_name:
                dataflow_target_id = self._generate_id(dataflow_name, activity_type, pipeline_id)
                activity_node["ActivityTargetId"] = dataflow_target_id
                activity_node["ActivityTargetName"] = dataflow_name
                activity_node["ActivityTargetType"] = "DataFlow"

    def _define_dependencies(self, activity, node_name_to_id, node_name_to_type, node_name_to_target_name, dependencies, counts):
        """
        Defines dependencies for a given activity and adds them to the dependencies list.
        """
        activity_name = activity.get("name")
        activity_id = node_name_to_id.get(activity_name)
        activity_type = node_name_to_type.get(activity_name)

        if activity_name in node_name_to_id:
            for dependency in activity.get("dependsOn", []):
                dependent_activity_name = dependency.get("activity")

                if dependent_activity_name in node_name_to_id:
                    counts["dependencies"] += 1

                    dependent_activity_id = node_name_to_id[dependent_activity_name]
                    dependency = {
                        "DependencySourceId": dependent_activity_id,
                        "DependencyTargetId": activity_id,
                        "DependencyName": dependent_activity_name,
                        "DependencySourceType": node_name_to_type[dependent_activity_name],
                        "DependencyTargetType": activity_type,
                        "DependencySourceName": node_name_to_target_name.get(dependent_activity_name),
                        "DependencyTargetName": node_name_to_target_name.get(activity_name),
                    }
                    dependencies.append(dependency)

    def _generate_id(self, *args):
        """
        Generate a unique hash key using SHA-256 based on provided arguments.

        Args:
            *args: Variable length argument list to include in the hash.

        Returns:
            str: A hexadecimal hash string.
        """
        combined_string = '|'.join(str(arg) for arg in args if arg)
        return hashlib.sha256(combined_string.encode()).hexdigest()

    def define_schemas(self) -> Tuple[StructType, StructType, StructType]:
        """
        Define schemas for pipelines, activities, and dependencies DataFrames.
        """
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

        return pipelines_schema, activities_schema, dependencies_schema

    def convert_to_dataframe(self, spark: SparkSession, combined_structure: dict) -> Tuple[Dict[str, DataFrame], DataFrame, DataFrame, DataFrame]:
        """
        Convert the combined hierarchical structure into Spark DataFrames.

        Args:
            spark (SparkSession): Spark session for DataFrame creation.
            combined_structure (dict): Hierarchical structure of pipelines, activities, and dependencies.

        Returns:
            Tuple: A dictionary of DataFrames and individual DataFrames for pipelines, activities, and dependencies.
        """
        pipelines_schema, activities_schema, dependencies_schema = self.define_schemas()
        current_timestamp = datetime.datetime.now()

        # Convert each part of the combined structure to DataFrames
        pipelines_df = spark.createDataFrame(combined_structure['pipelines'], pipelines_schema) \
                            .withColumn("CreatedDate", F.lit(current_timestamp))
        activities_df = spark.createDataFrame(combined_structure['activities'], activities_schema) \
                             .withColumn("CreatedDate", F.lit(current_timestamp))
        dependencies_df = spark.createDataFrame(combined_structure['dependencies'], dependencies_schema) \
                               .withColumn("CreatedDate", F.lit(current_timestamp))

        # Collect DataFrames in a dictionary
        dataframes = {
            "pipelines_df": pipelines_df,
            "activities_df": activities_df,
            "dependencies_df": dependencies_df
        }

        if self.debug:
            for df_name, df in dataframes.items():
                print(f"\n=== {df_name} ===")
                df.show(truncate=False)

        return dataframes, pipelines_df, activities_df, dependencies_df