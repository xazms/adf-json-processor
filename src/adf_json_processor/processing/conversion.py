import datetime
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType

# Define a function to return the schemas for all DataFrames
def define_schemas():
    """
    Define the schemas for pipelines, activities, and dependencies DataFrames.
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

# Convert the combined structure into Spark DataFrames
def convert_to_dataframe(spark: SparkSession, combined_structure: dict, debug: bool = False) -> Tuple[dict, DataFrame, DataFrame, DataFrame]:
    """
    Convert the combined hierarchical structure into Spark DataFrames.
    """
    pipelines_schema, activities_schema, dependencies_schema = define_schemas()

    current_timestamp = datetime.datetime.now()

    # Create DataFrames
    pipelines_df = spark.createDataFrame(combined_structure['pipelines'], pipelines_schema) \
                        .withColumn("CreatedDate", F.lit(current_timestamp))

    activities_df = spark.createDataFrame(combined_structure['activities'], activities_schema) \
                         .withColumn("CreatedDate", F.lit(current_timestamp))

    dependencies_df = spark.createDataFrame(combined_structure['dependencies'], dependencies_schema) \
                           .withColumn("CreatedDate", F.lit(current_timestamp))

    # Dictionary of DataFrames
    dataframes = {
        "pipelines_df": pipelines_df,
        "activities_df": activities_df,
        "dependencies_df": dependencies_df
    }

    if debug:
        for df_name, df in dataframes.items():
            print(f"\n=== {df_name} ===")
            display(df)

    return dataframes, pipelines_df, activities_df, dependencies_df

# === main.py ===

def process_json_files(file_handler, spark, config, include_types=None, include_empty=False, include_json=False, debug=False, save_to_file=False, helper=None):
    """
    Main function to process JSON files, extract hierarchical structure, and convert it to DataFrames.
    
    Args:
        file_handler (FileHandler): File handler for retrieving and reading JSON files.
        spark (SparkSession): Active Spark session.
        config (Config): Configuration object.
        include_types (list): List of activity types to include.
        include_empty (bool): Whether to include empty activities.
        include_json (bool): Whether to print the combined structure.
        debug (bool): Debug flag for logging.
        save_to_file (bool): If True, saves the combined hierarchical structure to a file.
        helper (object, optional): Helper object for logging.
    
    Returns:
        Tuple[DataFrame, DataFrame, DataFrame, DataFrame]: Pipelines, Activities, and Dependencies DataFrames.
    """
    # Step 1: Process the JSON files and build the hierarchical structure, along with counts
    combined_structure, total_counts = process_multiple_json_files_with_counts(file_handler, include_types, include_empty, include_json, debug)

    # Step 1.1: Print the total counts (for validation)
    if debug:
        print("\n=== Total Counts ===")
        print(f"Pipelines: {total_counts['pipelines']}")
        print(f"Activities by Type: {total_counts['activities']}")
        print(f"Dependencies: {total_counts['dependencies']}")

    # Step 2: Optionally print the structure
    if include_json:
        print_json_structure(combined_structure, debug)

    # Step 3: Save the combined structure to a file if save_to_file is True
    if save_to_file:
        save_json_to_file(combined_structure, config.output_path, helper, debug)

    # Step 4: Convert the combined structure into DataFrames
    dataframes, pipelines_df, activities_df, dependencies_df = convert_to_dataframe(spark, combined_structure, debug)

    return dataframes, pipelines_df, activities_df, dependencies_df