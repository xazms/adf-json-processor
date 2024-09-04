from adf_json_processor.file_handling.file_handler import FileHandler
from adf_json_processor.transformations.dataframe_utils import convert_to_dataframe, add_relationship_id
from adf_json_processor.catalog.catalog_utils import save_to_catalog
from adf_json_processor.workflow.pipeline_processor import process_multiple_json_files  # Updated to import from pipeline_processor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_json_files(file_handler: FileHandler, spark: SparkSession, config, include_types=None, include_empty=False, source_filename=None):
    """
    Process JSON files, convert them to DataFrame, and save to Unity Catalog.
    
    Args:
        file_handler (FileHandler): The file handler responsible for reading and writing data.
        spark (SparkSession): Active Spark session for DataFrame operations.
        config (Config): Configuration object containing settings and paths.
        include_types (list): List of activity types to include.
        include_empty (bool): Whether to include pipelines with no activities.
        source_filename (str): Specific source file to process (optional).
    
    Returns:
        tuple: nodes_df and links_with_ids DataFrames containing processed nodes and links.
    """
    # Update the source filename in the file handler if provided
    if source_filename:
        file_handler.update_source_filename(source_filename)

    # Process JSON files and build the combined structure using process_multiple_json_files from pipeline_processor
    combined_structure = process_multiple_json_files(
        file_handler=file_handler,
        include_types=include_types,
        include_empty=include_empty
    )

    # If no valid activities are found and 'include_empty' is False, exit
    if combined_structure is None:
        print("No activities found and 'include_empty' is set to False.")
        return None, None

    # Convert the hierarchical structure to Spark DataFrames
    nodes_df, links_df = convert_to_dataframe(spark, combined_structure)

    # Join links_df with nodes_df to map source_id and target_id based on node names
    links_with_source = links_df.join(
        nodes_df.select("id", "name"),
        links_df.source == nodes_df.name,
        "left"
    ).withColumnRenamed("id", "source_id").drop("name")

    links_with_ids = links_with_source.join(
        nodes_df.select("id", "name"),
        links_with_source.target == nodes_df.name,
        "left"
    ).withColumnRenamed("id", "target_id").drop("name")

    # Cast 'source_id', 'target_id' to integer type for consistency
    links_with_ids = links_with_ids.withColumn("source_id", col("source_id").cast("int"))
    links_with_ids = links_with_ids.withColumn("target_id", col("target_id").cast("int"))

    # Add a unique relationship_id column
    links_with_ids = add_relationship_id(links_with_ids)

    # Display the final DataFrame with source_id, target_id, and relationship_id for debugging
    print("\n=== Final Links DataFrame with source_id, target_id, and relationship_id ===")
    links_with_ids.select("source", "target", "source_id", "target_id", "relationship_id", "type").show(truncate=False)

    # Merge the updated DataFrames to Unity Catalog (nodes and links)
    save_to_catalog(nodes_df, links_with_ids)

    print("Processing complete.")
    return nodes_df, links_with_ids