from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window
from pyspark.sql import functions as F

def convert_to_dataframe(spark: SparkSession, combined_structure: dict):
    """
    Convert hierarchical JSON structure to Spark DataFrames.
    Ensures correct data types for 'id', 'level', and adds 'DWCreatedDate' timestamp.
    
    Args:
        spark (SparkSession): The active Spark session.
        combined_structure (dict): The hierarchical JSON structure containing nodes and links.
        
    Returns:
        tuple: A tuple containing two DataFrames: nodes_df and links_df.
    """
    # Convert nodes and links from the combined structure to DataFrames
    nodes_df = spark.createDataFrame(combined_structure['nodes']).withColumn("DWCreatedDate", current_timestamp())
    links_df = spark.createDataFrame(combined_structure['links']).withColumn("DWCreatedDate", current_timestamp())

    # Cast 'id' and 'level' in nodes_df to int
    nodes_df = nodes_df.withColumn("id", col("id").cast("int")).withColumn("level", col("level").cast("int"))

    # Display the DataFrames
    print("\n=== Nodes DataFrame ===")
    nodes_df.show(truncate=False)

    print("\n=== Links DataFrame ===")
    links_df.show(truncate=False)

    return nodes_df, links_df

def add_relationship_id(links_with_ids):
    """
    Add a unique relationship_id column to the links_with_ids DataFrame.
    
    Args:
        links_with_ids (DataFrame): The DataFrame containing link data.
        
    Returns:
        DataFrame: The updated DataFrame with the added 'relationship_id' column.
    """
    # Define window specification to ensure unique row numbering
    window_spec = Window.orderBy(F.monotonically_increasing_id())
    # Add a relationship_id as row_number over the window specification
    links_with_ids = links_with_ids.withColumn("relationship_id", row_number().over(window_spec).cast("int"))
    return links_with_ids
