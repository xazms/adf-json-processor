from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def merge_to_catalog(spark, target_table_name, source_df, key_columns):
    """
    Merge source_df into the target_table_name using Delta Lake's merge functionality.
    Detects changes and updates existing records or inserts new ones.
    
    Args:
        spark (SparkSession): The active Spark session.
        target_table_name (str): The target Delta table name.
        source_df (DataFrame): The source DataFrame to merge.
        key_columns (list): List of key columns to perform the merge.
    """
    if DeltaTable.isDeltaTable(spark, target_table_name):
        delta_table = DeltaTable.forName(spark, target_table_name)

        # Get the table version before merge
        version_before = delta_table.history().select("version").orderBy(F.desc("version")).first()["version"]
        print(f"\nVersion before merge for {target_table_name}: {version_before}")

        # Build merge condition using key_columns
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in key_columns])

        # Build change detection condition (excluding DWCreatedDate)
        data_columns = [c for c in source_df.columns if c != "DWCreatedDate"]
        change_condition = " OR ".join([f"target.{c} <> source.{c}" for c in data_columns])

        # Perform the merge operation
        delta_table.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition=F.expr(change_condition),
            set={col: F.col(f"source.{col}") for col in source_df.columns}
        ).whenNotMatchedInsertAll().execute()

        version_after = delta_table.history().select("version").orderBy(F.desc("version")).first()["version"]
        print(f"Merge completed for table {target_table_name}")

        return version_before, version_after
    else:
        # If the table doesn't exist, create it
        source_df.write.format("delta").saveAsTable(target_table_name)
        print(f"\nCREATE TABLE {target_table_name}")
        print(f"Table {target_table_name} created.")
        return None, None

def truncate_and_reload_fact_table(spark, table_name, fact_df):
    """
    Truncate and reload the fact table with new data.
    
    Args:
        spark (SparkSession): The active Spark session.
        table_name (str): The name of the fact table.
        fact_df (DataFrame): The DataFrame containing fact data to be loaded.
    """
    if DeltaTable.isDeltaTable(spark, table_name):
        spark.sql(f"TRUNCATE TABLE {table_name}")
        print(f"TRUNCATE TABLE {table_name}")
    else:
        print(f"Table {table_name} does not exist. Creating the table.")

    fact_df.write.format("delta").mode("append").saveAsTable(table_name)
    print(f"TRUNCATE and LOAD {table_name} with new data.")

def save_to_catalog(spark, nodes_df: DataFrame, links_with_ids: DataFrame):
    """
    Save the nodes and links DataFrames to Unity Catalog.
    Ensure the order of columns in fact table (relationship_id, source_id, target_id, followed by the rest).
    
    Args:
        spark (SparkSession): The active Spark session.
        nodes_df (DataFrame): DataFrame of nodes to save.
        links_with_ids (DataFrame): DataFrame of links with IDs to save.
    """
    # Define table names
    catalog_table_name_nodes = "dpuniformstoragetest.dim__adf_activities"
    catalog_table_name_links_with_ids = "dpuniformstoragetest.fact__adf_activity_relationships"

    # Sort columns for fact table: relationship_id, source_id, target_id, followed by the rest
    columns_order = ["relationship_id", "source_id", "target_id"] + \
                    [col for col in links_with_ids.columns if col not in ["relationship_id", "source_id", "target_id"]]

    # Reorder columns in fact table (links_with_ids)
    links_with_ids = links_with_ids.select(columns_order)

    # Merge Dimension Table, ignoring 'DWCreatedDate' when detecting changes
    print(f"\nMerging Dimension Table: {catalog_table_name_nodes}")
    merge_to_catalog(spark, catalog_table_name_nodes, nodes_df, key_columns=['id'])

    # Truncate and reload the Fact Table
    print(f"\nTruncating and Loading Fact Table: {catalog_table_name_links_with_ids}")
    truncate_and_reload_fact_table(spark, catalog_table_name_links_with_ids, links_with_ids)

    print(f"\nData merged and saved into Unity Catalog.")

def print_merge_changes(spark, table_name, version_before, version_after, data_changed):
    """
    Print merge changes, showing top 10 rows from the new version of the table or stating no changes.
    
    Args:
        spark (SparkSession): The active Spark session.
        table_name (str): The Delta table name to check.
        version_before (int): The version of the table before the merge.
        version_after (int): The version of the table after the merge.
        data_changed (bool): Boolean indicating if data was changed.
    """
    if data_changed:
        delta_table = DeltaTable.forName(spark, table_name)
        print(f"\n=== Changes detected in {table_name} (version {version_after}) ===")
        delta_table.toDF().limit(10).show(truncate=False)
    else:
        print(f"No changes detected in {table_name}. The table version remains the same.")