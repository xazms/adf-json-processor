import json
import uuid
import hashlib

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from custom_utils.dp_storage import writer  # Import writer module
from pyspark.sql import SparkSession
from typing import List, Dict

def table_exists(spark: SparkSession, table_name: str) -> bool:
    """
    Check if a Delta table exists in the Spark catalog.

    Args:
        spark (SparkSession): The active Spark session.
        table_name (str): The name of the Delta table to check.

    Returns:
        bool: True if the Delta table exists, False otherwise.
    """
    return spark._jsparkSession.catalog().tableExists(table_name)

def get_table_version(spark: SparkSession, table_name: str) -> int:
    """
    Get the current version of the Delta table.

    Args:
        spark (SparkSession): The active Spark session.
        table_name (str): The name of the Delta table.

    Returns:
        int: The version number of the Delta table.
    """
    delta_table = DeltaTable.forName(spark, table_name)
    return delta_table.history().select("version").orderBy(F.desc("version")).first()["version"]

def create_temp_views(dataframes: dict, debug=False):
    """
    Create temporary views for each DataFrame in the given dictionary.
    """
    if debug:
        print("\n=== Temporary views created ===")
    for df_name, df in dataframes.items():
        view_name = f"view_{df_name}"
        df.createOrReplaceTempView(view_name)
        if debug:
            print(f"{view_name} created")

def generate_hash_key(*args):
    """
    Generate a unique hash-based ID using provided values.
    
    Args:
        *args: Variable length argument list to generate a hash from.
    
    Returns:
        str: A unique hash key in hexadecimal.
    """
    combined_string = '|'.join(str(arg) for arg in args if arg)
    hash_object = hashlib.sha256(combined_string.encode())
    return hash_object.hexdigest()

def generate_unique_id():
    """
    Generate a unique UUID.
    
    Returns:
        str: A unique UUID string.
    """
    return str(uuid.uuid4())

def extract_last_part(path):
    """
    Extract the last part of a file path.
    
    Args:
        path (str): A string representing the full path.
    
    Returns:
        str: The last part of the path after the last slash (/).
    """
    return path.split('/')[-1] if path else None

def print_json_structure(json_data, title="Flattened JSON Structure", debug=False):
    """
    Print the JSON structure in a human-readable format.
    
    Args:
        json_data (dict): JSON data to print.
        title (str): Title of the JSON structure.
    """
    try:
        formatted_json = json.dumps(json_data, indent=4)
        if debug:
            print(f"\n=== {title} ===")
        print(formatted_json)
    except Exception as e:
        print(f"Error printing JSON structure: {e}")

def print_json_as_dataframe(spark, json_data):
    """
    Convert the combined JSON structure to a Spark DataFrame for visualization.
    """
    json_str = json.dumps(json_data)
    df = spark.read.json(spark.sparkContext.parallelize([json_str]))
    df.show(truncate=False)  # Print DataFrame for debugging before flattening

def save_json_to_file(json_data, output_path, helper=None, debug=False):
    """
    Saves the provided JSON data to the specified output file path.

    Args:
        json_data (dict): The JSON data to be saved.
        output_path (str): The file path where the JSON should be saved.
        helper (object, optional): Helper object for logging.
    """
    try:
        with open(output_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
        if debug:
            print(f"JSON successfully saved to {output_path}")
    except Exception as e:
        if debug:
            print(f"Error saving JSON to {output_path}: {str(e)}", "error")
        else:
            print(f"Error saving JSON to {output_path}: {str(e)}")

# ========================
# HELPER FUNCTIONS
# ========================

def pretty_print_message(message: str, helper=None, level: str = "info"):
    """
    Helper function to print or log messages in a consistent format.

    Args:
        message (str): The message to be printed or logged.
        helper (object, optional): Helper object for logging.
        level (str): The level of the message ('info', 'error', etc.).
    """
    if helper:
        helper.write_message(message)
    else:
        print(f"[{level.upper()}] {message}")


# ========================
# DESTINATION DETAILS HANDLING
# ========================

def get_destination_details(spark, destination_environment, source_datasetidentifier, helper=None):
    """
    Retrieves the destination path, database name, and table name for the given environment and dataset identifier.
    """
    try:
        destination_path = writer.get_destination_path_extended(destination_environment, source_datasetidentifier)
        database_name, table_name = writer.get_databricks_table_info_extended(destination_environment, source_datasetidentifier)
        pretty_print_message(f"Destination Path: {destination_path}\nDatabase: {database_name}\nTable: {table_name}", helper)
        return destination_path, database_name, table_name
    except Exception as e:
        pretty_print_message(f"Error retrieving destination details: {e}", helper, "error")
        raise

# ========================
# DUPLICATE HANDLING AND VALIDATION
# ========================

def delete_or_filter_duplicates(spark: SparkSession, temp_view_name: str, key_columns: List[str], helper=None):
    """
    Deletes or filters out duplicate records from the source data in the temporary view based on key columns.
    
    Args:
        spark (SparkSession): Active Spark session.
        temp_view_name (str): Name of the temporary view containing the source data.
        key_columns (List[str]): List of key columns to check for duplicates.
        helper (object, optional): Helper object for logging.
    
    Returns:
        str: Name of the new temporary view with duplicates removed.
    """
    key_columns_str = ', '.join(key_columns)

    # Query to remove duplicates by keeping the first occurrence based on key columns
    filtered_view_name = f"{temp_view_name}_deduped"
    dedupe_query = f"""
    WITH ranked_data AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_columns_str} ORDER BY {key_columns_str}) AS row_num
        FROM {temp_view_name}
    )
    SELECT * FROM ranked_data WHERE row_num = 1
    """

    try:
        deduped_df = spark.sql(dedupe_query)
        deduped_df.createOrReplaceTempView(filtered_view_name)
        pretty_print_message(f"Created new view without duplicates: {filtered_view_name}", helper)
        return filtered_view_name
    except Exception as e:
        pretty_print_message(f"Error during duplicate filtering: {e}", helper, "error")
        raise

def validate_and_create_duplicate_view(spark: SparkSession, temp_view_name: str, key_columns: List[str], remove_duplicates: bool = False, helper=None):
    """
    Validates the data in the temporary view for duplicates based on the key columns and, 
    if found, creates and displays a view with the duplicate records.
    
    If remove_duplicates is True, the duplicates will be removed automatically.
    If remove_duplicates is False, the function raises an error and informs the user.
    
    Args:
        spark (SparkSession): The active Spark session.
        temp_view_name (str): Name of the temporary view to check for duplicates.
        key_columns (list): List of key columns to check for duplicates.
        remove_duplicates (bool): Whether to automatically remove duplicates if found.
        helper (object, optional): Helper object for logging.
    
    Returns:
        bool: True if duplicates were removed or no duplicates were found. False otherwise.
    """
    key_columns_str = ', '.join(key_columns)
    duplicate_keys_query = f"""
    SELECT {key_columns_str}, COUNT(*) AS duplicate_count
    FROM {temp_view_name}
    GROUP BY {key_columns_str}
    HAVING COUNT(*) > 1
    """

    # Execute the query to get duplicates
    duplicates_df = spark.sql(duplicate_keys_query)

    if duplicates_df.count() > 0:
        duplicates_view_name = f"view_duplicates_{temp_view_name}"
        duplicates_df.createOrReplaceTempView(duplicates_view_name)
        
        pretty_print_message(f"Duplicate records found. View created: {duplicates_view_name}", helper)
        display(spark.sql(f"SELECT * FROM {duplicates_view_name} ORDER BY duplicate_count DESC LIMIT 100"))

        if remove_duplicates:
            pretty_print_message(f"Duplicates were found and removed. Continuing with the creation.", helper)
            return delete_or_filter_duplicates(spark, temp_view_name, key_columns, helper)  # Call to delete or filter out duplicates
        else:
            pretty_print_message(f"Duplicate keys found in {temp_view_name}. Set 'remove_duplicates=True' to automatically remove duplicates.", helper, "error")
            raise ValueError(f"Duplicate keys found in {temp_view_name}. Create operation aborted.")
    else:
        pretty_print_message(f"No duplicates found in {temp_view_name}.", helper)
        return temp_view_name  # No changes needed, return the same view

# ========================
# FILE SYSTEM AND TABLE MANAGEMENT
# ========================

def ensure_path_exists(dbutils, destination_path, helper=None):
    """
    Ensures the destination path exists in DBFS. If not, creates the path.
    """
    try:
        dbutils.fs.ls(destination_path)
        pretty_print_message(f"Path already exists: {destination_path}", helper)
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            dbutils.fs.mkdirs(destination_path)
            pretty_print_message(f"Path did not exist. Created path: {destination_path}", helper)
        else:
            pretty_print_message(f"Error while ensuring path exists: {e}", helper, "error")
            raise


def recreate_delta_parquet(spark, destination_path, temp_view_name, helper=None):
    """
    Recreates Delta Parquet files from the data in the specified temp view.
    """
    try:
        df = spark.table(temp_view_name)
        pretty_print_message(f"Recreating Delta Parquet at {destination_path}", helper)
        # Show data if helper is available
        if helper:
            df.show()

        # Write data to Delta format
        df.write.format("delta").mode("overwrite").save(destination_path)
        pretty_print_message(f"Delta Parquet files written to: {destination_path}", helper)
    except Exception as e:
        pretty_print_message(f"Error recreating Delta Parquet: {e}", helper, "error")
        raise


def check_if_table_exists(spark, database_name, table_name, helper=None):
    """
    Checks whether a Databricks Delta table exists in the specified database.
    """
    try:
        table_check = spark.sql(f"SHOW TABLES IN {database_name}").collect()
        table_exists = any(row["tableName"] == table_name for row in table_check)

        if not table_exists:
            pretty_print_message(f"Table {database_name}.{table_name} does not exist.", helper)
        return table_exists
    except Exception as e:
        pretty_print_message(f"Error checking table existence: {e}", helper, "error")
        raise


def create_or_replace_table(spark, database_name, table_name, destination_path, temp_view_name, helper=None):
    """
    Creates a Databricks Delta table if it does not exist and writes data to it.
    """
    try:
        df = spark.table(temp_view_name)
        schema_str = ",\n    ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields])

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
            {schema_str}
        )
        USING DELTA
        LOCATION 'dbfs:{destination_path}/'
        """

        pretty_print_message(f"Creating table with query:\n{create_table_sql.strip()}", helper)

        if not check_if_table_exists(spark, database_name, table_name, helper):
            spark.sql(create_table_sql)
            pretty_print_message(f"Table {database_name}.{table_name} created.", helper)

            # Write data to the table location
            df.write.format("delta").mode("overwrite").save(destination_path)
            pretty_print_message(f"Data written to {destination_path}.", helper)
        else:
            pretty_print_message(f"Table {database_name}.{table_name} already exists.", helper)
    except Exception as e:
        pretty_print_message(f"Error creating or replacing table: {e}", helper, "error")
        raise


# ========================
# MAIN TABLE MANAGEMENT
# ========================

def manage_table_creation(spark, dbutils, destination_environment, source_datasetidentifier, temp_view_name, key_columns: List[str], remove_duplicates: bool = False, helper=None):
    """
    Orchestrates the table creation process by validating the path and creating the Delta table if needed.
    
    Args:
        spark: Spark session object.
        dbutils: Databricks utilities for file system operations.
        destination_environment: Target environment (e.g., storage account) for table creation.
        source_datasetidentifier: Unique identifier for the source dataset.
        temp_view_name: Name of the temp view containing data to be written.
        helper: Optional logging or helper object for logging details.
    """
    try:
        # Retrieve destination details (path, database, table)
        destination_path, database_name, table_name = get_destination_details(
            spark, destination_environment, source_datasetidentifier, helper
        )
        
        # Ensure the destination path exists, creating if necessary
        ensure_path_exists(dbutils, destination_path, helper)

        # Validate for duplicates (and remove if specified)
        temp_view_name = validate_and_create_duplicate_view(spark, temp_view_name, key_columns, remove_duplicates, helper)

        # Create or replace the Delta table with the data from the temporary view
        create_or_replace_table(spark, database_name, table_name, destination_path, temp_view_name, helper)
    except Exception as e:
        pretty_print_message(f"Error managing table creation: {e}", helper, "error")
        raise


def manage_multiple_table_creation(spark, dbutils, dataframes, destination_environment, remove_duplicates: bool = False, helper=None):
    """
    Loops through the dataframes and manages table creation for each.
    
    Args:
        spark: Spark session object.
        dbutils: Databricks utility object.
        dataframes: Dictionary of DataFrame names and DataFrames.
        destination_environment: Target environment (e.g., storage account) for table creation.
        helper: Optional logging or helper object for logging details.
    """
    key_columns_dict = {
        "pipelines_df": ["PipelineId"],
        "activities_df": ["ActivityId", "ParentId"],
        "dependencies_df": ["DependencySourceId", "DependencyTargetId"]
    }
    for df_name in dataframes.keys():
        temp_view_name = f"view_{df_name}"
        
        source_datasetidentifier = f"data_quality__adf_{df_name[:-3]}" if df_name.endswith("_df") else df_name

        if df_name in key_columns_dict:
            key_columns = key_columns_dict[df_name]
            pretty_print_message(f"\n--- Processing {df_name} ---", helper)
            pretty_print_message(f"Temp View: {temp_view_name}\nDataset Identifier: {source_datasetidentifier}", helper)
            
            # Manage table creation for the current DataFrame
            manage_table_creation(
                spark=spark,
                dbutils=dbutils,
                destination_environment=destination_environment,
                source_datasetidentifier=source_datasetidentifier,
                temp_view_name=temp_view_name,
                key_columns=key_columns,
                remove_duplicates=remove_duplicates,  # Pass the flag to remove duplicates if requested
                helper=helper
            )

# ========================
# UTILITY FUNCTIONS
# ========================

def pretty_print_message(message: str, helper=None, level: str = "info"):
    """
    Helper function to print or log messages in a consistent format.

    Args:
        message (str): The message to be printed or logged.
        helper (object, optional): Helper object for logging.
        level (str): The level of the message ('info', 'error', etc.).
    """
    if helper:
        helper.write_message(message)
    else:
        print(f"[{level.upper()}] {message}")

# ========================
# DESTINATION DETAILS HANDLING
# ========================

def get_merge_destination_details(spark: SparkSession, destination_environment: str, source_datasetidentifier: str, helper=None):
    """
    Retrieves the destination path, database name, and table name for merging data.
    
    Args:
        spark (SparkSession): Active Spark session.
        destination_environment (str): Destination storage environment.
        source_datasetidentifier (str): Source dataset identifier.
        helper (object, optional): Helper object for logging.
        
    Returns:
        Tuple: A tuple containing destination_path, database_name, and table_name.
    """
    try:
        destination_path = writer.get_destination_path_extended(destination_environment, source_datasetidentifier)
        database_name, table_name = writer.get_databricks_table_info_extended(destination_environment, source_datasetidentifier)
        pretty_print_message(f"Destination Details:\nPath: {destination_path}\nDatabase: {database_name}\nTable: {table_name}", helper)
        return destination_path, database_name, table_name
    except Exception as e:
        pretty_print_message(f"Error retrieving destination details: {str(e)}", helper, "error")
        raise

# ========================
# DUPLICATE HANDLING AND VALIDATION
# ========================

def delete_or_filter_duplicates(spark: SparkSession, temp_view_name: str, key_columns: List[str], helper=None):
    """
    Deletes or filters out duplicate records from the source data in the temporary view based on key columns.
    
    Args:
        spark (SparkSession): Active Spark session.
        temp_view_name (str): Name of the temporary view containing the source data.
        key_columns (List[str]): List of key columns to check for duplicates.
        helper (object, optional): Helper object for logging.
    
    Returns:
        str: Name of the new temporary view with duplicates removed.
    """
    key_columns_str = ', '.join(key_columns)

    # Query to remove duplicates by keeping the first occurrence based on key columns
    filtered_view_name = f"{temp_view_name}_deduped"
    dedupe_query = f"""
    WITH ranked_data AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_columns_str} ORDER BY {key_columns_str}) AS row_num
        FROM {temp_view_name}
    )
    SELECT * FROM ranked_data WHERE row_num = 1
    """

    try:
        deduped_df = spark.sql(dedupe_query)
        deduped_df.createOrReplaceTempView(filtered_view_name)
        pretty_print_message(f"Created new view without duplicates: {filtered_view_name}", helper)
        return filtered_view_name
    except Exception as e:
        pretty_print_message(f"Error during duplicate filtering: {e}", helper, "error")
        raise

def validate_and_create_duplicate_view(spark: SparkSession, temp_view_name: str, key_columns: List[str], remove_duplicates: bool = False, helper=None):
    """
    Validates the data in the temporary view for duplicates based on the key columns and, 
    if found, creates and displays a view with the duplicate records.
    
    If remove_duplicates is True, the duplicates will be removed automatically.
    If remove_duplicates is False, the function raises an error and informs the user.
    
    Args:
        spark (SparkSession): The active Spark session.
        temp_view_name (str): Name of the temporary view to check for duplicates.
        key_columns (list): List of key columns to check for duplicates.
        remove_duplicates (bool): Whether to automatically remove duplicates if found.
        helper (object, optional): Helper object for logging.
    
    Returns:
        bool: True if duplicates were removed or no duplicates were found. False otherwise.
    """
    key_columns_str = ', '.join(key_columns)
    duplicate_keys_query = f"""
    SELECT {key_columns_str}, COUNT(*) AS duplicate_count
    FROM {temp_view_name}
    GROUP BY {key_columns_str}
    HAVING COUNT(*) > 1
    """

    # Execute the query to get duplicates
    duplicates_df = spark.sql(duplicate_keys_query)

    if duplicates_df.count() > 0:
        duplicates_view_name = f"view_duplicates_{temp_view_name}"
        duplicates_df.createOrReplaceTempView(duplicates_view_name)
        
        pretty_print_message(f"Duplicate records found. View created: {duplicates_view_name}", helper)
        display(spark.sql(f"SELECT * FROM {duplicates_view_name} ORDER BY duplicate_count DESC LIMIT 100"))

        if remove_duplicates:
            pretty_print_message(f"Duplicates were found and removed. Continuing with the merge.", helper)
            return delete_or_filter_duplicates(spark, temp_view_name, key_columns, helper)  # Call to delete or filter out duplicates
        else:
            pretty_print_message(f"Duplicate keys found in {temp_view_name}. Set 'remove_duplicates=True' to automatically remove duplicates.", helper, "error")
            raise ValueError(f"Duplicate keys found in {temp_view_name}. Merge operation aborted.")
    else:
        pretty_print_message(f"No duplicates found in {temp_view_name}.", helper)
        return temp_view_name  # No changes needed, return the same view

# ========================
# SQL GENERATION AND EXECUTION
# ========================

def generate_merge_sql(temp_view_name: str, database_name: str, table_name: str, key_columns: List[str], helper=None):
    """
    Constructs the SQL query for the MERGE operation, excluding 'CreatedDate' from the update.

    Args:
        temp_view_name (str): Name of the temporary view containing source data.
        database_name (str): Name of the target database.
        table_name (str): Name of the target table.
        key_columns (list): List of key columns for matching.
        helper (object, optional): Helper object for logging.

    Returns:
        str: The MERGE SQL query.
    """
    # Fetch all columns from the temporary view, excluding key_columns and 'CreatedDate'
    all_columns = [col for col in spark.table(temp_view_name).columns if col not in key_columns and col != 'CreatedDate']

    # Create the match condition based on the key columns
    match_sql = ' AND '.join([f"s.{col.strip()} = t.{col.strip()}" for col in key_columns])

    # Create the update statement by specifying each column explicitly, excluding 'CreatedDate'
    update_sql = ', '.join([f"t.{col.strip()} = s.{col.strip()}" for col in all_columns])

    # Create the insert statement by specifying the columns and their values, including 'CreatedDate' for insert
    insert_columns = ', '.join([f"{col.strip()}" for col in key_columns + all_columns + ['CreatedDate']])
    insert_values = ', '.join([f"s.{col.strip()}" for col in key_columns + all_columns + ['CreatedDate']])

    # Generate the full MERGE SQL statement without the DELETE logic
    merge_sql = f"""
    MERGE INTO {database_name}.{table_name} AS t
    USING {temp_view_name} AS s
    ON {match_sql}
    WHEN MATCHED THEN
      UPDATE SET {update_sql}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns}) VALUES ({insert_values})
    """

    pretty_print_message(f"MERGE SQL without deletion:\n{merge_sql.strip()}", helper)
    return merge_sql

def generate_delete_sql(temp_view_name: str, database_name: str, table_name: str, key_columns: List[str], helper=None):
    """
    Constructs the SQL query to delete records from the target table that do not exist in the source view.

    Args:
        temp_view_name (str): Name of the temporary view containing source data.
        database_name (str): Name of the target database.
        table_name (str): Name of the target table.
        key_columns (list): List of key columns for matching.
        helper (object, optional): Helper object for logging.

    Returns:
        str: The DELETE SQL query.
    """
    # Create the condition for matching the key columns
    key_conditions = ' AND '.join([f"t.{col.strip()} = s.{col.strip()}" for col in key_columns])

    # Generate the DELETE SQL statement
    delete_sql = f"""
    DELETE FROM {database_name}.{table_name} AS t
    WHERE NOT EXISTS (
        SELECT 1 FROM {temp_view_name} AS s
        WHERE {key_conditions}
    )
    """

    pretty_print_message(f"DELETE SQL:\n{delete_sql.strip()}", helper)
    return delete_sql

def execute_merge_and_get_post_version(spark: SparkSession, database_name: str, table_name: str, merge_sql: str, pre_merge_version: int, helper=None):
    """
    Executes the MERGE operation and calculates the post-merge version.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): Target database name.
        table_name (str): Target table name.
        merge_sql (str): The SQL query for the MERGE operation.
        pre_merge_version (int): The version number of the Delta table before the merge.
        helper (object, optional): Helper object for logging.

    Returns:
        int: The version number of the Delta table after the merge.
    """
    try:
        spark.sql(merge_sql)
        pretty_print_message("Data merged successfully, including deletions.", helper)
        post_merge_version = pre_merge_version + 1
        return post_merge_version
    except Exception as e:
        pretty_print_message(f"Error during data merge: {e}", helper, "error")
        raise

def display_newly_merged_data(spark: SparkSession, database_name: str, table_name: str, pre_merge_version: int, post_merge_version: int, helper=None):
    """
    Displays the data that was newly merged by querying the differences between versions.
    
    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): Target database name.
        table_name (str): Target table name.
        pre_merge_version (int): Version number before the merge.
        post_merge_version (int): Version number after the merge.
        helper (object, optional): Helper object for logging.
    """
    merged_data_sql = f"""
    SELECT * FROM {database_name}.{table_name} VERSION AS OF {post_merge_version}
    EXCEPT
    SELECT * FROM {database_name}.{table_name} VERSION AS OF {pre_merge_version}
    """
    try:
        merged_data_df = spark.sql(merged_data_sql)
        if merged_data_df.count() == 0:
            pretty_print_message("No newly merged data.", helper)
        else:
            pretty_print_message("Displaying up to 100 rows of the newly merged data:", helper)
            display(merged_data_df.limit(100))
    except Exception as e:
        pretty_print_message(f"Error displaying merged data: {e}", helper, "error")
        raise

# ========================
# MERGE MANAGEMENT
# ========================

def get_pre_merge_version(spark: SparkSession, database_name: str, table_name: str, helper=None):
    """
    Retrieves the current version of the Delta table before the merge.
    
    Args:
        spark (SparkSession): Active Spark session.
        database_name (str): Target database name.
        table_name (str): Target table name.
        helper (object, optional): Helper object for logging.
    
    Returns:
        int: The current version of the Delta table.
    """
    try:
        pre_merge_version = spark.sql(f"DESCRIBE HISTORY {database_name}.{table_name} LIMIT 1").select("version").collect()[0][0]
        return pre_merge_version
    except Exception as e:
        pretty_print_message(f"Error retrieving pre-merge version: {e}", helper, "error")
        raise

def manage_data_merge(spark: SparkSession, destination_environment: str, source_datasetidentifier: str, temp_view_name: str, key_columns: List[str], remove_duplicates: bool = False, helper=None):
    """
    Manages the entire data merge process, including validation for duplicates, insertion, updating, 
    and deletion of records that are no longer in the source view.

    Args:
        spark (SparkSession): The active Spark session.
        destination_environment (str): Destination environment for storage.
        source_datasetidentifier (str): Source dataset identifier.
        temp_view_name (str): Name of the temporary view to merge.
        key_columns (list): List of key columns for the merge.
        remove_duplicates (bool): Flag to automatically remove duplicates if found.
        helper (object, optional): Helper object for logging.
    """
    # Step 1: Retrieve destination details
    destination_path, database_name, table_name = get_merge_destination_details(spark, destination_environment, source_datasetidentifier, helper)

    # Step 2: Validate for duplicates (and remove if specified)
    temp_view_name = validate_and_create_duplicate_view(spark, temp_view_name, key_columns, remove_duplicates, helper)

    # Step 3: Retrieve the current version of the table
    pre_merge_version = get_pre_merge_version(spark, database_name, table_name, helper)

    # Step 4: Generate the merge SQL query (without DELETE)
    merge_sql = generate_merge_sql(temp_view_name, database_name, table_name, key_columns, helper)

    # Step 5: Execute the merge
    execute_merge_and_get_post_version(spark, database_name, table_name, merge_sql, pre_merge_version, helper)

    # Step 6: Generate and execute the DELETE query
    delete_sql = generate_delete_sql(temp_view_name, database_name, table_name, key_columns, helper)
    spark.sql(delete_sql)

    # Step 7: Display the newly merged data
    post_merge_version = pre_merge_version + 1
    display_newly_merged_data(spark, database_name, table_name, pre_merge_version, post_merge_version, helper)

def manage_multiple_data_merge(spark: SparkSession, dbutils, destination_environment: str, dataframes: Dict[str, any], remove_duplicates: bool = False, helper=None):
    """
    Orchestrates the process of managing the merge for multiple dataframes.
    
    Args:
        spark (SparkSession): The active Spark session.
        dbutils: Databricks utilities for file system operations.
        destination_environment (str): Destination environment for storage.
        dataframes (dict): A dictionary containing DataFrame names and their corresponding objects.
        remove_duplicates (bool): If True, automatically remove duplicates during the merge process.
        helper (object, optional): Helper object for logging.
    """
    key_columns_dict = {
        "pipelines_df": ["PipelineId"],
        "activities_df": ["ActivityId", "ParentId"],
        "dependencies_df": ["DependencySourceId", "DependencyTargetId"]
    }

    for df_name in dataframes.keys():
        temp_view_name = f"view_{df_name}"
        
        source_datasetidentifier = f"data_quality__adf_{df_name[:-3]}" if df_name.endswith("_df") else df_name

        if df_name in key_columns_dict:
            key_columns = key_columns_dict[df_name]
            pretty_print_message(f"\n--- Processing {df_name} ---", helper)
            pretty_print_message(f"Temp View: {temp_view_name}\nDataset Identifier: {source_datasetidentifier}", helper)
            
            # Perform the merge for each dataframe
            manage_data_merge(
                spark=spark,
                destination_environment=destination_environment,
                source_datasetidentifier=source_datasetidentifier,
                temp_view_name=temp_view_name,
                key_columns=key_columns,
                remove_duplicates=remove_duplicates,  # Pass the flag to remove duplicates if requested
                helper=helper
            )