from pyspark.sql import SparkSession
from typing import List, Dict, Tuple
from adf_json_processor.utils.logger import Logger
from adf_json_processor.storage.writer import get_destination_path_extended, get_databricks_table_info_extended

# ==============================================================================  
# TableManager Class  
# ==============================================================================  
class TableManager:
    """
    Manages the creation and merging of Databricks Delta tables.

    Functionality:
      1. `create_table`: Creates a new Delta table from a temporary view.
      2. `merge_table`: Merges new data into an existing Delta table.
      3. `validate_and_create_duplicate_view`: Checks for duplicates and optionally removes them.
    """

    def __init__(self, spark: SparkSession, dbutils, destination_environment: str, logger: Logger = None):
        """
        Initializes the TableManager.

        Args:
            spark (SparkSession): Active Spark session.
            dbutils: Databricks utilities.
            destination_environment (str): Destination storage environment (e.g., storage account name).
            logger (Logger, optional): Custom logger instance. If not provided, a default logger is created.
        """
        self.spark = spark
        self.dbutils = dbutils
        self.destination_environment = destination_environment
        self.logger = logger if logger else Logger(debug=False)
        self.logger.log_info("✅ TableManager initialized.")

    def get_destination_details(self, source_datasetidentifier: str) -> Tuple[str, str, str]:
        """
        Retrieves the destination path, database name, and table name for a given dataset identifier.

        Args:
            source_datasetidentifier (str): Source dataset identifier.

        Returns:
            tuple: (destination_path, database_name, table_name)
        """
        try:
            destination_path = get_destination_path_extended(self.destination_environment, source_datasetidentifier)
            database_name = self.destination_environment  # Database is based on the environment
            table_name = source_datasetidentifier  # Table name is derived from dataset identifier

            self.logger.log_block("Destination Details", [
                f"Destination Path: {destination_path}",
                f"Database: {database_name}",
                f"Table: {table_name}"
            ], level="info")

            return destination_path, database_name, table_name
        except Exception as e:
            self.logger.log_error(f"Error retrieving destination details: {e}")
            raise

    def ensure_path_exists(self, destination_path: str):
        """
        Ensures that the destination path exists in DBFS, creating it if necessary.

        Args:
            destination_path (str): Destination path.
        """
        try:
            self.dbutils.fs.ls(destination_path)
            self.logger.log_info(f"✅ Path already exists: {destination_path}")
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                self.dbutils.fs.mkdirs(destination_path)
                self.logger.log_info(f"📂 Path did not exist. Created: {destination_path}")
            else:
                self.logger.log_error(f"Error ensuring path exists: {e}")
                raise

    def check_if_table_exists(self, database_name: str, table_name: str) -> bool:
        """
        Checks if a Delta table exists in the specified database.

        Args:
            database_name (str): Target database.
            table_name (str): Target table.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            self.logger.log_message(f"Checking existence of table: {database_name}.{table_name}", level="info")

            # Corrected SQL statement
            table_check = self.spark.sql(f"SHOW TABLES IN {database_name}").collect()
            exists = any(row["tableName"] == table_name for row in table_check)

            if exists:
                self.logger.log_message(f"✅ Table {database_name}.{table_name} exists.", level="info")
            else:
                self.logger.log_message(f"Table {database_name}.{table_name} does not exist.", level="info")

            return exists

        except Exception as e:
            self.logger.log_message(f"Error checking table existence: {e}", level="error")
            raise

    def create_table(self, source_datasetidentifier: str, temp_view_name: str):
        """
        Creates a new Delta table from a temporary view.

        Args:
            source_datasetidentifier (str): Source dataset identifier.
            temp_view_name (str): Temporary view containing the source data.
        """
        try:
            destination_path, database_name, table_name = self.get_destination_details(source_datasetidentifier)
            self.ensure_path_exists(destination_path)

            df = self.spark.table(temp_view_name)
            schema_str = ",\n    ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields])

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                {schema_str}
            )
            USING DELTA
            LOCATION 'dbfs:{destination_path}/'
            """
            self.logger.log_sql_query(create_table_sql, level="info")

            self.spark.sql(create_table_sql)
            self.logger.log_info(f"✅ Table `{database_name}.{table_name}` is ready.")

        except Exception as e:
            self.logger.log_error(f"Error creating table: {e}")
            raise

    def delete_or_filter_duplicates(self, temp_view_name: str, key_columns: List[str]) -> str:
        """
        Deletes or filters out duplicate records from the temporary view based on key columns.
        
        Args:
            temp_view_name (str): Name of the temporary view.
            key_columns (List[str]): Key columns.
        
        Returns:
            str: New temporary view name with duplicates removed.
        """
        key_columns_str = ', '.join(key_columns)
        filtered_view_name = f"{temp_view_name}_deduped"
        dedupe_query = f"""
        WITH ranked_data AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_columns_str} ORDER BY {key_columns_str}) AS row_num
            FROM {temp_view_name}
        )
        SELECT * FROM ranked_data WHERE row_num = 1
        """
        self.logger.log_block("Deduplicate Query", sql_query=dedupe_query, level="debug")
        try:
            deduped_df = self.spark.sql(dedupe_query)
            deduped_df.createOrReplaceTempView(filtered_view_name)
            self.logger.log_message(f"Created new view without duplicates: {filtered_view_name}", level="info")
            return filtered_view_name
        except Exception as e:
            self.logger.log_message(f"Error during duplicate filtering: {e}", level="error")
            raise

    def validate_and_create_duplicate_view(self, temp_view_name: str, key_columns: List[str], remove_duplicates: bool = False) -> str:
        """
        Ensures duplicates are handled properly.
        """
        key_columns_str = ', '.join(key_columns)
        duplicate_keys_query = f"""
        SELECT {key_columns_str}, COUNT(*) AS duplicate_count
        FROM {temp_view_name}
        GROUP BY {key_columns_str}
        HAVING COUNT(*) > 1
        """
        
        duplicates_df = self.spark.sql(duplicate_keys_query)

        if duplicates_df.count() > 0:
            duplicates_view_name = f"view_duplicates_{temp_view_name}"
            duplicates_df.createOrReplaceTempView(duplicates_view_name)
            self.logger.log_message(f"Duplicate records found. View created: {duplicates_view_name}", level="info")

            if remove_duplicates:
                return self.delete_or_filter_duplicates(temp_view_name, key_columns)  # ✅ Returns string
            else:
                raise ValueError(f"Duplicate keys found in {temp_view_name}. Merge operation aborted.")
        
        self.logger.log_message(f"No duplicates found in {temp_view_name}.", level="info")
        return temp_view_name  # ✅ Ensure returning a string

    def generate_merge_sql(self, temp_view_name: str, database_name: str, table_name: str, key_columns: List[str]) -> str:
        """
        Constructs the MERGE SQL query for updating and inserting records (excluding DELETE logic).
        """
        try:
            self.logger.log_start("generate_merge_sql")

            # ✅ Ensure temp_view_name is a string, not a list
            if isinstance(temp_view_name, list):
                raise ValueError(f"Expected temp_view_name as a string, but got a list: {temp_view_name}")

            df = self.spark.table(temp_view_name)  # <-- Error occurs here if temp_view_name is incorrect
            all_columns = [col for col in df.columns if col not in key_columns and col != 'CreatedDate']

            match_sql = ' AND '.join([f"s.{col.strip()} = t.{col.strip()}" for col in key_columns])
            update_sql = ', '.join([f"t.{col.strip()} = s.{col.strip()}" for col in all_columns])
            insert_columns = ', '.join([f"{col.strip()}" for col in key_columns + all_columns + ['CreatedDate']])
            insert_values = ', '.join([f"s.{col.strip()}" for col in key_columns + all_columns + ['CreatedDate']])

            merge_sql = f"""
            MERGE INTO {database_name}.{table_name} AS t
            USING {temp_view_name} AS s
            ON {match_sql}
            WHEN MATCHED THEN
            UPDATE SET {update_sql}
            WHEN NOT MATCHED THEN
            INSERT ({insert_columns}) VALUES ({insert_values})
            """

            self.logger.log_block("MERGE SQL Query", sql_query=merge_sql, level="info")
            return merge_sql

        except Exception as e:
            self.logger.log_message(f"Error generating MERGE SQL: {e}", level="error")
            raise

        finally:
            self.logger.log_end("generate_merge_sql")

    def generate_delete_sql(self, temp_view_name: str, database_name: str, table_name: str, key_columns: List[str]) -> str:
        """
        Constructs the DELETE SQL query to remove records from the target table that are not in the source view.
        
        Args:
            temp_view_name (str): Name of the temporary view.
            database_name (str): Target database.
            table_name (str): Target table.
            key_columns (List[str]): Key columns.
        
        Returns:
            str: The DELETE SQL query.
        """
        key_conditions = ' AND '.join([f"t.{col.strip()} = s.{col.strip()}" for col in key_columns])
        delete_sql = f"""
        DELETE FROM {database_name}.{table_name} AS t
        WHERE NOT EXISTS (
            SELECT 1 FROM {temp_view_name} AS s
            WHERE {key_conditions}
        )
        """
        self.logger.log_block("DELETE SQL Query", sql_query=delete_sql, level="info")
        return delete_sql

    def execute_merge_and_get_post_version(self, temp_view_name: str, database_name: str, table_name: str, merge_sql: str, pre_merge_version: int) -> int:
        """
        Executes the MERGE operation and calculates the post-merge version.
        
        Args:
            temp_view_name (str): Name of the temporary view.
            database_name (str): Target database.
            table_name (str): Target table.
            merge_sql (str): The MERGE SQL query.
            pre_merge_version (int): Pre-merge Delta table version.
        
        Returns:
            int: The post-merge version.
        """
        try:
            self.spark.sql(merge_sql)
            self.logger.log_message("Data merged successfully.", level="info")
            post_merge_version = pre_merge_version + 1
            return post_merge_version
        except Exception as e:
            self.logger.log_message(f"Error during data merge: {e}", level="error")
            raise

    def display_newly_merged_data(self, database_name: str, table_name: str, pre_merge_version: int, post_merge_version: int):
        """
        Retrieves the data that was newly merged by comparing table versions.
        
        Args:
            database_name (str): Target database.
            table_name (str): Target table.
            pre_merge_version (int): Pre-merge version.
            post_merge_version (int): Post-merge version.
        
        Returns:
            DataFrame: DataFrame containing the newly merged records.
        """
        merged_data_sql = f"""
        SELECT * FROM {database_name}.{table_name} VERSION AS OF {post_merge_version}
        EXCEPT
        SELECT * FROM {database_name}.{table_name} VERSION AS OF {pre_merge_version}
        """
        try:
            merged_data_df = self.spark.sql(merged_data_sql)
            if merged_data_df.count() == 0:
                self.logger.log_message("No newly merged data.", level="info")
            return merged_data_df
        except Exception as e:
            self.logger.log_message(f"Error retrieving merged data: {e}", level="error")
            raise

    def get_pre_merge_version(self, database_name: str, table_name: str) -> int:
        """
        Retrieves the current version of the Delta table before the merge.

        Args:
            database_name (str): Target database.
            table_name (str): Target table.

        Returns:
            int: The pre-merge version.
        """
        try:
            self.logger.log_message(f"Retrieving pre-merge version for {database_name}.{table_name}", level="info")
            
            # Ensure correct database and table reference
            query = f"DESCRIBE HISTORY {database_name}.{table_name} LIMIT 1"
            
            pre_merge_version = self.spark.sql(query).select("version").collect()[0][0]
            return pre_merge_version

        except Exception as e:
            self.logger.log_message(f"Error retrieving pre-merge version: {e}", level="error")
            raise

    def merge_table(self, source_datasetidentifier: str, temp_view_name: str, key_columns: List[str]):
        """
        Merges new data into an existing Delta table.

        Args:
            source_datasetidentifier (str): Source dataset identifier.
            temp_view_name (str): Temporary view containing new data.
            key_columns (List[str]): Key columns for identifying existing records.
        """
        try:
            destination_path, database_name, table_name = self.get_destination_details(source_datasetidentifier)
            self.ensure_path_exists(destination_path)

            merge_sql = self.generate_merge_sql(temp_view_name, database_name, table_name, key_columns)
            self.spark.sql(merge_sql)
            self.logger.log_info(f"✅ Data merged successfully into `{database_name}.{table_name}`")

            delete_sql = self.generate_delete_sql(temp_view_name, database_name, table_name, key_columns)
            self.spark.sql(delete_sql)
            self.logger.log_info(f"✅ Deleted records no longer present in `{temp_view_name}` from `{database_name}.{table_name}`")

        except Exception as e:
            self.logger.log_error(f"Error merging table: {e}")
            raise

    def manage_tables(self, dataframes: Dict[str, any], remove_duplicates: bool = False):
        """
        Orchestrates table creation and merge operations for multiple DataFrames.
        
        For each DataFrame:
          - A temporary view is created.
          - The table is created (if not already present).
          - Then, new data is merged into the table.
        
        Args:
            dataframes (dict): Dictionary mapping DataFrame names to DataFrame objects.
            remove_duplicates (bool): Whether to automatically remove duplicates during merge.
        """
        key_columns_dict = {
            "pipelines_df": ["PipelineId"],
            "activities_df": ["ActivityId", "ParentId"],
            "dependencies_df": ["DependencySourceId", "DependencyTargetId"]
        }
        for df_name, df in dataframes.items():
            temp_view_name = f"view_{df_name}"
            # Create or replace a temporary view for the DataFrame.
            df.createOrReplaceTempView(temp_view_name)
            source_datasetidentifier = f"data_quality__adf_{df_name[:-3]}" if df_name.endswith("_df") else df_name
            summary_lines = [
                f"Temp View: {temp_view_name}",
                f"Dataset Identifier: {source_datasetidentifier}"
            ]
            self.logger.log_block(f"Processing {df_name}", summary_lines, level="info")
            # First, attempt to create the table.
            self.create_table(source_datasetidentifier, temp_view_name)
            # Then, merge new data into the table.
            if df_name in key_columns_dict:
                key_columns = key_columns_dict[df_name]
                self.merge_table(source_datasetidentifier, temp_view_name, key_columns)