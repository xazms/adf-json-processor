import sqlparse
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict, Optional
from adf_json_processor.utils.logger import Logger
from pyspark.sql import functions as F

class DataManagementHandler:
    def __init__(self, logger: Optional[Logger] = None, debug: bool = False):
        self.logger = logger if logger else Logger(debug=debug)
        self.debug = debug

    def _format_sql_query(self, query: str) -> str:
        """Formats SQL queries using sqlparse for readability."""
        return sqlparse.format(query, reindent=True, keyword_case="upper")

    def log_sql_query(self, title: str, query: str):
        """Logs a formatted SQL query with a clear 'Executing SQL' message."""
        formatted_query = self._format_sql_query(query)
        self.logger.log_block(f"{title} - Executing SQL", [formatted_query])

    def get_destination_details(self, spark: SparkSession, destination_storage_account: str, df_name: str):
        """Retrieve destination details for each DataFrame."""
        destination_path = f"/mnt/{destination_storage_account}/data_quality__adf_{df_name[:-3]}"
        database_name = destination_storage_account
        table_name = f"data_quality__adf_{df_name[:-3]}"
        
        self.logger.log_block("Destination Details", [
            f"Destination Path: {destination_path}",
            f"Database: {database_name}",
            f"Table: {table_name}"
        ])
        
        return destination_path, database_name, table_name

    def ensure_path_exists(self, dbutils, destination_path: str):
        """Ensure that the destination path exists in DBFS."""
        try:
            dbutils.fs.ls(destination_path)
            self.logger.log_block("Path Validation", [f"Path already exists: {destination_path}"])
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                dbutils.fs.mkdirs(destination_path)
                self.logger.log_block("Path Validation", [f"Path did not exist. Created path: {destination_path}"])
            else:
                self.logger.log_message(f"Error while ensuring path exists: {e}", level="error")
                raise

    def create_or_replace_table(self, spark: SparkSession, database_name: str, table_name: str, destination_path: str, temp_view_name: str):
        """Creates or replaces a Databricks Delta table."""
        df = spark.table(temp_view_name)
        schema_str = ",\n    ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields])
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
            {schema_str}
        )
        USING DELTA
        LOCATION 'dbfs:{destination_path}/'
        """
        self.log_sql_query("Table Creation Process", create_table_sql)

        # Check if the table already exists and log
        if self.check_if_table_exists(spark, database_name, table_name):
            self.logger.log_block("Table Validation", [f"Table already exists: {database_name}.{table_name}"])
        else:
            spark.sql(create_table_sql)
            df.write.format("delta").mode("overwrite").save(destination_path)
            self.logger.log_message(f"Table {database_name}.{table_name} created and data written.", level="info")

    def check_if_table_exists(self, spark: SparkSession, database_name: str, table_name: str) -> bool:
        """Checks if a Databricks Delta table exists in the specified database."""
        table_check = spark.sql(f"SHOW TABLES IN {database_name}").collect()
        return any(row["tableName"] == table_name for row in table_check)

    def generate_merge_sql(self, spark: SparkSession, temp_view_name: str, database_name: str, table_name: str, key_columns: List[str]) -> str:
        """Generates the SQL query for the MERGE operation."""
        target_columns = [field.name for field in spark.table(f"{database_name}.{table_name}").schema]
        all_columns = [col for col in spark.table(temp_view_name).columns if col in target_columns and col not in key_columns]
        
        match_sql = ' AND '.join([f"s.`{col}` = t.`{col}`" for col in key_columns])
        update_sql = ',\n        '.join([f"t.`{col}` = s.`{col}`" for col in all_columns])
        insert_columns = key_columns + all_columns
        insert_values = [f"s.`{col}`" for col in insert_columns]

        merge_sql = f"""
        MERGE INTO {database_name}.{table_name} AS t
        USING {temp_view_name} AS s
        ON {match_sql}
        WHEN MATCHED THEN
            UPDATE SET
                {update_sql}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join([f'`{col}`' for col in insert_columns])})
            VALUES ({', '.join(insert_values)})
        """
        self.log_sql_query("Data Merge", merge_sql)
        return merge_sql

    def capture_table_snapshot(self, spark: SparkSession, database_name: str, table_name: str) -> DataFrame:
        """Captures a snapshot of the current state of the Delta table."""
        return spark.sql(f"SELECT * FROM {database_name}.{table_name}")

    def log_merge_changes(self, before_df: DataFrame, after_df: DataFrame, key_columns: List[str]):
        """Logs counts of rows that were inserted, updated, or deleted, including when no changes occur."""
        # Identify inserted and deleted rows
        inserted_rows = after_df.join(before_df, key_columns, "left_anti")
        deleted_rows = before_df.join(after_df, key_columns, "left_anti")

        # Identify updated rows by comparing columns other than the key columns
        non_key_columns = [col for col in after_df.columns if col not in key_columns]
        updated_rows = (
            after_df.alias("after")
            .join(before_df.alias("before"), key_columns, "inner")
            .where(" OR ".join([f"after.{col} != before.{col}" for col in non_key_columns]))
        )

        # Log counts for each change type
        inserted_count = inserted_rows.count()
        updated_count = updated_rows.count()
        deleted_count = deleted_rows.count()

        # Log the counts, even if they are zero
        self.logger.log_message(f"Inserted Rows: {inserted_count}", level="info")
        self.logger.log_message(f"Updated Rows: {updated_count}", level="info")
        self.logger.log_message(f"Deleted Rows: {deleted_count}", level="info")

    def delete_missing_records(self, spark: SparkSession, database_name: str, table_name: str, temp_view_name: str, key_columns: List[str]):
        """Deletes records from the target table that are not present in the source DataFrame, and logs the deletion count."""
        match_sql = ' AND '.join([f"t.`{col}` = s.`{col}`" for col in key_columns])
        delete_sql = f"""
        DELETE FROM {database_name}.{table_name} AS t
        WHERE NOT EXISTS (
            SELECT 1 FROM {temp_view_name} AS s
            WHERE {match_sql}
        )
        """
        # Count records before and after deletion
        initial_count = spark.table(f"{database_name}.{table_name}").count()

        # Execute delete query
        self.log_sql_query("Delete Missing Records", delete_sql)
        spark.sql(delete_sql)

        # Calculate number of deleted records
        final_count = spark.table(f"{database_name}.{table_name}").count()
        deleted_count = initial_count - final_count

        # Log deleted count if there are deleted records
        if deleted_count > 0:
            self.logger.log_message(
                f"Records missing from {temp_view_name} have been deleted from {database_name}.{table_name}. Deleted Rows: {deleted_count}",
                level="info"
            )

    def execute_merge(self, spark: SparkSession, database_name: str, table_name: str, temp_view_name: str, key_columns: List[str]):
        """Executes the MERGE operation with deletions and logs changes."""
        # Capture the snapshot of the table before the merge
        before_df = self.capture_table_snapshot(spark, database_name, table_name)

        # Generate and execute the MERGE SQL
        merge_sql = self.generate_merge_sql(spark, temp_view_name, database_name, table_name, key_columns)
        spark.sql(merge_sql)
        self.logger.log_message(f"Data merged into {database_name}.{table_name} using SQL.", level="info")

        # Perform deletion of records missing from the source DataFrame
        self.delete_missing_records(spark, database_name, table_name, temp_view_name, key_columns)

        # Capture the snapshot of the table after the merge and deletion
        after_df = self.capture_table_snapshot(spark, database_name, table_name)

        # Log detailed changes (inserts, updates, deletes)
        self.log_merge_changes(before_df, after_df, key_columns)

    def manage_data_operation(self, spark: SparkSession, dbutils, dataframes: Dict[str, DataFrame], destination_storage_account: str):
        """Manages the complete data operation for each DataFrame in `dataframes`."""
        self.logger.log_start("Data Operation Process")
        key_columns_dict = {
            "pipelines_df": ["PipelineId"],
            "activities_df": ["ActivityId", "ParentId"],
            "dependencies_df": ["DependencySourceId", "DependencyTargetId"]
        }

        try:
            for df_name, df in dataframes.items():
                temp_view_name = f"view_{df_name}"
                df.createOrReplaceTempView(temp_view_name)

                destination_path, database_name, table_name = self.get_destination_details(spark, destination_storage_account, df_name)

                self.ensure_path_exists(dbutils, destination_path)
                self.create_or_replace_table(spark, database_name, table_name, destination_path, temp_view_name)

                if df_name in key_columns_dict:
                    key_columns = key_columns_dict[df_name]
                    self.execute_merge(spark, database_name, table_name, temp_view_name, key_columns)

            self.logger.log_end("Data Operation Process", success=True, additional_message="Operation completed successfully.")
        except Exception as e:
            self.logger.log_end("Data Operation Process", success=False, additional_message=f"Error: {e}")
            self.logger.log_message(f"Error managing data operation: {e}", level="error")
            raise