from adf_json_processor.storage.writer import get_destination_path_extended, get_databricks_table_info_extended
from pyspark.sql import SparkSession
from typing import List, Dict, Tuple
from adf_json_processor.utils.logger import Logger

# ==============================================================================
# TableManager Class
# ==============================================================================
class TableManager:
    """
    Manages the creation, validation, and merging of Databricks Delta tables.

    Features:
      1. create_table: Creates a new Delta table from a temporary view.
      2. merge_table: Merges new data from a temporary view into an existing Delta table.
      3. delete_or_filter_duplicates: Removes duplicate records before merging.
      4. validate_and_create_duplicate_view: Ensures duplicate checks before merging.
      5. generate_merge_sql: Constructs an optimized MERGE SQL query.
      6. generate_delete_sql: Constructs a DELETE SQL query for stale records.
      7. execute_merge_and_get_post_version: Runs the merge and retrieves the updated version.
    """

    def __init__(self, spark: SparkSession, dbutils, destination_environment: str, logger):
        """
        Initialize the TableManager.

        Args:
            spark (SparkSession): Active Spark session.
            dbutils: Databricks utilities.
            destination_environment (str): Target storage environment.
            logger: Custom logger instance.
        """
        self.spark = spark
        self.dbutils = dbutils
        self.destination_environment = destination_environment
        self.logger = logger
        self.logger.log_info("TableManager initialized.")

    def _handle_error(self, error: Exception, message: str):
        """Handles errors uniformly across all functions."""
        self.logger.log_error(f"{message} | Error: {error}")
        raise

    def get_destination_details(self, source_datasetidentifier: str) -> Tuple[str, str, str]:
        """Retrieves destination path, database name, and table name."""
        try:
            destination_path = get_destination_path_extended(self.destination_environment, source_datasetidentifier)
            database_name, table_name = get_databricks_table_info_extended(self.destination_environment, source_datasetidentifier)
            return destination_path, database_name, table_name
        except Exception as e:
            self._handle_error(e, "Error retrieving destination details")

    def ensure_path_exists(self, destination_path: str):
        """Ensures that the destination path exists or creates it."""
        try:
            self.dbutils.fs.ls(destination_path)
            self.logger.log_info(f"Path exists: {destination_path}")
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                self.dbutils.fs.mkdirs(destination_path)
                self.logger.log_info(f"Created path: {destination_path}")
            else:
                self._handle_error(e, "Error ensuring path exists")

    def check_if_table_exists(self, database_name: str, table_name: str) -> bool:
        """Checks if a Delta table exists in the given database."""
        try:
            exists = any(row["tableName"] == table_name for row in self.spark.sql(f"SHOW TABLES IN {database_name}").collect())
            return exists
        except Exception as e:
            self._handle_error(e, f"Error checking table existence for {database_name}.{table_name}")

    def create_table(self, source_datasetidentifier: str, temp_view_name: str):
        """Creates a new Delta table if it doesn't already exist."""
        self.logger.log_start("create_table")
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
            self.logger.log_block("Executing Create SQL Query", sql_query=create_table_sql)

            if not self.check_if_table_exists(database_name, table_name):
                self.spark.sql(create_table_sql)
                df.write.format("delta").mode("overwrite").save(destination_path)
                self.logger.log_info(f"✅ Table `{database_name}.{table_name}` created and initialized.")
            else:
                self.logger.log_info(f"✅ Table `{database_name}.{table_name}` already exists.")

        except Exception as e:
            self._handle_error(e, f"Error creating table `{database_name}.{table_name}`")
        finally:
            self.logger.log_end("create_table", success=True)

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
        self.logger.log_sql_query(dedupe_query)

        try:
            deduped_df = self.spark.sql(dedupe_query)
            deduped_df.createOrReplaceTempView(filtered_view_name)
            self.logger.log_info(f"✅ Created new view without duplicates: `{filtered_view_name}`")
            return filtered_view_name
        except Exception as e:
            self._handle_error(e, "Error during duplicate filtering")

    def validate_and_create_duplicate_view(self, temp_view_name: str, key_columns: List[str], remove_duplicates: bool = False) -> str:
        """Validates the temporary view for duplicate records based on key columns."""
        key_columns_str = ', '.join(key_columns)
        duplicate_keys_query = f"""
        SELECT {key_columns_str}, COUNT(*) AS duplicate_count
        FROM {temp_view_name}
        GROUP BY {key_columns_str}
        HAVING COUNT(*) > 1
        """
        self.logger.log_block("Executing Duplicate SQL Query", sql_query=duplicate_keys_query)

        try:
            duplicates_df = self.spark.sql(duplicate_keys_query)

            if duplicates_df.count() > 0:
                self.logger.log_warning(f"⚠️ Duplicates found in `{temp_view_name}`.")
                duplicates_view_name = f"view_duplicates_{temp_view_name}"
                duplicates_df.createOrReplaceTempView(duplicates_view_name)

                if remove_duplicates:
                    self.logger.log_info(f"✅ Duplicates removed from `{temp_view_name}`.")
                    return self.delete_or_filter_duplicates(temp_view_name, key_columns)
                else:
                    raise ValueError(f"Duplicate keys found in `{temp_view_name}`. Merge operation aborted.")
            else:
                self.logger.log_info(f"✅ No duplicates found in `{temp_view_name}`.")
                return temp_view_name

        except Exception as e:
            self._handle_error(e, "Error validating duplicates")

    def generate_merge_sql(self, temp_view_name: str, database_name: str, table_name: str, key_columns: List[str]) -> str:
        """
        Generates an optimized MERGE SQL query for Delta table updates and inserts.
        """
        df = self.spark.table(temp_view_name)  
        non_key_columns = [col for col in df.columns if col not in key_columns]

        match_condition = ' AND '.join([f"s.{col} = t.{col}" for col in key_columns])
        update_condition = ' OR '.join([f"t.{col} <> s.{col}" for col in non_key_columns])
        update_statement = ', '.join([f"t.{col} = s.{col}" for col in non_key_columns])
        insert_columns = ', '.join(df.columns)
        insert_values = ', '.join([f"s.{col}" for col in df.columns])

        merge_sql = f"""
        MERGE INTO {database_name}.{table_name} AS t
        USING {temp_view_name} AS s
        ON {match_condition}
        WHEN MATCHED AND ({update_condition}) THEN UPDATE SET {update_statement}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
        return merge_sql

    def generate_delete_sql(self, temp_view_name: str, database_name: str, table_name: str, key_columns: List[str]) -> str:
        """
        Generates a DELETE SQL query to remove records that no longer exist in the source view.
        """
        key_conditions = ' AND '.join([f"t.{col} = s.{col}" for col in key_columns])
        return f"""
        DELETE FROM {database_name}.{table_name} AS t
        WHERE NOT EXISTS (
            SELECT 1 FROM {temp_view_name} AS s WHERE {key_conditions}
        )
        """

    def execute_merge_and_get_post_version(self, merge_sql: str, database_name: str, table_name: str, pre_merge_version: int) -> int:
        """Executes the MERGE operation and calculates the post-merge version."""
        try:
            self.spark.sql(merge_sql)
            post_merge_version = pre_merge_version + 1
            self.logger.log_info(f"✅ Merge completed for `{database_name}.{table_name}`.")
            return post_merge_version
        except Exception as e:
            self._handle_error(e, "Error executing merge")

    def display_newly_merged_data(self, database_name: str, table_name: str, pre_merge_version: int, post_merge_version: int):
        """Displays only the newly inserted or updated records."""
        merged_data_sql = f"""
        SELECT * FROM {database_name}.{table_name} VERSION AS OF {post_merge_version}
        EXCEPT
        SELECT * FROM {database_name}.{table_name} VERSION AS OF {pre_merge_version}
        """
        try:
            merged_data_df = self.spark.sql(merged_data_sql)
            if merged_data_df.count() > 0:
                self.logger.log_info("Displaying newly merged records:")
                #display(merged_data_df.limit(10))
        except Exception as e:
            self._handle_error(e, "Error displaying merged data")

    def get_inserted_count(self, database_name: str, table_name: str, temp_view_name: str, key_columns: List[str]) -> int:
        """
        Computes the number of inserted records: rows that exist in the new data but not in the target table.
        """
        query = f"""
            SELECT COUNT(*) AS cnt FROM {temp_view_name} s
            LEFT JOIN {database_name}.{table_name} t
            ON {' AND '.join([f's.{col} = t.{col}' for col in key_columns])}
            WHERE t.{key_columns[0]} IS NULL
        """
        return self.spark.sql(query).collect()[0]["cnt"]

    def get_deleted_count(self, database_name: str, table_name: str, temp_view_name: str, key_columns: List[str]) -> int:
        """
        Computes the number of deleted records: rows that exist in the target table but not in the new data.
        """
        query = f"""
            SELECT COUNT(*) AS cnt FROM {database_name}.{table_name} t
            LEFT JOIN {temp_view_name} s
            ON {' AND '.join([f's.{col} = t.{col}' for col in key_columns])}
            WHERE s.{key_columns[0]} IS NULL
        """
        return self.spark.sql(query).collect()[0]["cnt"]

    def get_updated_count(self, database_name: str, table_name: str, temp_view_name: str) -> int:
        """
        Computes the number of updated records: rows where any value (excluding keys) has changed.
        """
        query = f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT * FROM {database_name}.{table_name}
                EXCEPT
                SELECT * FROM {temp_view_name}
            ) AS changed_records
        """
        return self.spark.sql(query).collect()[0]["cnt"]

    def compute_change_counts(self, database_name: str, table_name: str, temp_view_name: str, key_columns: List[str]) -> Tuple[int, int, int]:
        """
        Computes the number of inserted, updated, and deleted records using modular functions.
        """
        inserted_count = self.get_inserted_count(database_name, table_name, temp_view_name, key_columns)
        deleted_count = self.get_deleted_count(database_name, table_name, temp_view_name, key_columns)
        updated_count = self.get_updated_count(database_name, table_name, temp_view_name)

        return inserted_count, updated_count, deleted_count

    def get_pre_merge_version(self, database_name: str, table_name: str) -> int:
        """Retrieves the latest available Delta table version before merging."""
        try:
            version_query = f"DESCRIBE HISTORY {database_name}.{table_name} LIMIT 1"
            versions_df = self.spark.sql(version_query)

            if versions_df.count() > 0:
                latest_version = versions_df.select("version").collect()[0]["version"]
                self.logger.log_debug(f"🔄 Latest Delta version for `{table_name}`: {latest_version}")
                return latest_version
            else:
                self.logger.log_warning(f"⚠️ No history found for `{database_name}.{table_name}`, assuming no previous merges.")
                return None  # ✅ Return None instead of 0 to handle fresh tables safely

        except Exception as e:
            self._handle_error(e, f"Error retrieving pre-merge version for `{database_name}.{table_name}`")

    def merge_table(self, source_datasetidentifier: str, temp_view_name: str, key_columns: List[str]) -> Tuple[Dict[str, int], Dict[str, str]]:
        """
        Merges new data into the existing Delta table and tracks inserted, updated, and deleted records.
        """

        database_name, table_name = get_databricks_table_info_extended(self.destination_environment, source_datasetidentifier)

        # ✅ Retrieve the last table version before merging
        pre_merge_version = self.get_pre_merge_version(database_name, table_name)

        # ✅ Generate MERGE SQL
        merge_sql = self.generate_merge_sql(temp_view_name, database_name, table_name, key_columns)

        # ✅ Log MERGE SQL using the correct `log_block` format
        self.logger.log_block("Executing MERGE SQL Query", sql_query=merge_sql)
        
        # ✅ Execute MERGE SQL
        self.spark.sql(merge_sql)

        # ✅ Generate DELETE SQL
        delete_sql = self.generate_delete_sql(temp_view_name, database_name, table_name, key_columns)

        # ✅ Log DELETE SQL using the correct `log_block` format
        self.logger.log_block("Executing DELETE SQL Query", sql_query=delete_sql)

        # ✅ Execute DELETE SQL
        self.spark.sql(delete_sql)

        # ✅ Compute the number of inserted, updated, and deleted records
        inserted_count, updated_count, deleted_count = self.compute_change_counts(database_name, table_name, temp_view_name, key_columns)

        # ✅ Log merge summary
        self.logger.log_block("Merge Summary", [
            f"📥 Inserted: {inserted_count}",
            f"✏️ Updated: {updated_count}",
            f"🗑️ Deleted: {deleted_count}"
        ])

        return {"inserted_count": inserted_count, "updated_count": updated_count, "deleted_count": deleted_count}, {}

    def merge_all_tables(self, dataframes: Dict[str, any]) -> Tuple[Dict[str, any], Dict[str, Dict[str, str]]]:
        """Orchestrates merging all tables and returns both summary and view references for display."""
        key_columns_dict = {
            "pipelines_df": ["PipelineId"],
            "activities_df": ["ActivityId", "ParentId"],
            "dependencies_df": ["DependencySourceId", "DependencyTargetId"]
        }

        total_summary = {
            "inserted_count": 0,
            "updated_count": 0,
            "deleted_count": 0
        }

        final_views = {}

        for df_name, df in dataframes.items():
            self.logger.log_debug(f"🔄 Processing `{df_name}`")
            temp_view_name = f"view_{df_name}"
            df.createOrReplaceTempView(temp_view_name)
            source_datasetidentifier = f"data_quality__adf_{df_name[:-3]}" if df_name.endswith("_df") else df_name
            key_columns = key_columns_dict.get(df_name, [])

            # ✅ Create table if not exists
            self.create_table(source_datasetidentifier, temp_view_name)

            # ✅ Merge table and retrieve affected record summary
            merge_summary, table_views = self.merge_table(source_datasetidentifier, temp_view_name, key_columns)

            # ✅ Accumulate counts
            for action in ["inserted_count", "updated_count", "deleted_count"]:
                total_summary[action] += merge_summary[action]

            # ✅ Store only non-empty views
            final_views[df_name] = {action: view for action, view in table_views.items() if view}

        # ✅ Log final summary
        summary_logs = [
            f"📥 Total Inserted: {total_summary['inserted_count']}" if total_summary["inserted_count"] > 0 else "✅ No new records inserted.",
            f"✏️ Total Updated: {total_summary['updated_count']}" if total_summary["updated_count"] > 0 else "✅ No records updated.",
            f"🗑️ Total Deleted: {total_summary['deleted_count']}" if total_summary["deleted_count"] > 0 else "✅ No records deleted."
        ]

        self.logger.log_block("📊 Final Summary Across All Tables", summary_logs)

        return total_summary, final_views

    def manage_tables(self, dataframes: Dict[str, any]):
        """Manages table creation and merging."""
        key_columns_dict = {
            "pipelines_df": ["PipelineId"],
            "activities_df": ["ActivityId", "ParentId"],
            "dependencies_df": ["DependencySourceId", "DependencyTargetId"]
        }

        for df_name, df in dataframes.items():
            temp_view_name = f"view_{df_name}"
            df.createOrReplaceTempView(temp_view_name)
            source_datasetidentifier = f"data_quality__adf_{df_name[:-3]}" if df_name.endswith("_df") else df_name
            key_columns = key_columns_dict.get(df_name, [])

            self.logger.log_block(f"Processing `{df_name}`", [
                f"Temp View: {temp_view_name}",
                f"Dataset Identifier: {source_datasetidentifier}",
                f"Key Columns: {key_columns}"
            ])

            # ✅ Validate duplicates before proceeding
            validated_view = self.validate_and_create_duplicate_view(temp_view_name, key_columns, remove_duplicates=True)

            # ✅ Create table if not exists
            #self.create_table(source_datasetidentifier, validated_view)

            # ✅ Merge table with validated data
            #self.merge_table(source_datasetidentifier, validated_view, key_columns)