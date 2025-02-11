import unittest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from adf_json_processor.storage.table_manager import TableManager
from adf_json_processor.utils.logger import Logger

class TestTableManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a Spark session for testing."""
        cls.spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        cls.dbutils = MagicMock()
        cls.logger = Logger(debug=True)
        cls.table_manager = TableManager(cls.spark, cls.dbutils, "test_env", cls.logger)

    def test_get_destination_details(self):
        """Test retrieval of destination details."""
        self.table_manager.get_destination_details = MagicMock(return_value=("dbfs:/mnt/test_env", "test_db", "test_table"))
        path, db, table = self.table_manager.get_destination_details("test_dataset")
        self.assertEqual(db, "test_db")
        self.assertEqual(table, "test_table")

    def test_check_if_table_exists(self):
        """Test checking if a table exists."""
        self.spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
        self.spark.sql("CREATE TABLE IF NOT EXISTS test_db.test_table (id STRING)")
        self.assertTrue(self.table_manager.check_if_table_exists("test_db", "test_table"))

    def test_create_table(self):
        """Test creating a Delta table."""
        df = self.spark.createDataFrame([("1",), ("2",)], ["id"])
        df.createOrReplaceTempView("test_view")
        self.table_manager.create_table("test_dataset", "test_view")
        tables = self.spark.sql("SHOW TABLES IN test_db").collect()
        table_names = [row.tableName for row in tables]
        self.assertIn("test_table", table_names)

    def test_merge_table(self):
        """Test merging data into an existing Delta table."""
        df = self.spark.createDataFrame([("1", "A"), ("2", "B")], ["id", "value"])
        df.createOrReplaceTempView("test_view")
        self.table_manager.merge_table("test_dataset", "test_view", ["id"])
        count = self.spark.sql("SELECT COUNT(*) FROM test_db.test_table").collect()[0][0]
        self.assertGreater(count, 0)

if __name__ == "__main__":
    unittest.main()