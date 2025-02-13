import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from adf_json_processor.utils.logger import Logger
from adf_json_processor.utils.helper import Helper
from adf_json_processor.processing.file_processor import Processor

class TestProcessor(unittest.TestCase):
    """Unit tests for the Processor class."""

    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for testing."""
        cls.spark = SparkSession.builder.master("local").appName("TestProcessor").getOrCreate()
        cls.logger = Logger(debug=True)
        cls.helper = Helper(logger=cls.logger)
        cls.processor = Processor(logger=cls.logger, helper=cls.helper, debug=True)

    def setUp(self):
        """Setup mock dependencies before each test."""
        self.mock_file_handler = MagicMock()
        self.mock_file_handler.get_filtered_file_list.return_value = [
            {"path": "/test/file1.json"}
        ]
        self.mock_file_handler.get_adf_file_content.return_value = '{"name": "test_pipeline", "properties": {"activities": []}}'

    def test_processor_initialization(self):
        """Test that Processor initializes correctly."""
        self.assertIsInstance(self.processor, Processor)

    def test_define_schemas(self):
        """Test that schemas are defined correctly."""
        schemas = self.processor.define_schemas()
        self.assertEqual(len(schemas), 3)  # Three schemas: Pipelines, Activities, Dependencies

    @patch("adf_json_processor.utils.helper.Helper.parse_json")
    def test_process_files_single_file(self, mock_parse_json):
        """Test processing a single ADF JSON file."""
        mock_parse_json.return_value = {"name": "test_pipeline", "properties": {"activities": []}}
        combined_structure, counts = self.processor.process_files(self.mock_file_handler)
        
        self.assertEqual(counts["pipelines"], 1)
        self.assertEqual(len(combined_structure["pipelines"]), 1)
        self.assertEqual(len(combined_structure["activities"]), 0)  # No activities in mock data
        self.assertEqual(len(combined_structure["dependencies"]), 0)

    @patch("adf_json_processor.utils.helper.Helper.parse_json")
    def test_convert_to_dataframe(self, mock_parse_json):
        """Test converting processed data into Spark DataFrames."""
        mock_parse_json.return_value = {"name": "test_pipeline", "properties": {"activities": []}}
        combined_structure, _ = self.processor.process_files(self.mock_file_handler)
        dataframes, pipelines_df, activities_df, dependencies_df = self.processor.convert_to_dataframe(self.spark, combined_structure)

        self.assertEqual(pipelines_df.count(), 1)
        self.assertEqual(activities_df.count(), 0)
        self.assertEqual(dependencies_df.count(), 0)

if __name__ == "__main__":
    unittest.main()