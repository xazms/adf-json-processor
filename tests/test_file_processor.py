import unittest
from adf_json_processor.processing.file_processor import FileProcessor

class TestFileProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = FileProcessor()

    def test_process_files(self):
        result = self.processor.process_files("sample.json")
        self.assertTrue("pipelines" in result, "Output should contain pipelines.")

if __name__ == "__main__":
    unittest.main()
