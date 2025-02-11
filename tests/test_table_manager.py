import unittest
from adf_json_processor.storage.table_manager import TableManager

class TestTableManager(unittest.TestCase):
    def setUp(self):
        self.manager = TableManager()

    def test_create_table(self):
        result = self.manager.create_table("test_table")
        self.assertTrue(result, "Table should be created.")

if __name__ == "__main__":
    unittest.main()
