import unittest
from adf_json_processor.catalog.catalog_utils import CatalogUtils

class TestCatalogUtils(unittest.TestCase):
    def setUp(self):
        self.utils = CatalogUtils()

    def test_get_metadata(self):
        metadata = self.utils.get_metadata("test_table")
        self.assertIsInstance(metadata, dict, "Metadata should be a dictionary.")

if __name__ == "__main__":
    unittest.main()
