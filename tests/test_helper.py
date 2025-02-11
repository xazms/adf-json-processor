import unittest
from adf_json_processor.utils.helper import Helper

class TestHelper(unittest.TestCase):
    def setUp(self):
        self.helper = Helper()

    def test_generate_hash_key(self):
        hash_key = self.helper.generate_hash_key("test")
        self.assertEqual(len(hash_key), 64, "Hash key should be 64 characters.")

if __name__ == "__main__":
    unittest.main()
