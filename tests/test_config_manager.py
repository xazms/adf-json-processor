import unittest
from adf_json_processor.config.config_manager import ConfigManager

class TestConfigManager(unittest.TestCase):
    def setUp(self):
        self.config = ConfigManager()

    def test_load_config(self):
        self.assertIsNotNone(self.config.load(), "Config should be loaded properly.")

if __name__ == "__main__":
    unittest.main()
