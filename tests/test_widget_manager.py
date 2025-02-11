import unittest
from unittest.mock import MagicMock
from adf_json_processor.utils.widget_manager import WidgetManager
from custom_utils.logging.logger import Logger

class TestWidgetManager(unittest.TestCase):
    """
    Unit tests for the WidgetManager class.
    """

    def setUp(self):
        """
        Setup a mock dbutils instance before each test.
        """
        self.mock_dbutils = MagicMock()
        self.mock_dbutils.widgets.text = MagicMock()
        self.mock_dbutils.widgets.removeAll = MagicMock()
        self.logger = Logger(debug=True)
        self.widget_manager = WidgetManager(self.mock_dbutils, self.logger)

    def test_initialize_widgets(self):
        """
        Test widget initialization to ensure correct setup.
        """
        self.widget_manager.initialize_widgets()
        self.assertEqual(self.mock_dbutils.widgets.text.call_count, 6)  # 6 widgets should be created

    def test_clear_widgets(self):
        """
        Test that existing widgets are cleared before re-initialization.
        """
        self.widget_manager.clear_widgets()
        self.mock_dbutils.widgets.removeAll.assert_called_once()

    def test_initialize_widgets_error_handling(self):
        """
        Test error handling when widget initialization fails.
        """
        self.mock_dbutils.widgets.text.side_effect = Exception("Widget creation error")
        with self.assertRaises(RuntimeError):
            self.widget_manager.initialize_widgets()

if __name__ == "__main__":
    unittest.main()