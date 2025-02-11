import unittest
from adf_json_processor.utils.logger import Logger

class TestLogger(unittest.TestCase):
    """
    Unit tests for the Logger class.
    """

    def setUp(self):
        """
        Setup the logger before each test.
        """
        self.logger = Logger(debug=True)

    def test_log_message_info(self):
        """
        Test logging an informational message.
        """
        self.logger.log_info("Test Info Message")

    def test_log_message_debug(self):
        """
        Test logging a debug message.
        """
        self.logger.log_debug("Test Debug Message")

    def test_log_error_raises_exception(self):
        """
        Test that logging an error raises a RuntimeError.
        """
        with self.assertRaises(RuntimeError):
            self.logger.log_error("Test Error Message")

if __name__ == "__main__":
    unittest.main()