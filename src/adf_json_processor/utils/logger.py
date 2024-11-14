import logging
import functools

class Logger:
    """
    A customizable logger class that supports console and file logging, with methods to log
    information, debug messages, warnings, errors, and critical errors.
    Provides a structured format for blocks and function entry/exit logging.
    """

    def __init__(self, debug=False, log_to_file=None):
        """
        Initializes the Logger with optional file logging and debug-level control.
        
        Args:
            debug (bool): Enable debug-level logging if True.
            log_to_file (str): Optional file path to log messages to a file.
        """
        self.debug = debug
        self.logger = logging.getLogger("custom_logger")

        # Ensure no duplicate handlers
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        self.logger.setLevel(logging.DEBUG if self.debug else logging.INFO)
        self._setup_console_handler()
        if log_to_file:
            self._setup_file_handler(log_to_file)

    def _setup_console_handler(self):
        """Sets up console logging with the appropriate debug level and format."""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG if self.debug else logging.INFO)
        console_handler.setFormatter(logging.Formatter('[%(levelname)s] - %(message)s'))
        self.logger.addHandler(console_handler)

    def _setup_file_handler(self, log_to_file):
        """
        Sets up file logging if a log file path is provided.
        
        Args:
            log_to_file (str): File path for saving log messages.
        """
        file_handler = logging.FileHandler(log_to_file)
        file_handler.setFormatter(logging.Formatter('[%(levelname)s] - %(message)s'))
        self.logger.addHandler(file_handler)

    def log_message(self, message, level="info"):
        """
        Logs a message at the specified level.
        
        Args:
            message (str): Message content.
            level (str): Logging level ('debug', 'info', 'warning', 'error', 'critical').
        """
        log_func = getattr(self.logger, level.lower(), self.logger.info)
        log_func(message)

    def log_info(self, message):
        """Logs an informational message."""
        self.log_message(message, level="info")

    def log_debug(self, message):
        """Logs a debug message."""
        self.log_message(message, level="debug")

    def log_warning(self, message):
        """Logs a warning message."""
        self.log_message(message, level="warning")

    def log_error(self, message):
        """Logs an error message."""
        self.log_message(message, level="error")

    def log_critical(self, message):
        """Logs a critical message."""
        self.log_message(message, level="critical")

    def log_block(self, header, content_lines, level="info", skip_prefix_for_blank=False):
        """
        Logs a structured block of messages with a header, content lines, and separators.
        
        Args:
            header (str): The header for the block.
            content_lines (list): List of lines to display under the block header.
            level (str): The logging level for each line.
            skip_prefix_for_blank (bool): If True, skips logging level prefix for blank lines.
        """
        separator_length = 50
        separator = "=" * separator_length
        formatted_header = f" {header} ".center(separator_length, "=")

        # Print header and separators directly for clarity
        print(f"\n{separator}\n{formatted_header}\n{separator}")
        
        # Log each content line with the specified logging level
        for line in content_lines:
            if line.strip():  # Log non-empty lines with prefix
                self.log_message(f"  {line}", level=level)
            elif skip_prefix_for_blank:  # Print truly blank lines if skip_prefix_for_blank is True
                print("")
        
        # Closing separator for the block
        print(f"{separator}\n")

    def log_start(self, method_name):
        """Logs the start of a method."""
        self.log_info(f"Starting {method_name}...")

    def log_end(self, method_name, success=True, additional_message=""):
        """
        Logs the end of a method, indicating success or failure.
        
        Args:
            method_name (str): The method name.
            success (bool): Indicates success (True) or failure (False).
            additional_message (str): Optional additional message.
        """
        status = "successfully" if success else "with errors"
        self.log_info(f"Finished {method_name} {status}. {additional_message}")

    def exit_notebook(self, message, dbutils=None):
        """
        Exits the notebook with an error message.
        
        Args:
            message (str): Error message to display and log.
            dbutils: Databricks utility for exiting the notebook.
        """
        self.log_error(message)
        if dbutils:
            dbutils.notebook.exit(f"[ERROR] {message}")
        else:
            raise SystemExit(f"[ERROR] {message}")

    def log_function_entry_exit(self, func):
        """
        Decorator to log entry and exit for a function.
        
        Args:
            func (function): The function to decorate.
        
        Returns:
            The wrapped function with entry and exit logging.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.log_debug(f"Entering {func.__name__} with args: {args}, kwargs: {kwargs}")
            result = func(*args, **kwargs)
            self.log_debug(f"Exiting {func.__name__} with result: {result}")
            return result
        return wrapper