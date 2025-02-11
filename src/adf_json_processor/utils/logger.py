import logging
import functools
import sqlparse
from pyspark.sql import DataFrame
from pygments import highlight
from pygments.lexers import SqlLexer, PythonLexer
from pygments.formatters import TerminalFormatter

class Logger:
    """
    Custom logger class for enhanced logging functionality, including debugging,
    block logging, SQL query formatting, Python code formatting, and function entry/exit logging.
    """

    def __init__(self, debug: bool = False, log_to_file: str = None):
        """
        Initialize the Logger.

        Args:
            debug (bool): Enable debug-level logging if True.
            log_to_file (str, optional): Path to log messages to a file.
        """
        self.debug = debug
        self.logger = logging.getLogger("custom_logger")
        self.logger.propagate = False

        # Clear existing handlers to prevent duplicate logging
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Set log level
        self.set_level(debug)

        # Define format for logging messages
        formatter = logging.Formatter('[%(levelname)s] - %(message)s')

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # File handler (if provided)
        if log_to_file:
            file_handler = logging.FileHandler(log_to_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def set_level(self, debug: bool):
        """Set the logging level based on the debug flag."""
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

    def update_debug_mode(self, debug: bool):
        """Update the debug mode and adjust the logger level."""
        self.debug = debug
        self.set_level(debug)

    def log_message(self, message: str, level: str = "info"):
        """Log a message at the specified level."""
        log_function = getattr(self.logger, level, self.logger.info)
        log_function(message)
        if level in {"error", "critical"}:
            raise RuntimeError(message)

    def log_block(self, header: str, content_lines=None, sql_query=None, level="info"):
        """Logs structured blocks of information for better readability."""
        separator_length = 100
        start_separator = "=" * separator_length
        end_separator = "-" * separator_length
        formatted_header = f" {header} ".center(separator_length, "=")

        print("\n" + start_separator)
        print(formatted_header)
        print(start_separator)

        if content_lines:
            for line in content_lines:
                if line.strip():
                    self.log_message(line, level=level)

        if sql_query:
            self.log_sql_query(sql_query, level=level)

        print(end_separator + "\n")

    def log_sql_query(self, query: str, level: str = "info"):
        """Format and log an SQL query with syntax highlighting."""
        formatted_query = sqlparse.format(query, reindent=True, keyword_case='upper')
        highlighted_query = highlight(formatted_query, SqlLexer(), TerminalFormatter())
        self.log_message(f"SQL Query:\n{highlighted_query}", level=level)

    def log_python_code(self, code: str, level: str = "info"):
        """Format and log Python code with syntax highlighting."""
        highlighted_code = highlight(code, PythonLexer(), TerminalFormatter())
        self.log_message(f"Python Code:\n{highlighted_code}", level=level)

    def log_start(self, method_name: str):
        """Log the start of a method."""
        self.log_message(f"Starting {method_name}...", level="info")

    def log_end(self, method_name: str, success: bool = True, additional_message: str = ""):
        """Log the end of a method execution."""
        status = "successfully" if success else "with warnings or issues"
        self.log_message(f"Finished {method_name} {status}. {additional_message}",
                         level="info" if success else "warning")

    def log_dataframe_summary(self, df: DataFrame, label: str, level="info"):
        """Log a summary of a DataFrame."""
        if df:
            row_count = df.count()
            column_count = len(df.columns)
            estimated_memory = df.rdd.map(lambda row: len(str(row))).sum() / (1024 * 1024)
            self.log_block(f"{label} DataFrame Info", [
                f"{label} Estimated Memory Usage: {estimated_memory:.2f} MB",
                f"{label} Rows: {row_count}",
                f"{label} Columns: {column_count}",
                f"{label} Schema:"
            ], level=level)
            df.printSchema()

            column_types = [(field.name, field.dataType.simpleString()) for field in df.schema.fields]
            self.log_block(f"{label} Column Types", [
                f"Column: {name}, Type: {dtype}" for name, dtype in column_types
            ], level="debug")

    def log_function_entry_exit(self, func):
        """Decorator to log the entry and exit of a function."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.log_debug(f"Entering {func.__name__} with args: {args}, kwargs: {kwargs}")
            result = func(*args, **kwargs)
            self.log_debug(f"Exiting {func.__name__} with result: {result}")
            return result
        return wrapper

    def log_info(self, message):
        """Convenience method for logging informational messages."""
        self.log_message(message, level="info")

    def log_debug(self, message: str):
        """Log a debug message."""
        self.log_message(message, level="debug")

    def log_warning(self, message: str):
        """Log a warning message."""
        self.log_message(message, level="warning")

    def log_error(self, message: str):
        """Log an error message and raise a RuntimeError."""
        self.log_message(message, level="error")

    def log_critical(self, message: str):
        """Log a critical message and raise a RuntimeError."""
        self.log_message(message, level="critical")

# Initialize logger instance for reuse across modules
logger = Logger(debug=True)