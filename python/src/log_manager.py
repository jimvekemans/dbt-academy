"""
This module provides a LogManager class that wraps the logging module.
It builds on the logging module by providing a single point of entry for logging messages.
To add a log message, use the log function.

Example:
```python
from log_manager import log
log("This is a log message", level="INFO")
```
"""

from datetime import datetime
from json import dumps as json_dumps
from logging import (
    Formatter,
    getLogger,
    Handler,
    Logger,
    LogRecord,
    getLevelNamesMapping,
)
from rich.logging import RichHandler
from os import path
from pprint import pformat


class DictLoggingHandler(Handler):
    """
    This class is a custom logging handler that stores logs in a dictionary.
    The goal is to store logs in memory so we get option down the line to:
    - Save logs to a file
    - Send logs to a database
    - Filter logs based on certain criteria
    """

    logs: dict

    def __init__(self):
        super().__init__()
        self.logs = self.get_logs()

    def get_logs(self) -> dict:
        if hasattr(self, "logs"):
            return self.logs
        else:
            return {}

    def emit(self, record: LogRecord) -> None:
        """
        This function is called when a log record is emitted.
        It stores the log record in a dictionary.
        Currently, only the level and message of a record are stored.

        :param record: The log record to be stored
        :type record: LogRecord
        """
        # Add fields to log_entry as needed
        log_entry = {
            "level": record.levelname,
            "message": record.getMessage(),
        }
        self.logs[datetime.now().isoformat()] = log_entry


class LogManager:
    """
    This class is a wrapper for the logging module.
    It provides a single point of entry for logging messages and saving logs to a JSON file.

    :param logs_directory: The directory where the logs will be saved
    :param project_name: The name of the project

    Example:
    ```
    log_manager = LogManager(logs_directory="logs", project_name="my_project")
    log_manager.logger.debug("This is a debug message")
    log_manager.logger.info("This is an info message")
    log_manager.save_logs_to_json()
    ```
    """

    _LOGGING_FORMAT: str = "%(asctime)s - %(levelname)s - %(message)s"

    logger: Logger

    start_datetime: datetime

    logs_directory: str
    log_filename: str

    def __init__(self, logs_directory: str = None, project_name: str = None):
        """
        This function initializes the LogManager object.
        It creates a logger and two handlers, one for the console and one for the logs file.
        The log file is saved in the logs directory.

        :param logs_directory: The directory where the logs will be saved
        :type logs_directory: str, optional
        :param project_name: The name of the project
        :type project_name: str, optional
        """
        self.start_datetime = datetime.now()

        self.logs_directory = logs_directory
        self.log_filename = (
            f'{project_name}_{self.start_datetime.strftime("%Y%m%d%H%M%S")}'
        )

        self.logger = getLogger("__main__")
        self.logger.setLevel(getLevelNamesMapping().get("INFO", 30))

        rich_handler = RichHandler(
            level=self.logger.level,
            rich_tracebacks=True,
            markup=True,
            show_path=False,
            show_time=False,
        )
        self.logger.addHandler(rich_handler)

        self.log_dict_handler = DictLoggingHandler()
        self.log_dict_handler.setFormatter(Formatter(self._LOGGING_FORMAT))
        self.logger.addHandler(self.log_dict_handler)

    def set_level(self, level: str) -> None:
        """
        This function sets the log level of the logger.

        :param level: The log level to be set
        :type level: str
        """
        self.logger.setLevel(getLevelNamesMapping().get(level, 30))

    def get_logs(self) -> dict:
        """
        This function returns the logs stored in the log_dict_handler.

        :return: The logs stored in the log_dict_handler
        :rtype: dict
        """
        return self.log_dict_handler.get_logs()

    def save_logs_to_json(self) -> None:
        """
        This function saves the logs of logs dict to a JSON file.
        The file is saved in the logs directory.
        """
        file_path = path.join(self.logs_directory, f"{self.log_filename}.json")
        logs = self.log_dict_handler.get_logs()
        with open(file_path, "w", encoding="utf-8") as logs_file:
            logs_file.write(json_dumps(logs, indent=4))


def log(message: str, level: str = "INFO") -> None:
    """
    This function logs a message with a specific level, acting as a wrapper for the current logger.
    The goal is to have a single point of entry for logging messages.

    :param message: The message to be logged
    :type message: str
    :param level: The level of the message
    :type level: str, optional
    """
    critical = level in ["CRITICAL", "ERROR"]
    getLogger("__main__").log(
        level=getLevelNamesMapping().get(level, 30),
        msg=stylize_message(message, level) if critical else message,
    )
    if critical:
        raise Exception(message)


def pretty_print(message: str, header: str = None) -> None:
    """
    This function calls the stylize_message function and prints the result.
    This function takes a message block and optionally a message block name.
    See the definition of stylize_message for an example.

    :param message: The message to be encapsulated
    :type message: str
    :param message_block_name: The name of the message block
    :type message_block_name: str, optional
    """
    message = stylize_message(message, header)
    print(message)


def stylize_message(message: str, header: str = None) -> str:
    """
    This function takes a message block and optionally a header.
    ╭────────────────────────────────────────╮
    │ And encapsulates the message like this │
    ╰────────────────────────────────────────╯
    ╭─ Info header ────────────────────╮
    │ Or like this, including a header │
    ╰──────────────────────────────────╯
    ╭─ With a header longer than the message ─╮
    │ It looks like this                      │
    ╰─────────────────────────────────────────╯
    ╭─ If the content is really very wide (>88) ─╮
    │ it will wrap around like this to make sure that it fits inside the constraint of the terminal window
    and it will look like this without the closing line on the right side
    │ Like this we can make sure that the original format doesn't break when lines are folded
    and the print statement stays consistent with the original format
    ╰────────────────────────────────────────────╯

    :param message: The message to be encapsulated
    :type message: str
    :param header: The header of the message block
    :type header: str, optional
    :return: The stylized message block
    :rtype: str
    """

    """ 
    IDEA FOR FANCY HEADER AND FOOTER FOR LONG MESSAGES
    THIS WOULD ESPECIALLY BE USEFUL FOR EXCEPTIONS
    ╭╮──────────╮
    ││  HEADER  │
    │╰──────────╯
    |  CONTENT THAT IS VERY WIDE AND WILL WRAP AROUND
    |  TO MAKE SURE IT FITS INSIDE THE TERMINAL WINDOW
    |  AND IT WILL LOOK LIKE THIS WITHOUT THE CLOSING LINE
    |  TO MAKE SURE THAT THE ORIGINAL FORMAT DOESN'T BREAK
    |  WHEN LINES ARE FOLDED
    │╭──────────╮
    ╰╯──────────╯
    """
    message = pformat(message) if type(message) != str else message
    message = message.strip()

    all_lines = [f"│ {line}" for line in str(message).split("\n")]
    max_len = min(max([len(line) for line in all_lines]), 88) + 2

    if header is not None:
        top_line = f"╭─ {header} {'─' * (max_len - 6 - len(header))}─╮"
    else:
        top_line = f"╭{'─' * (max_len - 2)}╮"
    bottom_line = f"╰{'─' * (len(top_line) - 2)}╯"
    max_len = len(top_line)

    if max_len < 88:
        message_block = (
            f"│\n".join([line.ljust(max_len - 1) for line in all_lines]) + "│"
        )
    else:
        message_block = f"\n".join([line.ljust(88) for line in all_lines])

    message_block = f"{top_line}\n{message_block}\n{bottom_line}"
    return message_block
