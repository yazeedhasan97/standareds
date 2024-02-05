import logging
import os
import re
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import time
from contextlib import ContextDecorator
from typing import Callable, Any


class MultipurposeLogger:
    # Pre-compile the regex pattern
    _valid_name_pattern = re.compile("^[A-Za-z0-9_.-]+$")

    def __init__(self, name: str, path: str = 'logs', is_test: bool = False, ):

        self.__name = None
        self.__path = None
        self.__log_file = None
        self.is_test = is_test

        self.__set_name(name)
        self.__set_path(path, create=True if path == 'logs' else False)

        self.logger = logging.getLogger(self.__name)
        self.initialize_logger_handler()

    def get_name(self):
        return self.__name

    def get_log_file(self):
        return self.__log_file

    def __set_name(self, name):
        """
        Set the logger's name with validation.

        Parameters:
        name (str): The name to be assigned to the logger.

        Raises:
        ValueError: If the name does not match the required pattern.
        """
        if not MultipurposeLogger._valid_name_pattern.match(name):
            raise ValueError("Name must contain only alphanumeric characters, underscores, hyphens, and periods.")
        self.__name = name

    def get_path(self):
        return self.__path

    def __set_path(self, path, create=False):
        """
        Set the path for the logger and optionally create it if it doesn't exist.

        Parameters:
        path (str): The path to be set for logging.
        create (bool): Whether to create the path if it doesn't exist.

        Raises:
        FileNotFoundError: If the path doesn't exist and 'create' is False.
        OSError: For issues related to creating the directory.
        """
        try:
            if not os.path.exists(path) and create:
                os.makedirs(path, exist_ok=True)
            elif not os.path.exists(path):
                raise FileNotFoundError(f"The given path '{path}' does not exist.")
            self.__path = path
        except OSError as e:
            raise OSError(f"Error creating directory '{path}': {e}")
        except Exception as e:
            print(f"An unexpected error occurred while setting the path '{path}': {e}")
            raise

    def check_and_reinitialize_log_file(self):
        """
        Check if the log file exists, and reinitialize if it does not.
        Mainly, This methods aim to solve the problem where the log file is removed during the runtime.
        """
        log_file_exists = os.path.exists(self.__log_file)
        if not log_file_exists:
            print("Log file was missing... Reinitializing log file and file handler.")
            self.warning("Log file was missing... Reinitializing log file and file handler.")
            self.initialize_logger_handler()

    def initialize_logger_handler(self, log_level: int = None, max_bytes: int = 10485760,
                                  backup_count: int = 1000, rotate_time: str = None):
        """
        Initialize the logger with specific settings.

        Parameters:
            log_level (int): the logging level for the file handler
            max_bytes (int): Maximum size in bytes for RotatingFileHandler. 10 MB by default.
            backup_count (int): Number of retention files to keep. 1000 file by default.
            rotate_time (str): Rotation interval for TimedRotatingFileHandler (e.g., 'midnight', 'W0', 'D').

        Raises:
            IOError: For issues related to file handling during logger setup.
        """
        try:
            formatter = logging.Formatter(
                '[%(asctime)s:%(levelname)s] || {%(pathname)s Line:%(lineno)d} -- %(message)s'
            )
            self.__log_file = os.path.join(self.__path, f'{self.__name}_{datetime.now():%Y%m%d_%H%M%S}.log')
            if rotate_time:
                file_handler = TimedRotatingFileHandler(
                    filename=self.__log_file,
                    when=rotate_time,
                    backupCount=backup_count,
                    encoding='utf-8',  # Optional: Specify encoding if needed
                )
                file_handler.suffix = "%Y%m%d_%H%M%S.log"
            else:
                file_handler = RotatingFileHandler(
                    filename=self.__log_file,
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding='utf-8',
                )

            print(f"Log File: {self.__log_file}")
            if self.is_test:
                file_handler.setLevel(logging.DEBUG)
                self.logger.setLevel(logging.DEBUG)
            else:
                file_handler.setLevel(logging.INFO)
                self.logger.setLevel(logging.INFO)

            if log_level:
                file_handler.setLevel(log_level)
                self.logger.setLevel(log_level)

            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        except IOError as e:
            raise IOError(f"Error initializing file handler for logger: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while initializing file handler for logger: {e}")
            raise

    def info(self, message):
        self.logger.info(f"- {message}")
        print(f"{message}")

    def warning(self, message):
        self.logger.warning(f"- {message}")
        print(f"{message}")

    def error(self, message):
        self.logger.error(f"- {message}")
        print(f"{message}")

    def debug(self, message):
        self.logger.debug(f"- {message}")
        print(f"{message}")


class TimeLogger(ContextDecorator):
    """Used to track the performance of sections of codes.

    Use as either a context manager or decorator.
    """

    def _init_(
            self, task_name: str | None = None, log_func: Callable[[str], None] = print
    ) -> None:
        self._task_name = task_name
        self._log_func = log_func
        self._start_time: int | None = None

    def _enter_(self):
        self._log_func(f"Entering: {self._task_name}")
        self._start_time = time.perf_counter()

    def _exit_(self, exc_type, exc, exc_tb):
        seconds = time.perf_counter() - self._start_time
        self._log_func(f"Exiting: {self._task_name} after {seconds} seconds.")

    def _call_(self, func: Callable[..., Any]) -> Callable[..., Any]:
        self.task_name = self._task_name or func.name_
        return super()._call_(func)


if __name__ == "__main__":
    logger = MultipurposeLogger(
        name='TestLogger'
    )
    while True:
        logger.info('Hi, How are you')
        logger.error('Hi, How are you')
