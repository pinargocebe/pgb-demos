"""
Main file for any exceptions.
"""

from typing import Any


class UnsupportedFileFormatException(Exception):
    """Exception class for unsupported file formats"""

    def __init__(self, error: Any):
        self.error = error
        Exception.__init__(self, error)

    def __str__(self):
        return f"{self.__class__.__qualname__}: {self.error}"


class MissingDataException(Exception):
    """Exception class for missing data"""

    def __init__(self, error: Any):
        self.error = error
        Exception.__init__(self, error)

    def __str__(self):
        return f"{self.__class__.__qualname__}: {self.error}"
