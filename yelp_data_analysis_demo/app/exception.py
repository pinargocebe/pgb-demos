"""
Main script for the app specific exceptions.
"""


class UnsupportedEntityException(Exception):
    """Exception class for the not implemented entity operations."""


class DataProcessingFailure(Exception):
    """Exception class for the data processor errors."""
