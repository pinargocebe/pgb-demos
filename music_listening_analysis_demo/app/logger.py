"""
Main file for logging configuration.
"""

import logging
import os

LOG_LEVEL = os.getenv("LOG_LEVEL") or "INFO"

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)
