"""
Main script for logging.
"""

import os

from pyspark.sql import SparkSession


class LoggerProvider:  # pylint: disable=too-few-public-methods
    """Logger class for the spark application"""

    def get_logger(self, spark: SparkSession):
        """Sets log level for the logger and prepares logger instance.

        Parameters
        ----------
        spark: SparkSession
            spark session

        Returns
        ----------
        Logger instance
        """
        log_level = os.getenv("LOG_LEVEL") or "INFO"
        spark.sparkContext.setLogLevel(log_level)
        log4j_logger = spark._jvm.org.apache.log4j  # type: ignore # pylint: disable=protected-access
        return log4j_logger.LogManager.getLogger(self.__full_name__())

    def __full_name__(self):
        klass = self.__class__
        module = klass.__module__
        if module == "__builtin__":
            return klass.__name__  # avoid outputs like '__builtin__.str'
        return module + "." + klass.__name__
