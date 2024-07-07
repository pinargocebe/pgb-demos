"""
Main script to read and convert schema files to spark schema.
"""

import json
import os

from exception import UnsupportedEntityException
from pyspark.sql.types import StructType


class SchemaFactory:  # pylint: disable=too-few-public-methods
    """Schema factory class to prepare spark schema for the data entities"""

    @staticmethod
    def get_schema(entity_type: str) -> StructType:
        """Gets schema of the given entity.

        Parameters
        ----------
        entity_type: str
            type of the entity

        Returns
        ----------
        StructType
            Prepared spark schema for the given entity.

        Exceptions
        ----------
        UnsupportedEntityException
            Raises 'UnsupportedEntityException',
            if the there is not any schema file for the given entity type.
        """
        if entity_type in ["checkin", "tip", "review", "business", "user"]:
            schema_file_path = f"{os.getenv('SCHEMA_PATH')}{entity_type}.json"
            with open(schema_file_path, encoding="UTF-8") as f:
                d = json.load(f)
                return StructType.fromJson(d)
        else:
            raise UnsupportedEntityException(f"Provide schema for entity: '{entity_type}'")
