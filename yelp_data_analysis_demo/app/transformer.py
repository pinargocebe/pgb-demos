"""
Main script to do clean-up, transformation, schema validation,
write and optimization over the data entities.
"""

from abc import ABC, abstractmethod

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, StringType

from app.exception import UnsupportedEntityException


class Transformer(ABC):
    """Interface for the entity transformers"""

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Abstract method to implement the transformation over the given dataframe

        Parameters
        ----------
        df: DataFrame
            data for the entity

        Returns
        ----------
        DataFrame
            Transformed data
        """

    @abstractmethod
    def merge_into(self, new_df: DataFrame, old_df: DeltaTable):
        """Abstract method to merge new data into the delta lake by conditionally checking the given new and old data.

        Parameters
        ----------
        new_df: DataFrame
            new data for the entity
        old_df: DataFrame
            old data for the entity
        """

    @abstractmethod
    def optimize(self, spark: SparkSession, path: str):
        """Abstract method to do delta lake file compaction and z-ordering for the entity data.

        Parameters
        ----------
        spark: SparkSession
            spark session instance.
        path: str
            data path for the entity
        """


class BusinessTransformer(Transformer):
    """Transformer class for the 'business' entity"""

    def transform(self, df: DataFrame) -> DataFrame:
        """Implement the transformations over the given dataframe for 'business' entity.


        Parameters
        ----------
        df: DataFrame
            data for the 'business' entity

        Returns
        ----------
        DataFrame
            Transformed 'business' entity data
        """
        df = df.withColumn(
            "categories",
            F.cast(  # type: ignore
                ArrayType(StringType(), True),
                F.when(df.categories.isNotNull(), F.split("categories", ", ")).otherwise(F.array()),
            ),
        )
        cols = df.columns
        cols.remove("business_id")
        return df.select(["business_id"] + cols)

    def merge_into(self, new_df: DataFrame, old_df: DeltaTable):
        """Merges new 'business' entity data into the delta lake by conditionally checking the given new and old data.

        Parameters
        ----------
        new_df: DataFrame
            new data for the 'business' entity
        old_df: DataFrame
            old data for the 'business' entity
        """
        (
            old_df.alias("source")
            .merge(new_df.alias("target"), "source.business_id = target.business_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def optimize(self, spark: SparkSession, path: str):
        """Does delta lake file compaction and z-ordering for the 'business' entity data.

        Parameters
        ----------
        spark: SparkSession
            spark session instance.
        path: str
            data path for the 'business' entity
        """
        delta_table = DeltaTable.forPath(spark, path)
        delta_table.optimize().executeCompaction()
        delta_table.optimize().executeZOrderBy("business_id")


class CheckinTransformer(Transformer):
    """Transformer class for the 'checkin' entity"""

    def transform(self, df: DataFrame) -> DataFrame:
        """Implement the transformations over the given dataframe for 'checkin' entity.


        Parameters
        ----------
        df: DataFrame
            data for the 'checkin' entity

        Returns
        ----------
        DataFrame
            Transformed 'checkin' entity data
        """
        df = df.withColumn(
            "date",
            F.when(df.date.isNotNull(), F.split("date", ", ")).otherwise(F.array()),
        )
        df = df.withColumn("date", F.explode(df.date))
        df = df.withColumn("date", F.to_timestamp(df.date, "yyyy-MM-dd HH:mm:ss"))
        return df.withColumn("checkin_id", F.sha2(F.concat_ws("||", *df.columns), 256))

    def merge_into(self, new_df: DataFrame, old_df: DeltaTable):
        """Merges new 'checkin' entity data into the delta lake by conditionally checking the given new and old data.

        Parameters
        ----------
        new_df: DataFrame
            new data for the 'checkin' entity
        old_df: DataFrame
            old data for the 'checkin' entity
        """
        (
            old_df.alias("source")
            .merge(new_df.alias("target"), "source.checkin_id = target.checkin_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def optimize(self, spark: SparkSession, path: str):
        """Does delta lake file compaction and z-ordering for the 'checkin' entity data.

        Parameters
        ----------
        spark: SparkSession
            spark session instance.
        path: str
            data path for the 'checkin' entity
        """
        DeltaTable.forPath(spark, path).optimize().executeCompaction()


class ReviewTransformer(Transformer):
    """Transformer class for the 'review' entity"""

    def transform(self, df: DataFrame) -> DataFrame:
        """Implement the transformations over the given dataframe for 'review' entity.


        Parameters
        ----------
        df: DataFrame
            data for the 'review' entity

        Returns
        ----------
        DataFrame
            Transformed 'review' entity data
        """
        return df.withColumn("date", F.to_timestamp(df.date, "yyyy-MM-dd HH:mm:ss"))

    def merge_into(self, new_df: DataFrame, old_df: DeltaTable):
        """Merges new 'review' entity data into the delta lake by conditionally checking the given new and old data.

        Parameters
        ----------
        new_df: DataFrame
            new data for the 'review' entity
        old_df: DataFrame
            old data for the 'review' entity
        """
        (
            old_df.alias("source")
            .merge(new_df.alias("target"), "source.review_id = target.review_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def optimize(self, spark: SparkSession, path: str):
        """Does delta lake file compaction and z-ordering for the 'review' entity data.

        Parameters
        ----------
        spark: SparkSession
            spark session instance.
        path: str
            data path for the 'review' entity
        """
        delta_table = DeltaTable.forPath(spark, path)
        delta_table.optimize().executeCompaction()
        delta_table.optimize().executeZOrderBy("business_id")


class TipTransformer(Transformer):
    """Transformer class for the 'tip' entity"""

    def transform(self, df: DataFrame) -> DataFrame:
        """Implement the transformations over the given dataframe for 'tip' entity.


        Parameters
        ----------
        df: DataFrame
            data for the 'tip' entity

        Returns
        ----------
        DataFrame
            Transformed 'tip' entity data
        """
        df = df.withColumn("date", F.to_timestamp(df.date, "yyyy-MM-dd HH:mm:ss"))
        return df.withColumn("tip_id", F.sha2(F.concat_ws("||", *df.columns), 256))

    def merge_into(self, new_df: DataFrame, old_df: DeltaTable):
        """Merges new 'tip' entity data into the delta lake by conditionally checking the given new and old data.

        Parameters
        ----------
        new_df: DataFrame
            new data for the 'tip' entity
        old_df: DataFrame
            old data for the 'tip' entity
        """
        (
            old_df.alias("source")
            .merge(
                new_df.alias("target"),
                "source.tip_id = target.tip_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def optimize(self, spark: SparkSession, path: str):
        """Does delta lake file compaction and z-ordering for the 'tip' entity data.

        Parameters
        ----------
        spark: SparkSession
            spark session instance.
        path: str
            data path for the 'tip' entity
        """
        DeltaTable.forPath(spark, path).optimize().executeCompaction()


class UserTransformer(Transformer):
    """Transformer class for the 'user' entity"""

    def transform(self, df: DataFrame) -> DataFrame:
        """Implement the transformations over the given dataframe for 'user' entity.


        Parameters
        ----------
        df: DataFrame
            data for the 'user' entity

        Returns
        ----------
        DataFrame
            Transformed 'user' entity data
        """
        return df.withColumn("yelping_since", F.to_timestamp(df.yelping_since, "yyyy-MM-dd HH:mm:ss"))

    def merge_into(self, new_df: DataFrame, old_df: DeltaTable):
        """Merges new 'user' entity data into the delta lake by conditionally checking the given new and old data.

        Parameters
        ----------
        new_df: DataFrame
            new data for the 'user' entity
        old_df: DataFrame
            old data for the 'user' entity
        """
        (
            old_df.alias("source")
            .merge(new_df.alias("target"), "source.user_id = target.user_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def optimize(self, spark: SparkSession, path: str):
        """Does delta lake file compaction and z-ordering for the 'user' entity data.

        Parameters
        ----------
        spark: SparkSession
            spark session instance.
        path: str
            data path for the 'user' entity
        """
        DeltaTable.forPath(spark, path).optimize().executeCompaction()


class TransformerFactory:  # pylint: disable=too-few-public-methods
    """Transformer factory class to create related transformer instance for the entity."""

    @staticmethod
    def get_transformer(entity_type: str) -> Transformer:
        """Creates transformer instance entity for the given entity type.

        Parameters
        ----------
        entity_type: str
            entity type

        Returns
        ----------
        Transformer
            Created Transformer instance for the given entity

        Exceptions
        ----------
        UnsupportedEntityException
            Raises 'UnsupportedEntityException',
            if the there is not any transformer implementation for the given entity type.
        """
        if entity_type == "business":
            return BusinessTransformer()
        elif entity_type == "checkin":
            return CheckinTransformer()
        elif entity_type == "review":
            return ReviewTransformer()
        elif entity_type == "tip":
            return TipTransformer()
        elif entity_type == "user":
            return UserTransformer()
        else:
            raise UnsupportedEntityException(f"Implement transformer for entity: '{entity_type}'")
