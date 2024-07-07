"""
Main script for data processing to prepare bronze, silver and gold layers of the datalake.
"""

import os
import pathlib
from datetime import datetime

import pyspark.sql.functions as F
from delta import DeltaTable, configure_spark_with_delta_pip
from dotenv import load_dotenv
from logger import LoggerProvider
from pyspark.sql import SparkSession, Window
from transformer import TransformerFactory

from schema import SchemaFactory


class DataProcessor(LoggerProvider):
    """DataProcessor class to process input data and prepare bronze, silver and gold layers of the datalake"""

    def __init__(self):
        load_dotenv()

        master_url = os.getenv("MASTER_NODE_URL") or "local"
        builder = (
            SparkSession.builder.master(master_url)
            .appName("Yelp Data Lake Demo")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        self.spark_session = configure_spark_with_delta_pip(builder).getOrCreate()

        self.logger = self.get_logger(self.spark_session)
        self.input_path = os.getenv("INPUT_PATH")
        self.bronze_path = os.getenv("BRONZE_PATH")
        self.silver_path = os.getenv("SILVER_PATH")
        self.gold_path = os.getenv("GOLD_PATH")
        self.logger.info(
            f"Data processor intialized with parameters: {self.input_path=}, "
            f"{self.bronze_path=}, "
            f"{self.silver_path=}, "
            f"{self.gold_path=}"
        )

    def process(self):
        """Processes the input data:
        - Writes raw input data to bronze layer.
        - Does transformation, clean-up, schema validation, optimization
          and writes validated data into silver layer.
        - Calculates aggreagations and writes to gold layer.
        """
        try:
            for f in pathlib.Path(self.input_path).iterdir():
                if f.is_file():
                    file_path = f"{self.input_path}{f.name}"
                    entity = f.name.split("_")[-1].split(".")[0]
                    self.logger.info(f"Processing entity: '{entity}'")
                    bronze_data_path = self.raw_to_bronze(file_path, entity)

                    self.bronze_to_silver(bronze_data_path, entity)
                    self.logger.debug(f"Processed entity: {entity}")

            self.logger.info("Preparing aggregations.")
            # Calculate stars per business on a weekly basis
            self.agg_stars_per_business()
            # Calculate number of checkins of a business compared to overall star rating
            self.agg_number_of_checkins()
            self.logger.debug("Prepared aggregations.")
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.error("Failed to process data: ", e)

    def agg_number_of_checkins(self):
        """Prepares aggregation to find number of checkins of a business compared to overall star rating.
        and writes results into gold layer.
        """
        self.logger.info(
            "Calculating  aggregation: 'number of checkins of a business compared to overall star rating'."
        )
        checkin_df_path = f"{self.silver_path}/checkin/"
        business_df_path = f"{self.silver_path}/business/"
        checkin_df = self.spark_session.read.format("delta").load(checkin_df_path)
        business_df = (
            self.spark_session.read.format("delta")
            .load(business_df_path)
            .withColumnRenamed("business_id", "b_id")
            .select("b_id", "stars")
        )
        w = Window.partitionBy(F.col("b_id"))
        number_of_checkins_df = (
            business_df.join(checkin_df, business_df.b_id == checkin_df.business_id)
            .withColumn("number_of_checkins", F.count("date").over(w))
            .select("business_id", "number_of_checkins", "stars")
            .dropDuplicates()
        )
        number_of_checkins_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            f"{self.gold_path}number_of_chekins_vs_stars_per_business"
        )
        self.logger.debug(
            "Calculated  aggregation: 'number of checkins of a business compared to overall star rating'."
        )

    def agg_stars_per_business(self):
        """Prepares aggregation to find stars per business on a weekly basis.
        and writes results into gold layer.
        """
        review_df_path = f"{self.silver_path}/review/"
        review_df = self.spark_session.read.format("delta").load(review_df_path).select("business_id", "date", "stars")
        self.logger.info("Calculating  aggregation: 'stars per business on a weekly basis'.")
        stars_per_business_df = (
            review_df.withColumn("week_start", F.date_sub(F.next_day(F.col("date"), "sunday"), 7))
            .withColumn("week_end", F.next_day(F.col("date"), "sunday"))
            .withColumn("week", F.concat_ws(" - ", "week_start", "week_end"))
            .groupBy("business_id", "week")
            .agg(F.avg("stars").alias("weekly_stars_per_business"))
            .withColumn("weekly_stars_per_business", F.round("weekly_stars_per_business"))
            .orderBy("week")
            .select("business_id", "week", "weekly_stars_per_business")
        )
        stars_per_business_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            f"{self.gold_path}weekly_stars_per_business"
        )
        self.logger.debug("Calculated  aggregation: 'stars per business on a weekly basis'.")

    def bronze_to_silver(self, bronze_data_path: str, entity: str):
        """Does transformations, drop duplicate data and writes bronze layer data of the given entity into silver layer.

        Parameters
        ----------
        bronze_data_path: str
            path of the bronze layer data
        entity: str
            name of the entity
        """
        self.logger.info(f"Preparing '{entity}' entity data for silver layer.")
        bronze_df = (
            self.spark_session.read.format("parquet").schema(SchemaFactory.get_schema(entity)).load(bronze_data_path)
        )
        self.logger.info(f"Validating schema of '{entity}' entity.")
        transformer = TransformerFactory.get_transformer(entity)
        silver_df = transformer.transform(bronze_df)
        self.logger.debug(f"Validated schema of '{entity}' entity.")

        self.logger.info(f"Dropping duplicates of '{entity}' entity.")
        silver_df = silver_df.dropDuplicates()
        self.logger.debug(f"Dropped duplicates of '{entity}' entity.")
        self.logger.debug(f"Prepared '{entity}' entity data for silver layer.")

        self.logger.info(f"Writing '{entity}' entity to silver layer.")
        silver_data_path = f"{self.silver_path}{entity}"
        if DeltaTable.isDeltaTable(self.spark_session, silver_data_path):
            silver_delta_table = DeltaTable.forPath(self.spark_session, silver_data_path)
            transformer.merge_into(silver_df, silver_delta_table)
        else:
            silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_data_path)
        self.logger.info(f"Wrote '{entity}' entity to silver layer.")

        self.logger.info(f"Optimizing '{entity}' data.")
        transformer.optimize(self.spark_session, silver_data_path)
        self.logger.debug(f"Optimized '{entity}' data.")

    def raw_to_bronze(self, file_path: str, entity: str) -> str:
        """Reads raw input data for the given entity and writes it to the bronye layer
        without doing any change on the data.

        Parameters
        ----------
        file_path: str
            path for the input data
        entity: str
            name of the entity

        Returns
        ----------
        str
            Path of the bronye data for the given entity.

        """
        # read raw data
        self.logger.info(f"Writing '{entity}' entity to bronze layer.")
        bronze_df = self.spark_session.read.format("json").load(file_path)
        # write raw data to bronze layer
        ct = datetime.now()
        bronze_data_path = f"{self.bronze_path}/{entity}/year={ct.year}/month={ct.month}/day={ct.day}/"
        bronze_df.write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(bronze_data_path)
        self.logger.debug(f"Wrote '{entity}' entity to bronze layer.")
        return bronze_data_path


if __name__ == "__main__":
    data_processor = DataProcessor()
    data_processor.process()
