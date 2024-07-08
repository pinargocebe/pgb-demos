import os
from unittest.mock import patch

import pytest
from pyspark.sql.types import ArrayType, StringType, TimestampType
from pyspark.testing.utils import assertDataFrameEqual

from app.data_processor import DataProcessor
from app.exception import DataProcessingFailure

entities = ["business", "checkin", "review", "tip", "user"]


def test_raw_to_bronze(spark):
    processor = DataProcessor()

    for entity in entities:
        input_path = f"{os.getenv('INPUT_PATH')}{entity}.json"
        processor.raw_to_bronze(input_path, entity)

    assert_bronze_data(spark)


def test_raw_to_bronze_existing_data(spark):
    processor = DataProcessor()

    for entity in entities:
        input_path = f"{os.getenv('INPUT_PATH')}{entity}.json"
        processor.raw_to_bronze(input_path, entity)

        # process same data again
        processor.raw_to_bronze(input_path, entity)

    assert_bronze_data(spark)


def test_bronze_to_silver(spark):
    processor = DataProcessor()

    for entity in entities:
        input_path = f"{os.getenv('INPUT_PATH')}{entity}.json"
        bronze_path = processor.raw_to_bronze(input_path, entity)
        processor.bronze_to_silver(bronze_path, entity)

    assert_silver_data(spark)


def test_bronze_to_silver_existing_data(spark):
    processor = DataProcessor()

    for entity in entities:
        input_path = f"{os.getenv('INPUT_PATH')}{entity}.json"
        bronze_path = processor.raw_to_bronze(input_path, entity)
        processor.bronze_to_silver(bronze_path, entity)

        # process same data again
        processor.bronze_to_silver(bronze_path, entity)

    assert_silver_data(spark)


def test_agg_number_of_checkins(spark):
    # prepare test data
    checkin_df = spark.read.format("json").load("tests/data/checkin_agg_data/")
    checkin_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{os.getenv('SILVER_PATH')}checkin/"
    )
    business_df = spark.read.format("json").load("tests/data/business_agg_data/")
    business_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{os.getenv('SILVER_PATH')}business/"
    )
    # aggregate
    processor = DataProcessor()
    processor.agg_number_of_checkins()
    result_df = spark.read.format("delta").load(f"{os.getenv('GOLD_PATH')}number_of_chekins_vs_stars_per_business")

    # assert
    assert result_df.count() == 2
    result_df.show(10)
    expected_df = spark.createDataFrame(
        data=[
            ("1GVRe_vfFHLMNtE3Gfv3zg", 42, 2.5),
            ("21IkQYUsyfl8MWYy_wQxfw", 39, 2.5),
        ],
        schema=["business_id", "number_of_checkins", "stars"],
    )
    assertDataFrameEqual(expected_df, result_df)


def test_agg_stars_per_business(spark):
    # prepare test data
    review_df = spark.read.format("json").load("tests/data/review_agg_data/")
    review_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{os.getenv('SILVER_PATH')}review/"
    )
    # aggregate
    processor = DataProcessor()
    processor.agg_stars_per_business()
    result_df = spark.read.format("delta").load(f"{os.getenv('GOLD_PATH')}weekly_stars_per_business")

    # assert
    assert result_df.count() == 7
    expected_df = spark.createDataFrame(
        data=[
            ("GXFMD0Z4jEVZBCsbPf4CTQ", "2018-07-08 - 2018-07-15", 5.0),
            ("GXFMD0Z4jEVZBCsbPf4CTQ", "2019-01-27 - 2019-02-03", 5.0),
            ("0BXnGq6eJuiWR7270TipoA", "2019-06-02 - 2019-06-09", 3.0),
            ("GXFMD0Z4jEVZBCsbPf4CTQ", "2021-01-24 - 2021-01-31", 1.0),
            ("GXFMD0Z4jEVZBCsbPf4CTQ", "2021-03-07 - 2021-03-14", 4.0),
            ("0BXnGq6eJuiWR7270TipoA", "2021-04-18 - 2021-04-25", 5.0),
            ("0BXnGq6eJuiWR7270TipoA", "2021-06-27 - 2021-07-04", 5.0),
        ],
        schema=["business_id", "week", "weekly_stars_per_business"],
    )
    assertDataFrameEqual(expected_df, result_df)


def test_process(spark):
    pass  # TODO


def test_process_failed(spark):
    with pytest.raises(DataProcessingFailure):
        with patch("app.data_processor.DataProcessor.raw_to_bronze", side_effect=Exception("Error happened")):
            processor = DataProcessor()
            processor.process()


def assert_bronze_data(spark):
    # business
    business_df = spark.read.format("parquet").load(f"{os.getenv('BRONZE_PATH')}business")
    assert business_df.count() == 3
    expected_business_ids = ["Pns2l4eNsfO8kk83dixA6A", "mpf3x-BjTdTEA3yCZrAYPw", "tUFrWirKiKi_TAnsVWINQQ"]
    assert [row.business_id for row in business_df.collect()] == expected_business_ids

    # checkin
    checkin_df = spark.read.format("parquet").load(f"{os.getenv('BRONZE_PATH')}checkin")
    assert checkin_df.count() == 3
    expected_business_ids = ["Pns2l4eNsfO8kk83dixA6A", "mpf3x-BjTdTEA3yCZrAYPw", "tUFrWirKiKi_TAnsVWINQQ"]
    assert [row.business_id for row in checkin_df.collect()] == expected_business_ids

    # review
    review_df = spark.read.format("parquet").load(f"{os.getenv('BRONZE_PATH')}review")
    assert review_df.count() == 11
    expected_review_ids = [
        "hzvRRb40oPttxAdyr7kfow",
        "xUkBPk-QfcW4i3MRU5TeXw",
        "7zGoOrFQT5WylJYax1pYnA",
        "2DhY5MYiQ8oy-ZqZthYiOg",
        "hsqyCkeIxNOeQ12S94EPYQ",
        "modGqK95718eNnJc9pEIjA",
        "GeXmTsLHIL3nFNRvmG7jbg",
        "TChqW_vZ1O7U89P81560Fg",
        "4uppC_A_XyB8t99IIBhbFw",
        "djq1krF2uxdv5wbCsRj2YA",
        "1qbH9dXUmyZbngXpB-nerA",
    ]
    assert [row.review_id for row in review_df.collect()] == expected_review_ids

    # tip
    tip_df = spark.read.format("parquet").load(f"{os.getenv('BRONZE_PATH')}tip")
    assert tip_df.count() == 5
    expected_business_ids = ["I42Ebt8haptL_Wew3r9wLA", "tUFrWirKiKi_TAnsVWINQQ", "mpf3x-BjTdTEA3yCZrAYPw"]
    assert [row.business_id for row in tip_df.select("business_id").dropDuplicates().collect()] == expected_business_ids
    expected_user_ids = [
        "d6zIVWiJyPB6PZuAxVexwg",
        "_5swqa5xUdLar-Q-bBZSDA",
        "oAvO0BOHOagOI7WVGXlWSA",
        "moSLKqdFUI-B80vun67UfQ",
        "trf3Qcz8qvCDKXiTgjUcEg",
    ]
    assert [row.user_id for row in tip_df.select("user_id").dropDuplicates().collect()] == expected_user_ids

    # user
    user_df = spark.read.format("parquet").load(f"{os.getenv('BRONZE_PATH')}user")
    assert user_df.count() == 7
    expected_user_ids = [
        "CiwVvb7jWijWB5jkmatzKA",
        "QkCbMKBktkrkOFJugHvY6w",
        "ZCACyKwpELPtwV4Hue5pcg",
        "q-RkAwuq6h6unalI2CWF0Q",
        "mdDTEN3J9ufdyT6GxPaRbA",
        "d6zIVWiJyPB6PZuAxVexwg",
        "_5swqa5xUdLar-Q-bBZSDA",
    ]
    assert [row.user_id for row in user_df.collect()] == expected_user_ids


def assert_silver_data(spark):
    # business
    business_df = spark.read.format("delta").load(f"{os.getenv('SILVER_PATH')}business/")
    assert business_df.count() == 3
    assert business_df.columns[0] == "business_id"
    assert dict(business_df.dtypes)["categories"] == ArrayType(StringType(), True).simpleString()

    # checkin
    checkin_df = spark.read.format("delta").load(f"{os.getenv('SILVER_PATH')}checkin/")
    assert checkin_df.count() == 417
    assert "checkin_id" in checkin_df.columns
    assert dict(checkin_df.dtypes)["date"] == TimestampType().simpleString()

    # review
    review_df = spark.read.format("delta").load(f"{os.getenv('SILVER_PATH')}review/")
    assert review_df.count() == 11
    assert dict(review_df.dtypes)["date"] == TimestampType().simpleString()

    # tip
    tip_df = spark.read.format("delta").load(f"{os.getenv('SILVER_PATH')}tip/")
    assert tip_df.count() == 5
    assert "tip_id" in tip_df.columns
    assert dict(tip_df.dtypes)["date"] == TimestampType().simpleString()

    # user
    user_df = spark.read.format("delta").load(f"{os.getenv('SILVER_PATH')}user/")
    assert user_df.count() == 7
    assert dict(user_df.dtypes)["yelping_since"] == TimestampType().simpleString()
