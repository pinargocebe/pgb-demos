import os
import shutil
from unittest import mock

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local")
        .appName("local-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "MASTER_NODE_URL": "local",
            "INPUT_PATH": "tests/data/input/",
            "BRONZE_PATH": "tests/data/out/raw_data/",
            "SILVER_PATH": "tests/data/out/processed_data/",
            "GOLD_PATH": "tests/data/out/results/",
            "SCHEMA_PATH": "schema/",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield


@pytest.fixture(autouse=True)
def teardown():
    yield
    test_out_path = "tests/data/out/"
    if os.path.exists(test_out_path):
        shutil.rmtree(test_out_path)
