import os
from unittest import mock

import duckdb
import pytest

from app.db_connector import ARTISTS_TABLE_NAME, TIME_TABLE_NAME, USER_LISTENS_TABLE_NAME, USERS_TABLE_NAME

TEST_DB = "test.db"


@pytest.fixture(autouse=True)
def setup():
    with duckdb.connect(TEST_DB) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {USER_LISTENS_TABLE_NAME}")
        conn.execute(f"DROP TABLE IF EXISTS {ARTISTS_TABLE_NAME}")
        conn.execute(f"DROP TABLE IF EXISTS {TIME_TABLE_NAME}")
        conn.execute(f"DROP TABLE IF EXISTS {USERS_TABLE_NAME}")


@pytest.fixture()
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "DB_NAME": "test",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield  # restore the environment after
