import duckdb
from conftest import TEST_DB

from app.db_connector import ARTISTS_TABLE_NAME, TIME_TABLE_NAME, USER_LISTENS_TABLE_NAME, USERS_TABLE_NAME, DBConnector


def test_create_tables():
    DBConnector(db_name="test")

    with duckdb.connect(TEST_DB) as conn:
        table_df = conn.execute("SHOW TABLES").df()

    assert len(table_df) == 4
    assert table_df["name"].array == [
        ARTISTS_TABLE_NAME,
        TIME_TABLE_NAME,
        USER_LISTENS_TABLE_NAME,
        USERS_TABLE_NAME,
    ]
