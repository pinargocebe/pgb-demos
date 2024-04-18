"""
Main module for the database operations.
"""

import os

import duckdb

from app.logger import logger

USER_LISTENS_TABLE_NAME = "user_listens"
ARTISTS_TABLE_NAME = "artists"
TIME_TABLE_NAME = "time"
USERS_TABLE_NAME = "users"


class DBConnector:
    """
    Class for the database operations.
    """

    USER_TABLE_CREATE_STMT = f"""CREATE TABLE IF NOT EXISTS {USERS_TABLE_NAME} (
            user_id VARCHAR PRIMARY KEY NOT NULL,
            user_name VARCHAR UNIQUE NOT NULL
        )"""

    ARTIST_TABLE_CREATE_STMT = f"""CREATE TABLE IF NOT EXISTS {ARTISTS_TABLE_NAME} (
            artist_id VARCHAR PRIMARY KEY NOT NULL,
            artist_name VARCHAR
        )"""

    TIME_TABLE_CREATE_STMT = f"""CREATE TABLE IF NOT EXISTS {TIME_TABLE_NAME} (
            listen_time BIGINT PRIMARY KEY NOT NULL,
            timestamp TIMESTAMP_NS NOT NULL,
            date DATE NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            day_name VARCHAR NOT NULL,
            hour INTEGER NOT NULL
        )"""

    USER_LISTENS_TABLE_CREATE_STMT = f"""CREATE TABLE IF NOT EXISTS {USER_LISTENS_TABLE_NAME} (
            listen_id VARCHAR PRIMARY KEY NOT NULL,
            user_id VARCHAR REFERENCES users(user_id) NOT NULL,
            listen_time BIGINT REFERENCES time(listen_time) NOT NULL,
            artist_id VARCHAR REFERENCES artists(artist_id) NOT NULL,
            song_name VARCHAR,
            album_name VARCHAR
        )"""

    def __init__(self, db_name=None):
        # duck db uses in-memory db, if environment variable is not set
        self.db_name = (
            db_name + ".db"
            if db_name is not None
            else (os.getenv("DB_NAME") + ".db" if os.getenv("DB_NAME") else ":memory:")
        )
        # create tables if necessary
        self.create_tables()

    def create_tables(self):
        """Create database tables if the tables don't exit."""
        try:
            logger.info("Creating database tables.")
            with duckdb.connect(database=self.db_name) as conn:
                conn.execute(DBConnector.USER_TABLE_CREATE_STMT)
                conn.execute(DBConnector.ARTIST_TABLE_CREATE_STMT)
                conn.execute(DBConnector.TIME_TABLE_CREATE_STMT)
                conn.execute(DBConnector.USER_LISTENS_TABLE_CREATE_STMT)
                logger.debug("Created database tables.")
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "Failed to create database tables: %s error occured: %s",
                type(e),
                e,
            )
            raise e

    def execute(self, query: str):
        """Executed given query string on database.

        Parameters
        ----------
        query: str
            Query string to execute
        """
        with duckdb.connect(database=self.db_name) as conn:
            conn.execute(query=query)
