"""
Main module for the etl operations: extract, load, transform, validate etc.
"""

import hashlib
import json
import sys
from typing import Any, Optional, Tuple
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pandera as pa

from app.db_connector import ARTISTS_TABLE_NAME, TIME_TABLE_NAME, USER_LISTENS_TABLE_NAME, USERS_TABLE_NAME, DBConnector
from app.exceptions import MissingDataException, UnsupportedFileFormatException
from app.logger import logger
from app.schema import artist_schema, time_schema, user_listens_schema, user_schema

ZIP_FILE_EXT = ".zip"
TXT_FILE_EXT = ".txt"
JSON_FILE_EXT = ".json"

USER_COLS = {"user_name": "user_name"}
ARTIST_COLS = {
    "track_metadata.additional_info.artist_msid": "artist_id",
    "track_metadata.artist_name": "artist_name",
}
DATETIME_COLS = {"listened_at": "listen_time"}
USER_LISTENS_COLS = {
    "user_name": "user_name",
    "recording_msid": "listen_id",
    "listened_at": "listen_time",
    "track_metadata.track_name": "song_name",
    "track_metadata.release_name": "album_name",
    "track_metadata.additional_info.artist_msid": "artist_id",
}


def extract(path) -> Optional[pd.DataFrame]:
    """Reads given json file into a dataframe.

    Parameters
    ----------
    path
        Path of the file to extract.

    Returns
    -------
    pd.Dataframe
        Extracted data from given file as dataframe
    """
    df = None
    try:
        logger.info("Extracting file '%s'", path)
        if path.endswith(ZIP_FILE_EXT):
            dfs = []
            with ZipFile(path) as z_file:
                for z_f in z_file.namelist():
                    with z_file.open(z_f) as f:
                        dfs.append(normalize_json(f))
            df = pd.concat(dfs)
        elif path.endswith(TXT_FILE_EXT) or path.endswith(JSON_FILE_EXT):
            with open(path, encoding="UTF-8") as f:
                df = normalize_json(f)
        else:
            msg = "Unsupported file format, provide '.zip' or '.txt' file/path as input."
            raise UnsupportedFileFormatException(msg)
        logger.debug("Extracted file '%s', rows: %d", path, len(df))
    except UnsupportedFileFormatException as e:
        raise e
    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "Failed to extract data from file: '%s', %s error occured: %s",
            path,
            type(e),
            e,
        )
        raise e
    return df


def transform(df: pd.DataFrame) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Does validations and transformtions for the given dataframe.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe to process

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
        Tuple of the transformed dataframes per each table of the data model.
    """
    try:
        logger.info("Doing transformations and validations for the input data")
        # check required columns
        check_required_columns(df)
        # prepare dataframes
        user_df, artist_df, time_df, user_listens_df = prepare_dataframes(df)
        # validation for missing values types etc.
        user_df = validate(user_df, user_schema)
        artist_df = validate(artist_df, artist_schema)
        time_df = validate(time_df, time_schema)
        user_listens_df = validate(user_listens_df, user_listens_schema)
        logger.info("Done transformations and validations for the input data")
    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "Failed to transform data: %s error occured: %s",
            type(e),
            e,
        )
        raise e
    return user_df, artist_df, time_df, user_listens_df


# pylint: disable=unused-argument
def load(
    user_df: pd.DataFrame,
    artist_df: pd.DataFrame,
    time_df: pd.DataFrame,
    user_listens_df: pd.DataFrame,
):
    """Loads given tadaframes into corresponding database tables.

    Parameters
    ----------
    user_df: pd.DataFrame
        Users table data
    artist_df: pd.DataFrame
        Artists table data
    time_df: pd.DataFrame
        Time table data
    user_listens_df: pd.DataFrame
        User Listens table data
    """
    try:
        logger.info("Loading data into database.")
        db_connector = DBConnector()
        db_connector.execute(f"INSERT OR IGNORE INTO {USERS_TABLE_NAME} BY NAME SELECT * FROM user_df")
        db_connector.execute(f"INSERT OR IGNORE INTO {ARTISTS_TABLE_NAME} BY NAME SELECT * FROM artist_df")
        db_connector.execute(f"INSERT OR IGNORE INTO {TIME_TABLE_NAME} BY NAME SELECT * FROM time_df")
        db_connector.execute(f"INSERT OR IGNORE INTO {USER_LISTENS_TABLE_NAME} BY NAME SELECT * FROM user_listens_df")
        logger.debug("Loaded data into database.")
    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "Failed to load data into database: %s error occured: %s",
            type(e),
            e,
        )
        raise e


def normalize_json(file) -> pd.DataFrame:
    """Read each line of the given json file and normalize its content.

    Parameters
    ----------
    file:
        File to read its content

    Returns
    -------
    pd.Dataframe
        normalized json data as dataframe
    """
    lines = file.read().splitlines()
    df = pd.DataFrame(lines, columns=["json_data"])
    df = pd.json_normalize(df["json_data"].apply(json.loads))
    return df


def check_required_columns(df: pd.DataFrame):
    """Checks that given dataframe contains all the required columns
       to create output data model.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe to process

    Raises
    -------
    MissingDataException
        if one or more column(s) are missing.
    """
    logger.info("Checking missing columns.")
    required_cols = set(
        list(USER_COLS.keys()) + list(USER_LISTENS_COLS.keys()) + list(ARTIST_COLS.keys()) + list(DATETIME_COLS.keys())
    )
    missing_cols = [col for col in required_cols if col not in df.columns]
    if len(missing_cols) > 0:
        e = MissingDataException(f"Data has missing columns: {missing_cols}")
        logger.error(
            "Failed to transform data: %s error occured: %s",
            type(e),
            e,
        )
        raise e
    logger.debug("Checked missing columns. Data contains all required columns.")


def prepare_dataframes(df: pd.DataFrame) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Prepares dataframes and rename columns and drop duplicates
    for each corresponding table of the data model.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe to process

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
        Tuple of the prepared dataframes per each table of the data model.
    """
    logger.info("Preparing dataframes.")
    # select required cols and rename column names
    user_df = df[USER_COLS.keys()].rename(columns=USER_COLS)
    artist_df = df[ARTIST_COLS.keys()].rename(columns=ARTIST_COLS)
    time_df = df[DATETIME_COLS.keys()].rename(columns=DATETIME_COLS)
    user_listens_df = df[USER_LISTENS_COLS.keys()].rename(columns=USER_LISTENS_COLS)

    # add extra columns
    user_df["user_id"] = create_hash_key(user_df["user_name"])

    dt = pd.to_datetime(time_df["listen_time"], unit="s")
    time_df["timestamp"] = dt
    time_df["date"] = dt.dt.date.astype("datetime64[ns]")
    time_df["year"] = dt.dt.year
    time_df["month"] = dt.dt.month
    time_df["day"] = dt.dt.day
    time_df["day_name"] = dt.dt.day_name().astype(str)
    time_df["hour"] = dt.dt.hour

    user_listens_df["user_id"] = create_hash_key(user_listens_df["user_name"])
    user_listens_df = user_listens_df.drop(columns="user_name")
    logger.debug("Prepared dataframes.")

    # drop_duplicates
    logger.info("Removing duplicates.")
    user_df = user_df.drop_duplicates()
    artist_df = artist_df.drop_duplicates()
    time_df = time_df.drop_duplicates()
    user_listens_df = user_listens_df.drop_duplicates()
    logger.debug("Removed duplicates.")
    return user_df, artist_df, time_df, user_listens_df


def validate(df: pd.DataFrame, schema: pa.DataFrameSchema) -> pd.DataFrame:
    """Validates given dataframe against given schema.
    Checks missing data, uniqueness, types etc.
    and removes invalid data from given dataframe.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe to validate
    df: pa.DataFrameSchema
        Schema to validate given dataframe

    Returns
    -------
    pd.DataFrame
        Validated and cleaned-up dataframe
    """
    logger.info("Running validation for '%s' data", schema.name)
    df = schema.validate(df, lazy=True)
    logger.debug("Finished validation for '%s' data", schema.name)
    return df


@np.vectorize
def create_hash_key(value: Any) -> str:
    """Vectorized function to create hash of the given input

    Parameters
    ----------
    value: Any
        Value to create hash of it.

    Returns
    -------
    str
        Created hash code as string.
    """
    return str(hashlib.sha256(value.encode()).hexdigest())


if __name__ == "__main__":
    file_path = sys.argv[1]
    logger.info("Starting ETL job.")
    extracted_data = extract(file_path)
    if extracted_data is not None:
        users, artists, times, user_listens = transform(extracted_data)
        load(users, artists, times, user_listens)
    else:
        logger.error("Could not extracted any data to further process.")
    logger.info("ETL job finished successfully.")
