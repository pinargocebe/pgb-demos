import hashlib
import logging

import duckdb
import numpy as np
import pandas as pd
import pytest
from conftest import TEST_DB
from pandas.testing import assert_frame_equal

from app.db_connector import ARTISTS_TABLE_NAME, TIME_TABLE_NAME, USER_LISTENS_TABLE_NAME, USERS_TABLE_NAME
from app.etl import check_required_columns, extract, load, normalize_json, prepare_dataframes, transform, validate
from app.exceptions import MissingDataException, UnsupportedFileFormatException
from app.schema import artist_schema, time_schema, user_listens_schema, user_schema

dataset_df = pd.DataFrame(
    [
        {
            "listened_at": 1555286560,
            "recording_msid": "1e1b2aa0-b2db-42ed-a8ba-89c303499408",
            "user_name": "NichoBI",
            "track_metadata.additional_info.release_msid": "34dbfc73-31e4-45c3-9d6e-04d8b6c5fd4a",
            "track_metadata.additional_info.release_mbid": None,
            "track_metadata.additional_info.recording_mbid": None,
            "track_metadata.additional_info.release_group_mbid": None,
            "track_metadata.additional_info.artist_mbids": [],
            "track_metadata.additional_info.tags": [],
            "track_metadata.additional_info.work_mbids": [],
            "track_metadata.additional_info.isrc": None,
            "track_metadata.additional_info.spotify_id": None,
            "track_metadata.additional_info.tracknumber": None,
            "track_metadata.additional_info.track_mbid": None,
            "track_metadata.additional_info.artist_msid": "f1d39567-27e7-40af-852a-abaed88ec838",
            "track_metadata.additional_info.recording_msid": "1e1b2aa0-b2db-42ed-a8ba-89c303499408",
            "track_metadata.artist_name": "Withered Hand",
            "track_metadata.track_name": "Love In the Time of Ecstacy",
            "track_metadata.release_name": "Good News",
        },
        {
            "listened_at": 1555286378,
            "recording_msid": "283062c8-75e2-406a-8c5e-f38136aa5a68",
            "user_name": "NichoBI",
            "track_metadata.additional_info.release_msid": "34dbfc73-31e4-45c3-9d6e-04d8b6c5fd4a",
            "track_metadata.additional_info.release_mbid": None,
            "track_metadata.additional_info.recording_mbid": None,
            "track_metadata.additional_info.release_group_mbid": None,
            "track_metadata.additional_info.artist_mbids": [],
            "track_metadata.additional_info.tags": [],
            "track_metadata.additional_info.work_mbids": [],
            "track_metadata.additional_info.isrc": None,
            "track_metadata.additional_info.spotify_id": None,
            "track_metadata.additional_info.tracknumber": None,
            "track_metadata.additional_info.track_mbid": None,
            "track_metadata.additional_info.artist_msid": "f1d39567-27e7-40af-852a-abaed88ec838",
            "track_metadata.additional_info.recording_msid": "283062c8-75e2-406a-8c5e-f38136aa5a68",
            "track_metadata.artist_name": "Withered Hand",
            "track_metadata.track_name": "Cornflake",
            "track_metadata.release_name": "Good News",
        },
        {
            "listened_at": 1555286137,
            "recording_msid": "8fe0c93d-f44a-4159-b069-5cc3c2e9c16b",
            "user_name": "NichoBI",
            "track_metadata.additional_info.release_msid": "34dbfc73-31e4-45c3-9d6e-04d8b6c5fd4a",
            "track_metadata.additional_info.release_mbid": None,
            "track_metadata.additional_info.recording_mbid": None,
            "track_metadata.additional_info.release_group_mbid": None,
            "track_metadata.additional_info.artist_mbids": [],
            "track_metadata.additional_info.tags": [],
            "track_metadata.additional_info.work_mbids": [],
            "track_metadata.additional_info.isrc": None,
            "track_metadata.additional_info.spotify_id": None,
            "track_metadata.additional_info.tracknumber": None,
            "track_metadata.additional_info.track_mbid": None,
            "track_metadata.additional_info.artist_msid": "f1d39567-27e7-40af-852a-abaed88ec838",
            "track_metadata.additional_info.recording_msid": "8fe0c93d-f44a-4159-b069-5cc3c2e9c16b",
            "track_metadata.artist_name": "Withered Hand",
            "track_metadata.track_name": "Providence",
            "track_metadata.release_name": "Good News",
        },
    ]
)

expected_user_df = pd.DataFrame(
    [
        {
            "user_name": "NichoBI",
            "user_id": str(hashlib.sha256("NichoBI".encode()).hexdigest()),
        },
    ]
)

invalid_user_df = pd.DataFrame(
    [
        {
            "user_name": None,
            "user_id": str(hashlib.sha256("NichoBI".encode()).hexdigest()),
        },
    ]
)

expected_artist_df = pd.DataFrame(
    [
        {
            "artist_id": "f1d39567-27e7-40af-852a-abaed88ec838",
            "artist_name": "Withered Hand",
        },
    ]
)

invalid_artist_df = pd.DataFrame(
    [
        {
            "artist_id": None,
            "artist_name": "Withered Hand",
        },
    ]
)

expected_time_df = pd.DataFrame(
    [
        {
            "listen_time": 1555286560,
            "timestamp": pd.to_datetime(1555286560, unit="s"),
            "date": pd.to_datetime(1555286560, unit="s").date(),
            "year": 2019,
            "month": 4,
            "day": 15,
            "day_name": "Monday",
            "hour": 0,
        },
        {
            "listen_time": 1555286378,
            "timestamp": pd.to_datetime(1555286378, unit="s"),
            "date": pd.to_datetime(1555286378, unit="s").date(),
            "year": 2019,
            "month": 4,
            "day": 14,
            "day_name": "Sunday",
            "hour": 23,
        },
        {
            "listen_time": 1555286137,
            "timestamp": pd.to_datetime(1555286137, unit="s"),
            "date": pd.to_datetime(1555286137, unit="s").date(),
            "year": 2019,
            "month": 4,
            "day": 14,
            "day_name": "Sunday",
            "hour": 23,
        },
    ]
)
expected_time_df = expected_time_df.astype(
    {
        "timestamp": "datetime64[ns]",
        "date": "datetime64[ns]",
        "year": "int32",
        "month": "int32",
        "day": "int32",
        "day_name": "str",
        "hour": "int32",
    }
)

invalid_time_df = pd.DataFrame(
    [
        {
            "listen_time": 1555286560,
            "year": 2019,
            "month": 4,
            "day": 15,
            "day_name": "Monday",
            "hour": 0,
        },
        {
            "listen_time": 1555286560,
            "year": 2019,
            "month": 4,
            "day": 14,
            "day_name": "Sunday",
            "hour": 23,
        },
        {
            "listen_time": 1555286137,
            "year": 2019,
            "month": np.NaN,
            "day": 14,
            "day_name": "Sunday",
            "hour": 23,
        },
    ]
)

expected_user_listens_df = pd.DataFrame(
    [
        {
            "listen_id": "1e1b2aa0-b2db-42ed-a8ba-89c303499408",
            "listen_time": 1555286560,
            "song_name": "Love In the Time of Ecstacy",
            "album_name": "Good News",
            "artist_id": "f1d39567-27e7-40af-852a-abaed88ec838",
            "user_id": str(hashlib.sha256("NichoBI".encode()).hexdigest()),
        },
        {
            "listen_id": "283062c8-75e2-406a-8c5e-f38136aa5a68",
            "listen_time": 1555286378,
            "song_name": "Cornflake",
            "album_name": "Good News",
            "artist_id": "f1d39567-27e7-40af-852a-abaed88ec838",
            "user_id": str(hashlib.sha256("NichoBI".encode()).hexdigest()),
        },
        {
            "listen_id": "8fe0c93d-f44a-4159-b069-5cc3c2e9c16b",
            "listen_time": 1555286137,
            "song_name": "Providence",
            "album_name": "Good News",
            "artist_id": "f1d39567-27e7-40af-852a-abaed88ec838",
            "user_id": str(hashlib.sha256("NichoBI".encode()).hexdigest()),
        },
    ]
)

invalid_user_listens_df = pd.DataFrame(
    [
        {
            "listen_id": "1e1b2aa0-b2db-42ed-a8ba-89c303499408",
            "listen_time": 1555286560,
            "song_name": "Love In the Time of Ecstacy",
            "album_name": "Good News",
            "artist_id": "f1d39567-27e7-40af-852a-abaed88ec838",
            "user_id": str(hashlib.sha256("NichoBI".encode()).hexdigest()),
        },
        {
            "listen_id": "1e1b2aa0-b2db-42ed-a8ba-89c303499408",
            "listen_time": 1555286378,
            "song_name": "Cornflake",
            "album_name": "Good News",
            "artist_id": "f1d39567-27e7-40af-852a-abaed88ec838",
            "user_id": str(hashlib.sha256("NichoBI".encode()).hexdigest()),
        },
        {
            "listen_id": "8fe0c93d-f44a-4159-b069-5cc3c2e9c16b",
            "listen_time": 1555286137,
            "song_name": "Providence",
            "album_name": "Good News",
            "artist_id": np.NaN,
            "user_id": str(hashlib.sha256("NichoBI".encode()).hexdigest()),
        },
    ]
)


def test_normalize_json():
    with open("tests/data/dataset.txt") as f:
        res = normalize_json(f)

    assert len(res) == 3
    assert_frame_equal(dataset_df, res)


def test_extract_zip():
    res = extract("tests/data/dataset.zip")

    assert len(res) == 3
    assert_frame_equal(dataset_df, res)


def test_extract_txt():
    res = extract("tests/data/dataset.txt")

    assert len(res) == 3
    assert_frame_equal(dataset_df, res)


def test_extract_unsupported_format():
    with pytest.raises(UnsupportedFileFormatException) as exc:
        extract("tests/data/dataset.xml")

    assert "Unsupported file format, provide '.zip' or '.txt' file/path as input." in exc.value.error


def test_extract_corrupted_file(caplog):
    file_path = "tests/data/dataset_corrupted.txt"
    caplog.set_level(logging.INFO)
    with pytest.raises(Exception):
        extract(file_path)

    assert len(caplog.records) == 2
    assert caplog.messages == [
        f"Extracting file '{file_path}'",
        f"Failed to extract data from file: '{file_path}', <class 'json.decoder.JSONDecodeError'> error occured: Unterminated string starting at: line 1 column 135 (char 134)",
    ]


def test_check_missing_colums():
    missing_col_df = pd.DataFrame(
        [
            {
                "listened_at": 1555286560,
                "recording_msid": "1e1b2aa0-b2db-42ed-a8ba-89c303499408",
                "user_name": "NichoBI",
            },
            {
                "listened_at": 1555286378,
                "recording_msid": "283062c8-75e2-406a-8c5e-f38136aa5a68",
                "user_name": "NichoBI",
            },
            {
                "user_name": "NichoBI",
            },
        ]
    )
    with pytest.raises(MissingDataException) as exc:
        check_required_columns(missing_col_df)

    expected_error = "Data has missing columns:"
    assert expected_error in exc.value.error
    expected_missing_cols = [
        "track_metadata.additional_info.artist_msid",
        "track_metadata.release_name",
        "track_metadata.artist_name",
        "track_metadata.track_name",
    ]
    for col in expected_missing_cols:
        assert col in exc.value.error


def test_check_non_missing_colums(caplog):
    caplog.set_level(logging.DEBUG)
    check_required_columns(dataset_df)
    assert len(caplog.records) == 2
    assert caplog.messages == [
        "Checking missing columns.",
        "Checked missing columns. Data contains all required columns.",
    ]


def test_prepare_dataframes():
    user_df, artist_df, time_df, user_listens_df = prepare_dataframes(dataset_df)

    assert_frame_equal(expected_user_df, user_df, check_dtype=False)
    assert_frame_equal(expected_artist_df, artist_df, check_dtype=False)
    assert_frame_equal(expected_time_df, time_df, check_dtype=False)
    assert_frame_equal(expected_user_listens_df, user_listens_df, check_dtype=False)


def test_validate_user():
    # valid
    try:
        validate(expected_user_df, user_schema)
    except Exception as e:
        assert False, f"User schema raised an exception {e}"

    # invalid user df
    res = validate(invalid_user_df, user_schema)
    assert len(res) == 0

    # non_unique id
    non_unique_user_df = pd.DataFrame(
        [
            {
                "user_name": "TEST NAME",
                "user_id": str(hash("TEST NAME")),
            },
            {
                "user_name": "NichoBI",
                "user_id": str(hash("TEST NAME")),
            },
        ]
    )
    res = validate(non_unique_user_df, user_schema)
    assert len(res) == 1


def test_validate_artist():
    # valid
    try:
        validate(expected_artist_df, artist_schema)
    except Exception as e:
        assert False, f"Artist schema raised an exception {e}"

    # invalid artist df
    res = validate(invalid_artist_df, artist_schema)
    assert len(res) == 0


def test_validate_time():
    # valid
    try:
        validate(expected_time_df, time_schema)
    except Exception as e:
        assert False, f"Time schema raised an exception {e}"

    # invalid time df
    res = validate(invalid_time_df, time_schema)
    assert len(res) == 1


def test_validate_user_listens():
    # valid
    try:
        validate(expected_user_listens_df, user_listens_schema)
    except Exception as e:
        assert False, f"User Listens schema raised an exception {e}"

    # invalid user listens df
    res = validate(invalid_user_listens_df, user_listens_schema)
    assert len(res) == 1


def test_transform():
    users, artists, times, user_listens = transform(dataset_df)

    assert_frame_equal(expected_user_df, users)
    assert_frame_equal(expected_artist_df, artists)
    assert_frame_equal(expected_time_df, times)
    assert_frame_equal(expected_user_listens_df, user_listens)


def test_load(setup, setenvvar):
    users, artists, times, user_listens = transform(dataset_df)
    load(users, artists, times, user_listens)
    with duckdb.connect(TEST_DB) as conn:
        user_df = conn.sql(f"SELECT * from {USERS_TABLE_NAME}").df()
        artist_df = conn.sql(f"SELECT * from {ARTISTS_TABLE_NAME}").df()
        time_df = conn.sql(f"SELECT * from {TIME_TABLE_NAME}").df()
        user_listens_df = conn.sql(f"SELECT * from {USER_LISTENS_TABLE_NAME}").df()

    assert_frame_equal(expected_user_df.sort_index(axis=1), user_df.sort_index(axis=1))
    assert_frame_equal(expected_artist_df.sort_index(axis=1), artist_df.sort_index(axis=1))
    time_df["date"] = time_df["date"].astype("datetime64[ns]")
    assert_frame_equal(expected_time_df.sort_index(axis=1), time_df.sort_index(axis=1))
    assert_frame_equal(expected_user_listens_df.sort_index(axis=1), user_listens_df.sort_index(axis=1))


def test_load_already_ingested_data(setup, setenvvar):
    users, artists, times, user_listens = transform(dataset_df)
    load(users, artists, times, user_listens)
    load(users, artists, times, user_listens)

    with duckdb.connect(TEST_DB) as conn:
        user_df = conn.sql(f"SELECT * from {USERS_TABLE_NAME}").df()
        artist_df = conn.sql(f"SELECT * from {ARTISTS_TABLE_NAME}").df()
        time_df = conn.sql(f"SELECT * from {TIME_TABLE_NAME}").df()
        user_listens_df = conn.sql(f"SELECT * from {USER_LISTENS_TABLE_NAME}").df()

    assert_frame_equal(expected_user_df.sort_index(axis=1), user_df.sort_index(axis=1))
    assert_frame_equal(expected_artist_df.sort_index(axis=1), artist_df.sort_index(axis=1))
    time_df["date"] = time_df["date"].astype("datetime64[ns]")
    assert_frame_equal(expected_time_df.sort_index(axis=1), time_df.sort_index(axis=1))
    assert_frame_equal(expected_user_listens_df.sort_index(axis=1), user_listens_df.sort_index(axis=1))
