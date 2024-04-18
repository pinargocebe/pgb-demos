"""
Main file for data model schemas.
"""

import numpy as np
import pandera as pa

user_schema = pa.DataFrameSchema(
    {
        "user_name": pa.Column(str, pa.Check(lambda s: s.str.strip() != ""), nullable=False, required=True),
        "user_id": pa.Column(str, nullable=False, required=True),
    },
    unique=["user_id"],
    report_duplicates="exclude_first",
    drop_invalid_rows=True,
    name="user",
)

artist_schema = pa.DataFrameSchema(
    {
        "artist_id": pa.Column(str, nullable=False, required=True),
        "artist_name": pa.Column(str),
    },
    unique=["artist_id"],
    report_duplicates="exclude_first",
    drop_invalid_rows=True,
    name="artist",
)

time_schema = pa.DataFrameSchema(
    {
        "listen_time": pa.Column(np.int64, nullable=False, required=True),
        "timestamp": pa.Column(np.datetime64, nullable=False, required=True),
        "date": pa.Column(np.datetime64, nullable=False, required=True),
        "year": pa.Column(np.int32, nullable=False, required=True),
        "month": pa.Column(np.int32, nullable=False, required=True),
        "day": pa.Column(np.int32, nullable=False, required=True),
        "day_name": pa.Column(str, nullable=False, required=True),
        "hour": pa.Column(np.int32, nullable=False, required=True),
    },
    unique=["listen_time"],
    report_duplicates="exclude_first",
    drop_invalid_rows=True,
    name="time",
)

user_listens_schema = pa.DataFrameSchema(
    {
        "listen_id": pa.Column(str, nullable=False, required=True),
        "listen_time": pa.Column(np.int64, nullable=False, required=True),
        "song_name": pa.Column(str),
        "album_name": pa.Column(str),
        "artist_id": pa.Column(str, nullable=False, required=True),
        "user_id": pa.Column(str, nullable=False, required=True),
    },
    unique=["listen_id"],
    report_duplicates="exclude_first",
    drop_invalid_rows=True,
    name="user_listens",
)
