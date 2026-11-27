
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)


ISSUE_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("number", LongType()),
    StructField("state", StringType()),
    StructField("created_at", StringType()),
    StructField("closed_at", StringType()),
])


PULL_REQUEST_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("number", LongType()),
    StructField("state", StringType()),
    StructField("merged", BooleanType()),
    StructField("created_at", StringType()),
    StructField("closed_at", StringType()),
    StructField("merged_at", StringType()),
])


GITHUB_EVENT_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("created_at", StringType()),
    StructField("public", BooleanType()),

    StructField("actor", StructType([
        StructField("id", LongType()),
        StructField("login", StringType()),
    ])),

    StructField("repo", StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
    ])),

    StructField("org", StructType([
        StructField("id", LongType()),
        StructField("login", StringType()),
    ])),

    StructField("payload", StructType([
        StructField("action", StringType()),
        StructField("issue", ISSUE_SCHEMA),
        StructField("pull_request", PULL_REQUEST_SCHEMA),
    ])),

    StructField("_corrupt_record", StringType()),
])
