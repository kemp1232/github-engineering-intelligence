
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from github_engineering_intelligence.transformations.schemas import (
    GITHUB_EVENT_SCHEMA,
)
from github_engineering_intelligence.transformations.spark_session import (
    create_spark_session,
)


def flatten_events(df: DataFrame) -> DataFrame:
    flattened = df.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.try_to_timestamp("created_at").alias("event_at"),
        F.col("public").alias("is_public"),

        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),

        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),

        F.col("org.id").alias("org_id"),
        F.col("org.login").alias("org_login"),

        F.col("payload.action").alias("action"),

        F.col("payload.issue.id").alias("issue_id"),
        F.col("payload.issue.number").alias("issue_number"),
        F.col("payload.issue.state").alias("issue_state"),

        F.try_to_timestamp(
            "payload.issue.created_at"
        ).alias("issue_created_at"),

        F.try_to_timestamp(
            "payload.issue.closed_at"
        ).alias("issue_closed_at"),

        F.col("payload.pull_request.id").alias("pr_id"),
        F.col("payload.pull_request.number").alias("pr_number"),
        F.col("payload.pull_request.state").alias("pr_state"),
        F.col("payload.pull_request.merged").alias("pr_merged"),

        F.try_to_timestamp(
            "payload.pull_request.created_at"
        ).alias("pr_created_at"),

        F.try_to_timestamp(
            "payload.pull_request.closed_at"
        ).alias("pr_closed_at"),

        F.try_to_timestamp(
            "payload.pull_request.merged_at"
        ).alias("pr_merged_at"),

        F.input_file_name().alias("source_file"),
        F.col("_corrupt_record"),
    )

    return (
        flattened
        .withColumn("event_date", F.to_date("event_at"))
        .withColumn("event_hour", F.hour("event_at"))
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        raw_df = (
            spark.read
            .schema(GITHUB_EVENT_SCHEMA)
            .option("mode", "PERMISSIVE")
            .json("data/bronze/*.json.gz")
        )

        flat_df = flatten_events(raw_df)

        print("\nFlattened schema:")
        flat_df.printSchema()

        print("\nRaw event count:")
        print(raw_df.count())

        print("\nFlattened event count:")
        print(flat_df.count())

        print("\nSample flattened events:")
        flat_df.select(
            "event_id",
            "event_type",
            "repo_name",
            "event_at",
            "action",
        ).show(10, truncate=False)

        print("\nEvents per hour:")
        flat_df.groupBy(
            "event_date",
            "event_hour",
        ).count().orderBy(
            "event_date",
            "event_hour",
        ).show(30)

        print("\nMissing event timestamps:")
        print(
            flat_df.filter(
                F.col("event_at").isNull()
            ).count()
        )

        print("\nPull request action distribution:")
        flat_df.filter(
            F.col("event_type") == "PullRequestEvent"
        ).groupBy(
            "action"
        ).count().orderBy(
            "count", ascending=False
        ).show(30)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
