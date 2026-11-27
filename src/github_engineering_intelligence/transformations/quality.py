
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from github_engineering_intelligence.transformations.schemas import (
    GITHUB_EVENT_SCHEMA,
)
from github_engineering_intelligence.transformations.spark_session import (
    create_spark_session,
)
from github_engineering_intelligence.transformations.flatten import (
    flatten_events,
)


def validate_events(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:

    validated = df.withColumn(
        "rejection_reason",

        F.when(
            F.col("_corrupt_record").isNotNull(),
            F.lit("malformed_json"),
        ).when(
            F.col("event_id").isNull()
            | (F.trim(F.col("event_id")) == ""),
            F.lit("missing_event_id"),
        ).when(
            F.col("event_type").isNull()
            | (F.trim(F.col("event_type")) == ""),
            F.lit("missing_event_type"),
        ).when(
            F.col("event_at").isNull(),
            F.lit("invalid_event_timestamp"),
        ).when(
            F.col("repo_id").isNull(),
            F.lit("missing_repo_id"),
        ).when(
            F.col("repo_name").isNull()
            | (F.trim(F.col("repo_name")) == ""),
            F.lit("missing_repo_name"),
        ).when(
            F.col("actor_id").isNull(),
            F.lit("missing_actor_id"),
        ).otherwise(
            F.lit(None).cast("string")
        )
    )

    valid_df = validated.filter(
        F.col("rejection_reason").isNull()
    ).drop(
        "rejection_reason",
        "_corrupt_record",
    )

    invalid_df = validated.filter(
        F.col("rejection_reason").isNotNull()
    )

    return valid_df, invalid_df


def deduplicate_events(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["event_id"])


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    flat_df = None

    try:
        raw_df = (
            spark.read
            .schema(GITHUB_EVENT_SCHEMA)
            .option("mode", "PERMISSIVE")
            .json("data/bronze/*.json.gz")
        )

        flat_df = flatten_events(raw_df).cache()

        total_count = flat_df.count()

        valid_df, invalid_df = validate_events(flat_df)

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        clean_df = deduplicate_events(valid_df)

        clean_count = clean_df.count()
        duplicate_count = valid_count - clean_count

        print("\nData quality summary")
        print("------------------------")
        print(f"Total records:     {total_count}")
        print(f"Valid records:     {valid_count}")
        print(f"Invalid records:   {invalid_count}")
        print(f"Duplicate records: {duplicate_count}")
        print(f"Clean records:     {clean_count}")

        print("\nRejection reasons:")
        invalid_df.groupBy(
            "rejection_reason"
        ).count().show(truncate=False)

        print("\nDuplicate event IDs:")
        valid_df.groupBy("event_id").count().filter(
            F.col("count") > 1
        ).show(20, truncate=False)

        assert total_count == valid_count + invalid_count

        assert total_count == (
            clean_count
            + duplicate_count
            + invalid_count
        )

        print("\nData reconciliation: PASSED")

    finally:
        if flat_df is not None:
            flat_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
