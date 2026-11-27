
from pathlib import Path

from github_engineering_intelligence.transformations.schemas import (
    GITHUB_EVENT_SCHEMA,
)
from github_engineering_intelligence.transformations.spark_session import (
    create_spark_session,
)
from github_engineering_intelligence.transformations.flatten import (
    flatten_events,
)
from github_engineering_intelligence.transformations.quality import (
    validate_events,
    deduplicate_events,
)


BRONZE_PATH = "data/bronze/*.json.gz"
SILVER_PATH = Path("data/silver/events")
QUARANTINE_PATH = Path("data/quarantine/events")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    flat_df = None
    clean_df = None

    try:
        raw_df = (
            spark.read
            .schema(GITHUB_EVENT_SCHEMA)
            .option("mode", "PERMISSIVE")
            .json(BRONZE_PATH)
        )

        flat_df = flatten_events(raw_df).cache()

        valid_df, invalid_df = validate_events(flat_df)

        clean_df = deduplicate_events(valid_df).cache()

        raw_count = flat_df.count()
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        clean_count = clean_df.count()

        duplicate_count = valid_count - clean_count

        if raw_count != (
            clean_count + invalid_count + duplicate_count
        ):
            raise RuntimeError("Data reconciliation failed")

        print("\nWriting Silver dataset...")

        (
            clean_df.write
            .mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("event_date")
            .parquet(str(SILVER_PATH))
        )

        print("\nWriting quarantine dataset...")

        (
            invalid_df.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(QUARANTINE_PATH))
        )

        print("\nTransformation summary")
        print("--------------------------")
        print(f"Raw records:       {raw_count}")
        print(f"Clean records:     {clean_count}")
        print(f"Invalid records:   {invalid_count}")
        print(f"Duplicate records: {duplicate_count}")
        print(f"Silver location:   {SILVER_PATH}")
        print(f"Quarantine:        {QUARANTINE_PATH}")

        print("\nVerifying persisted datasets...")

        silver_df = spark.read.parquet(
            str(SILVER_PATH)
        )

        quarantine_df = spark.read.parquet(
            str(QUARANTINE_PATH)
        )

        if silver_df.count() != clean_count:
            raise RuntimeError("Silver count mismatch")

        if quarantine_df.count() != invalid_count:
            raise RuntimeError("Quarantine count mismatch")

        print("Silver verification: PASSED")
        print("Quarantine verification: PASSED")

    finally:
        if clean_df is not None:
            clean_df.unpersist()

        if flat_df is not None:
            flat_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
