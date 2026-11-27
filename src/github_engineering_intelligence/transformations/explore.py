
from github_engineering_intelligence.transformations.spark_session import (
    create_spark_session,
)

from github_engineering_intelligence.transformations.schemas import (
    GITHUB_EVENT_SCHEMA,
)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = (
            spark.read
            .schema(GITHUB_EVENT_SCHEMA)
            .option("mode", "PERMISSIVE")
            .json("data/bronze/*.json.gz")
        )

        print("\nDataset schema:")
        df.printSchema()

        print("\nTotal events:")
        print(df.count())

        print("\nSample events:")
        df.select(
            "id",
            "type",
            "repo.name",
            "created_at",
        ).show(10, truncate=False)

        print("\nEvent distribution:")
        df.groupBy("type").count().orderBy(
            "count", ascending=False
        ).show(30, truncate=False)

        print("\nUnique repositories:")
        print(df.select("repo.id").distinct().count())

        print("\nUnique actors:")
        print(df.select("actor.id").distinct().count())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
