
from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("GitHub Engineering Intelligence")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "true")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = create_spark_session()

    try:
        print(f"Spark version: {spark.version}")

        df = spark.range(10)
        df.show()

    finally:
        spark.stop()
