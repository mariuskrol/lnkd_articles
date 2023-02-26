import os

import delta
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import asc


def get_spark() -> SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder.appName("TestApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    dir = os.getcwd()
    spark = get_spark()

    df = spark.read.format("delta").load(dir + os.sep + "people")
    df.groupBy("state").count().sort(asc("state")).show()


if __name__ == "__main__":
    main()
