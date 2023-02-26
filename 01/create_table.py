import os

import delta
import mimesis
import pyspark

from pyspark.sql import SparkSession


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


def create_dataset(i: int = 100) -> list[dict]:
    fake = mimesis.Generic()
    output = [
        {
            "name": fake.person.name(),
            "surname": fake.person.surname(),
            "birthday": fake.datetime.date(1960, 2010),
            "email": fake.person.email(),
            "country": fake.address.country(),
            "state": fake.address.state(),
            "city": fake.address.city(),
        }
        for _ in range(i)
    ]
    return output


def main():
    dir = os.getcwd()
    spark = get_spark()

    df = spark.createDataFrame(create_dataset(i=1_000_000))

    df = df.select(
        df.name, df.surname, df.birthday, df.email, df.country, df.state, df.city
    )
    df.write.format("delta").mode("overwrite").save(dir + os.sep + "people")
    df.show()


if __name__ == "__main__":
    main()
