from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("SparkAppRobopulse").getOrCreate()
