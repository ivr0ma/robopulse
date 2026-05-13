from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    """
    Создает стандартную SparkSession для jobs проекта Robopulse.

    Returns:
        Активная SparkSession с именем приложения SparkAppRobopulse.
    """
    return SparkSession.builder.appName("SparkAppRobopulse").getOrCreate()
