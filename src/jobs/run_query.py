from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def load_hive_table(spark_session, table_name):
    return spark_session.read.table(table_name)


def job_runner(data_frame):

    return data_frame\
        .filter(col("country") == "India")\
        .select(col("id"))


if __name__ == "__main__":

    test_table = "some_test_table"

    spark = SparkSession\
        .builder\
        .appName("Test Query")\
        .getOrCreate()

    frame = load_hive_table(spark, test_table)
    job_runner(frame)

    spark.stop()
