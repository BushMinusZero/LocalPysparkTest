import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.jobs import run_query


pytestmark = pytest.mark.usefixtures("spark_session")


def test_it(spark_session):
    """ test run_test_query with spark_session
    Args:
        spark_session: test fixture SparkSession
    """

    test_input = [
        {'id': 1, 'country': 'Japan'},
        {'id': 2, 'country': 'India'},
        {'id': 3, 'country': 'United States'},
        {'id': 4, 'country': 'Canada'}
    ]

    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('country', StringType(), True)
    ])

    input_rdd = spark_session.sparkContext.parallelize(test_input, 1)
    input_df = spark_session.createDataFrame(input_rdd, schema)

    results = run_query.job_runner(input_df)

    expected_results = 1
    assert results.count() == expected_results
