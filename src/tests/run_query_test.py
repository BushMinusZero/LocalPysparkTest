import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.jobs import run_query


pytestmark = pytest.mark.usefixtures("spark_context", "hive_context")


def test_it(spark_context, hive_context):
    """ test run_test_query with spark_context and hive_context
    Args:
        spark_context: test fixture SparkContext
        hive_context: test fixture HiveContext
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

    input_rdd = spark_context.parallelize(test_input, 1)
    input_df = hive_context.createDataFrame(input_rdd, schema)

    results = run_query.job_runner(input_df)

    expected_results = 1
    assert results.count() == expected_results
