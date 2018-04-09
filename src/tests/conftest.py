import pytest
from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import findspark
findspark.init()


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing"))
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())
    return sc


@pytest.fixture(scope="session")
def hive_context(spark_context):
    """  fixture for creating a Hive Context. Creating a fixture enables it to be reused across all
        tests in a session
    Args:
        spark_context: spark_context fixture
    Returns:
        HiveContext for tests
    """
    return HiveContext(spark_context)

@pytest.fixture(scope="session")
def spark_session(request):
    """  fixture for creating a Hive Context. Creating a fixture enables it to be reused across all
        tests in a session
    Args:
        spark_context: spark_context fixture
    Returns:
        HiveContext for tests
    """
    spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
    yield spark_session
    spark_session.stop()
