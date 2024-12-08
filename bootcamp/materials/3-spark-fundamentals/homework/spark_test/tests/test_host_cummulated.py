from chispa.dataframe_comparer import assert_df_equality
from ..jobs.host_cummulated_job import do_host_cummulated_transform
from collections import namedtuple
from pyspark.sql.functions import lit
from pyspark.sql.types import DecimalType
from datetime import datetime

# Define namedtuples for mock data structures
Events = namedtuple("Events", "host event_time")
HostCummulated = namedtuple("HostCummulated", "host start_date host_activity_datelist")

def test_host_cummulated_initial(spark):
    # Mock data for events
    events_mock_data = [
        Events(host="host1", event_time=datetime.strptime("2023-01-01", "%Y-%m-%d").date()),
        Events(host="host2", event_time=datetime.strptime("2023-01-01", "%Y-%m-%d").date()),
        Events(host="host1", event_time=datetime.strptime("2023-01-02", "%Y-%m-%d").date()),
    ]

    host_cummulated_mock_data = [
        HostCummulated(
            host="example.com",
            start_date=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            host_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date()]
        )
    ]
    
    # Create DataFrames from mock data
    events_mock_df = spark.createDataFrame(events_mock_data)
    host_cummulated_mock_df = spark.createDataFrame(host_cummulated_mock_data)
    # Remove data from host_cummulated_mock_df
    host_cummulated_mock_df = host_cummulated_mock_df.filter(lit(False))
    
    # Call the function to be tested
    output_df = do_host_cummulated_transform(spark, host_cummulated_mock_df, events_mock_df, "2023-01-01")
    
    # Define the expected result
    expected_result = [
        HostCummulated(
            host="host1",
            start_date=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            host_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date()]
        ),
        HostCummulated(
            host="host2",
            start_date=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            host_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date()]
        )
    ]

    # Compare the result with the expected result
    expected_result_df = spark.createDataFrame(expected_result)
    assert_df_equality(output_df, expected_result_df, ignore_nullable=True)

def test_host_cummulated_cummulative(spark):
    # Mock data for events
    events_mock_data = [
        Events(host="host1", event_time=datetime.strptime("2023-01-02", "%Y-%m-%d").date()),
        Events(host="host2", event_time=datetime.strptime("2023-01-02", "%Y-%m-%d").date()),
        Events(host="host1", event_time=datetime.strptime("2023-01-03", "%Y-%m-%d").date()),
    ]

    host_cummulated_mock_data = [
        HostCummulated(
            host="host1",
            start_date=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            host_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date()]
        )
    ]
    
    # Create DataFrames from mock data
    events_mock_df = spark.createDataFrame(events_mock_data)
    host_cummulated_mock_df = spark.createDataFrame(host_cummulated_mock_data)
    
    # Call the function to be tested
    output_df = do_host_cummulated_transform(spark, host_cummulated_mock_df, events_mock_df, "2023-01-02")
    
    # Define the expected result
    expected_result = [
        HostCummulated(
            host="host1",
            start_date=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            host_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date(), datetime.strptime("2023-01-02", "%Y-%m-%d").date()]
        ),
        HostCummulated(
            host="host2",
            start_date=datetime.strptime("2023-01-02", "%Y-%m-%d").date(),
            host_activity_datelist=[datetime.strptime("2023-01-02", "%Y-%m-%d").date()]
        )
    ]

    # Compare the result with the expected result
    expected_result_df = spark.createDataFrame(expected_result)
    assert_df_equality(output_df, expected_result_df, ignore_nullable=True)