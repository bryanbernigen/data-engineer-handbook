from chispa.dataframe_comparer import assert_df_equality
from ..jobs.device_activity_datelist_job import do_device_activity_datelist_transform
from collections import namedtuple
from pyspark.sql.functions import lit
from pyspark.sql.types import DecimalType
from datetime import datetime

# Define namedtuples for mock data structures
Events = namedtuple("Events", "user_id device_id event_time")
Devices = namedtuple("Devices", "device_id browser_type")
DeviceActivityDatelist = namedtuple("DeviceActivityDatelist", "user_id browser_type valid_date device_activity_datelist")

def test_device_activity_datelist_initial(spark):
    # Mock data for devices
    devices_mock_data = [
        Devices(device_id=1, browser_type="Browser1"),
        Devices(device_id=2, browser_type="Browser2"),
        Devices(device_id=3, browser_type="Browser3")
    ]
    
    # Mock data for events (user activity)
    events_mock_data = [
        Events(user_id=1, device_id=1, event_time="2023-01-01"),
        Events(user_id=1, device_id=2, event_time="2023-01-01"),
        Events(user_id=1, device_id=3, event_time="2023-01-02"),
    ]

    device_activity_datelist_mock_data = [
        DeviceActivityDatelist(
            user_id=1,
            browser_type="Browser1",
            valid_date=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            device_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date()]
        )
    ]
    
    # Create DataFrames from mock data
    devices_mock_df = spark.createDataFrame(devices_mock_data)
    events_mock_df = spark.createDataFrame(events_mock_data)
    device_activity_datelist_mock_df = spark.createDataFrame(device_activity_datelist_mock_data)
    # Remove data from device_activity_datelist_mock_df
    device_activity_datelist_mock_df = device_activity_datelist_mock_df.filter(lit(False))
    
    # Perform the transformation using the function under test
    output_df = do_device_activity_datelist_transform(spark, events_mock_df, devices_mock_df, device_activity_datelist_mock_df, "2023-01-01", "2023-01-01")
    
    # Mock data for the expected output
    expected_output_data = [
        DeviceActivityDatelist(
            user_id=1,
            browser_type="Browser2",
            valid_date=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            device_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date()]
        ),
        DeviceActivityDatelist(
            user_id=1,
            browser_type="Browser1",
            valid_date=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            device_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date()]
        ),
    ]
    
    # Create the expected output DataFrame
    expected_output_df = spark.createDataFrame(expected_output_data)
    expected_output_df = expected_output_df.withColumn("user_id", expected_output_df.user_id.cast(DecimalType(10,0)))
    
    # Assert that the output DataFrame matches the expected output
    assert_df_equality(output_df, expected_output_df, ignore_nullable=True)

def test_device_activity_datelist_cummulative(spark):
    # Mock data for devices
    devices_mock_data = [
        Devices(device_id=1, browser_type="Browser1"),
        Devices(device_id=2, browser_type="Browser2"),
        Devices(device_id=3, browser_type="Browser3")
    ]
    
    # Mock data for events (user activity)
    events_mock_data = [
        Events(user_id=1, device_id=1, event_time="2023-01-02"),
        Events(user_id=1, device_id=2, event_time="2023-01-02"),
        Events(user_id=1, device_id=1, event_time="2023-01-03"),
    ]
    
    # Mock data for existing cumulative device activity
    device_activity_datelist_mock_data = [
        DeviceActivityDatelist(
            user_id=1,
            browser_type="Browser1",
            valid_date=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            device_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date()]
        ),
        DeviceActivityDatelist(
            user_id=2,
            browser_type="Browser1",
            valid_date=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            device_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date()]
        )
    ]
    
    # Create DataFrames from mock data
    devices_mock_df = spark.createDataFrame(devices_mock_data)
    events_mock_df = spark.createDataFrame(events_mock_data)
    device_activity_datelist_mock_df = spark.createDataFrame(device_activity_datelist_mock_data)
    
    # Perform the transformation using the function under test
    result_df = do_device_activity_datelist_transform(
        spark, events_mock_df, devices_mock_df, device_activity_datelist_mock_df, "2023-01-01", "2023-01-02"
    )

    # Define expected output values with schema1 formatting:
    expected_values = [
        # Adjusting the user_id to DecimalType(10,0), valid_date as DateType, and device_activity_datelist as DateType[]
        DeviceActivityDatelist(
            user_id=1,
            browser_type="Browser1",
            valid_date=datetime.strptime("2023-01-02", "%Y-%m-%d").date(),  # DateType
            device_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date(), 
                                      datetime.strptime("2023-01-02", "%Y-%m-%d").date()]  # Array of DateType
        ),
        DeviceActivityDatelist(
            user_id=1,
            browser_type="Browser2",
            valid_date=datetime.strptime("2023-01-02", "%Y-%m-%d").date(),  # DateType
            device_activity_datelist=[datetime.strptime("2023-01-02", "%Y-%m-%d").date()]  # Array of DateType
        ),
        DeviceActivityDatelist(
            user_id=2,
            browser_type="Browser1",
            valid_date=datetime.strptime("2023-01-02", "%Y-%m-%d").date(),  # DateType
            device_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d").date()]  # Array of DateType
        )
    ]
    
    # Create DataFrame from expected values
    expected_df = spark.createDataFrame(expected_values)
    expected_df = expected_df.withColumn("user_id", expected_df.user_id.cast(DecimalType(10,0)))
    
    # Compare the resulting DataFrame with the expected DataFrame
    assert_df_equality(result_df, expected_df)
