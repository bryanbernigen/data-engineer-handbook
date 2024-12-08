from pyspark.sql import SparkSession

ddl_user_devices_cumulated ="""
    CREATE TABLE IF NOT EXISTS bootcamp.user_devices_cumulated (
        user_id INT,  -- Consider using INT if user_id is an integer
        browser_type STRING,  -- STRING is a more common type in Spark for text data
        valid_date DATE,
        device_activity_datelist ARRAY<DATE>
    )
    USING iceberg
    PARTITIONED BY (valid_date);

"""

ddl_hosts_cumulated_spark = """
    CREATE TABLE IF NOT EXISTS bootcamp.hosts_cumulated (
        host STRING,
        start_date DATE,
        host_activity_datelist ARRAY<DATE>
    )
    USING iceberg  
    PARTITIONED BY (start_date);
"""


if __name__ == "__main__":
    spark = SparkSession.builder.appName("DDL").getOrCreate()
    spark.sql(ddl_user_devices_cumulated)
    spark.sql(ddl_hosts_cumulated_spark)