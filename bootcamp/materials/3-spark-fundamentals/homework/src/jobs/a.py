from pyspark.sql import SparkSession

query =f"""
    SELECT * FROM events LIMIT 10
"""

def do_device_activity_datelist_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("user_devices_cumulated")
    return spark.sql(query)

def main():
    spark = SparkSession.builder.appName("device_activity_datelist").getOrCreate()
    output_df = do_device_activity_datelist_transformation(spark, spark.table("user_devices_cumulated"))
    output_df.write.mode("overwrite").insertInto("user_devices_cumulated")