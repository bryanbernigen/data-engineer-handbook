from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("local").getOrCreate()

    device_df = spark.read.option("header", "true").csv("/home/iceberg/data/devices.csv")
    device_df.write.mode("overwrite").insertInto("devices")

    event_df = spark.read.option("header", "true").csv("/home/iceberg/data/events.csv")
    event_df.write.mode("overwrite").insertInto("events")