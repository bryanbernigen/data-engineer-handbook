from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("local").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Read the CSV files into DataFrames with the header option properly set
matches_df = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
match_details_df = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
medals_matches_players_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")

medals_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
maps_df = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv").withColumnRenamed("name", "map_name").withColumnRenamed("description", "map_description")

# Bucket the DataFrames so it will automatically use Bucket join
matches_df.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bootcamp.matches_bucketed")
match_details_df.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")
medals_matches_players_df.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bootcamp.medals_matches_players_bucketed")

medals_df.write.mode("overwrite").saveAsTable("bootcamp.medals")
maps_df.write.mode("overwrite").saveAsTable("bootcamp.maps")


