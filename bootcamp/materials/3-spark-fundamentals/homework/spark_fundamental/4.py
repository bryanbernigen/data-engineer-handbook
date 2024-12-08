from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

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

joined_df = matches_df \
    .join(match_details_df, matches_df["match_id"] == match_details_df["match_id"], "left") \
    .join(medals_matches_players_df,
          (matches_df["match_id"] == medals_matches_players_df["match_id"]) &
          (match_details_df["player_gamertag"] ==
           medals_matches_players_df["player_gamertag"]),
          "left") \
    .join(broadcast(medals_df), medals_df["medal_id"] == medals_matches_players_df["medal_id"], "left") \
    .join(broadcast(maps_df), maps_df["mapid"] == matches_df["mapid"], "left")\
    .drop(match_details_df["match_id"], medals_matches_players_df["match_id"],
          medals_matches_players_df["medal_id"], medals_matches_players_df["player_gamertag"],
          matches_df["mapid"])

sort_type_1 = joined_df.sortWithinPartitions("mapid", "playlist_id")
sort_type_1.write.mode("overwrite").saveAsTable("bootcamp.sort_type_1")
sort_type_1_size = spark.table("demo.bootcamp.sort_type_1.files").agg(
    F.sum("file_size_in_bytes").alias("size"),
    F.count("*").alias("num_files")
).withColumn("source", F.lit("map, playlist"))

sort_type_2 = joined_df.sortWithinPartitions("playlist_id", "mapid")
sort_type_2.write.mode("overwrite").saveAsTable("bootcamp.sort_type_2")
sort_type_2_size = spark.table("demo.bootcamp.sort_type_2.files").agg(
    F.sum("file_size_in_bytes").alias("size"),
    F.count("*").alias("num_files")
).withColumn("source", F.lit("playlist, map"))

sort_type_3 = joined_df.sortWithinPartitions("player_gamertag", "mapid", "playlist_id")
sort_type_3.write.mode("overwrite").saveAsTable("bootcamp.sort_type_3")
sort_type_3_size = spark.table("demo.bootcamp.sort_type_3.files").agg(
    F.sum("file_size_in_bytes").alias("size"),
    F.count("*").alias("num_files")
).withColumn("source", F.lit("player, map, playlist"))

result = sort_type_1_size.unionAll(sort_type_2_size).unionAll(sort_type_3_size)
result.show()