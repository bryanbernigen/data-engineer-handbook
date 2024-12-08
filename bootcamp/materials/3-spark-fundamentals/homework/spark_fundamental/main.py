from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F

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

# Join all the DataFrames
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


# Which player averages the most kills per game?
res1 = spark.sql("""
    SELECT
        player_gamertag,
        AVG(player_total_kills) AS avg
    FROM bootcamp.match_details_bucketed
    GROUP BY player_gamertag
    ORDER BY avg DESC
""")
res1.show()


# Which playlist gets played the most?
res2 = spark.sql("""
    SELECT
        playlist_id,
        COUNT(playlist_id) AS count
    FROM bootcamp.matches_bucketed
    GROUP BY playlist_id
    ORDER BY count DESC
""")
res2.show()


# Which map gets played the most?
res3 = spark.sql("""
    SELECT 
        /*+ BROADCAST(mp) */
        mp.mapid,
        mp.map_name,
        mp.description,
        count(*) AS count
    FROM bootcamp.matches_bucketed AS m
        LEFT JOIN bootcamp.maps AS mp
            ON m.mapid = mp.mapid
    GROUP BY
        mp.mapid, mp.map_name, mp.map_description
    ORDER BY count DESC
""")
res3.show()


# Which map do players get the most Killing Spree medals on?
res4 = spark.sql("""
    SELECT
        /*+ BROADCAST(mp), BROADCAST(mdl) */
        mp.mapid,
        mp.map_name,
        COUNT(m.match_id) AS count
    FROM bootcamp.matches_bucketed m
        LEFT JOIN bootcamp.match_details_bucketed md
            ON m.match_id = md.match_id
        LEFT JOIN bootcamp.medals_matches_players_bucketed mmp 
            ON m.match_id = mmp.match_id AND md.player_gamertag = mmp.player_gamertag
        LEFT JOIN bootcamp.maps mp
            ON m.mapid = mp.mapid
        LEFT JOIN bootcamp.medals mdl
            ON mdl.medal_id = mmp.medal_id
    WHERE mdl.classification = 'KillingSpree'
    GROUP BY mp.mapid, mp.map_name
""")
res4.show()


# Which sort have the smallest file size?
sort_type_1 = joined_df.sortWithinPartitions("mapid", "playlist_id")
sort_type_1.write.mode("overwrite").saveAsTable("bootcamp.sort_type_1")
sort_type_1_size = spark.table("bootcamp.sort_type_1.files").agg(
    F.sum("file_size_in_bytes").alias("size"),
    F.count("*").alias("num_files")
).withColumn("source", F.lit("map, playlist"))

sort_type_2 = joined_df.sortWithinPartitions("playlist_id", "mapid")
sort_type_2.write.mode("overwrite").saveAsTable("bootcamp.sort_type_2")
sort_type_2_size = spark.table("bootcamp.sort_type_2.files").agg(
    F.sum("file_size_in_bytes").alias("size"),
    F.count("*").alias("num_files")
).withColumn("source", F.lit("playlist, map"))

sort_type_3 = joined_df.sortWithinPartitions("player_gamertag", "mapid", "playlist_id")
sort_type_3.write.mode("overwrite").saveAsTable("bootcamp.sort_type_3")
sort_type_3_size = spark.table("bootcamp.sort_type_3.files").agg(
    F.sum("file_size_in_bytes").alias("size"),
    F.count("*").alias("num_files")
).withColumn("source", F.lit("player, map, playlist"))

result = sort_type_1_size.unionAll(sort_type_2_size).unionAll(sort_type_3_size)
result.show()


