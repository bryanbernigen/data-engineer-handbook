
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("local").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

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
        COUNT(DISTINCT(m.match_id)) AS count
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