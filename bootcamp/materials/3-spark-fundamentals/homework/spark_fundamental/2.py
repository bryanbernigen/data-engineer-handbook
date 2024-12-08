from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("local").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

joined_df = spark.sql("""
        SELECT 
        /*+ BROADCAST(med), BROADCAST(mp) */
                *
        FROM bootcamp.matches_bucketed m
                LEFT JOIN bootcamp.match_details_bucketed md
                        ON m.match_id = md.match_id
                LEFT JOIN bootcamp.medals_matches_players_bucketed mmp
                        ON (
                                m.match_id = mmp.match_id
                                AND md.player_gamertag = mmp.player_gamertag
                        )
                LEFT JOIN bootcamp.medals med
                        ON med.medal_id = mmp.medal_id
                LEFT JOIN bootcamp.maps mp
                        ON mp.mapid = m.mapid
""")

# Show the result
joined_df.show()
joined_df.saveAsTable("bootcamp.joined_df")