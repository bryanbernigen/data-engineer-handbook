from pyspark.sql import SparkSession

def do_host_cummulated_transform(spark, hosts_cumulated_df, events_df, today):
    query = f"""
        WITH previous AS (
            SELECT *
            FROM hosts_cumulated
        ),
        current AS (
            SELECT
                host,
                CAST('{today}' AS DATE) AS start_date,
                ARRAY(CAST('{today}' AS DATE)) AS host_activity_datelist
            FROM
                events
            WHERE
                CAST(event_time AS DATE) = CAST('{today}' AS DATE)
                AND host IS NOT NULL
            GROUP BY host
        ),
        updated_hosts AS (
            SELECT
                p.host,
                p.start_date,
                ARRAY_UNION(p.host_activity_datelist, c.host_activity_datelist) AS host_activity_datelist
            FROM previous p
                INNER JOIN current c
                    ON p.host = c.host
        ),
        new_hosts AS (
            SELECT
                c.host,
                c.start_date,
                c.host_activity_datelist
            FROM current c
                LEFT JOIN previous p
                    ON c.host = p.host
            WHERE
                p.host IS NULL
        )
        SELECT
            host,
            start_date,
            host_activity_datelist
        FROM updated_hosts
        UNION ALL
        SELECT
            host,
            start_date,
            host_activity_datelist
        FROM new_hosts
    """


    hosts_cumulated_df.createOrReplaceTempView("hosts_cumulated")
    events_df.createOrReplaceTempView("events")
    return spark.sql(query)

def main():
    spark = SparkSession.builder.appName("local").getOrCreate()

    output_df = do_host_cummulated_transform(spark, spark.table("bootcamp.hosts_cumulated"), spark.table("bootcamp.events"), "2023-01-01")
    output_df.write.mode("overwrite").insertInto("bootcamp.hosts_cumulated")

if __name__ == "__main__":
    main()