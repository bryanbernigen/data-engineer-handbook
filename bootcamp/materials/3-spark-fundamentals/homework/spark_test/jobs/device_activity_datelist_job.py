from pyspark.sql import SparkSession

def do_device_activity_datelist_transform(spark, events_df, devices_df, user_devices_cumulated_df, yesterday_date, today_date):
    query = f"""
        WITH yesterday AS (
            SELECT *
            FROM user_devices_cumulated
            WHERE valid_date = CAST('{yesterday_date}' AS DATE)
        ),
        today AS (
            SELECT
                user_id,
                browser_type,
                CAST(event_time AS DATE) AS valid_date,
                ARRAY(CAST(event_time AS DATE)) AS device_activity_datelist
            FROM events AS e
                INNER JOIN devices AS d
                    ON e.device_id = d.device_id
            WHERE
                CAST(event_time AS DATE) = CAST('{today_date}' AS DATE)
                AND user_id IS NOT NULL
            GROUP BY user_id, browser_type, CAST(event_time AS DATE)
        ),
        cummulated AS (
            SELECT
                CAST(COALESCE(y.user_id, t.user_id) AS NUMERIC) AS user_id,
                COALESCE(y.browser_type, t.browser_type) AS browser_type,
                CAST('{today_date}' AS DATE) AS valid_date,
                CASE
                    WHEN y.device_activity_datelist IS NULL THEN t.device_activity_datelist
                    WHEN t.device_activity_datelist IS NULL THEN y.device_activity_datelist
                    ELSE ARRAY_UNION(y.device_activity_datelist, t.device_activity_datelist)
                END AS device_activity_datelist
            FROM yesterday AS y
                FULL OUTER JOIN today AS t
                    ON y.user_id = t.user_id
                    AND y.browser_type = t.browser_type
        )
        SELECT * FROM cummulated
    """

    events_df.createOrReplaceTempView("events")
    devices_df.createOrReplaceTempView("devices")
    user_devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")
    return spark.sql(query)


def main():
    today_date = "2023-01-01"
    yesterday_date = "2023-01-01"
    spark = SparkSession.builder.appName("local").getOrCreate()

    output_df = do_device_activity_datelist_transform(spark, spark.table("bootcamp.events"), spark.table("bootcamp.devices") ,spark.table("bootcamp.user_devices_cumulated") , yesterday_date, today_date)
    output_df.write.mode("overwrite").insertInto("bootcamp.user_devices_cumulated")

if __name__ == "__main__":
    main()
