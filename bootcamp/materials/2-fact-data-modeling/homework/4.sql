WITH user_devices AS(
    SELECT
        user_id,
        browser_type,
        valid_date,
        UNNEST(device_activity_datelist) AS unnested_date
    FROM user_devices_cumulated
    WHERE
        valid_date = '2023-01-31'
),
placeholder_int AS(
    SELECT
        user_id,
        browser_type,
        CASE
            WHEN (valid_date - unnested_date::DATE) < 32
                THEN POW(2, 32 - (valid_date - unnested_date::DATE)-1)
            ELSE 0
        END AS placeholder_int_value
    FROM user_devices
), device_activity_datelist AS (
    SELECT
        user_id,
        browser_type,
        CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int
    FROM placeholder_int
    GROUP BY user_id, browser_type
)
SELECT * 
FROM device_activity_datelist 