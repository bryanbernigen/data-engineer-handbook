with rowed_game_details AS (
    SELECT
        *,
        row_number() over (PARTITION BY game_id, player_id) AS row_num
    FROM
        game_details
),
deduped_game_details AS (
    SELECT
        *
    FROM
        rowed_game_details
    WHERE
        row_num = 1
)
SELECT * FROM deduped_game_details;



CREATE TABLE IF NOT EXISTS user_devices_cumulated (
    user_id NUMERIC,
    browser_type TEXT,
    valid_date DATE,
    device_activity_datelist DATE[],

    PRIMARY KEY (user_id, browser_type, valid_date)
);



CREATE OR REPLACE FUNCTION insert_user_device_cummulated(yesterday_date DATE, today_date DATE)
RETURNS VOID AS $$
BEGIN
    WITH yesterday AS (
        SELECT * 
        FROM user_devices_cumulated
        WHERE valid_date = yesterday_date
    ),
    today AS (
        SELECT
            user_id,
            browser_type,
            event_time::DATE AS valid_date,
            ARRAY[event_time::DATE] AS device_activity_datelist
        FROM events AS e
            INNER JOIN devices AS d
                ON e.device_id = d.device_id
        WHERE
            event_time::DATE = today_date
            AND user_id IS NOT NULL
        GROUP BY user_id, browser_type, event_time::DATE
    ), 
    cummulated AS (
        SELECT
            COALESCE(y.user_id, t.user_id) AS user_id,
            COALESCE(y.browser_type, t.browser_type) AS browser_type,
            today_date::DATE AS valid_date,
            COALESCE(y.device_activity_datelist,ARRAY[]::DATE[]) ||
                COALESCE(t.device_activity_datelist,ARRAY[]::DATE[]) AS device_activity_datelist
        FROM yesterday AS y
            FULL OUTER JOIN today AS t
                ON y.user_id = t.user_id
                    AND y.browser_type = t.browser_type
    )
    INSERT INTO user_devices_cumulated (user_id, browser_type, valid_date, device_activity_datelist)
    SELECT
        *
    FROM cummulated;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    yesterday DATE := '2023-01-01';  
    today DATE := '2023-01-01';
BEGIN
    LOOP
        PERFORM insert_user_device_cummulated(yesterday, today);
        yesterday := today;
        today := today + INTERVAL '1 day';
        EXIT WHEN today > '2023-01-31';
    END LOOP;
END;
$$


/* Type 1 (with Series) */
WITH user_devices AS(
    SELECT
        user_id,
        browser_type,
        valid_date,
        device_activity_datelist
    FROM user_devices_cumulated
    WHERE
        valid_date = '2023-01-31'
),
series AS (
    SELECT
        generate_series(
            '2023-01-01'::DATE, 
            '2023-01-31'::DATE, 
            '1 day'::INTERVAL
        ) AS series_date
),
placeholder_int AS(
    SELECT
        user_id,
        browser_type,
        CASE
            WHEN
                device_activity_datelist @> ARRAY[series_date]::DATE[]
                THEN POW(2, 32 - (valid_date - series_date::DATE))
            ELSE 0
        END AS placeholder_int_value
    FROM user_devices
        CROSS JOIN series
), device_activity_datelist AS (
    SELECT
        user_id,
        browser_type,
        CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS device_activity_datelist_bit
    FROM placeholder_int
    GROUP BY user_id, browser_type
)
SELECT * 
FROM device_activity_datelist



/* Type 2 (no Series) */
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
        CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS device_activity_datelist_bit
    FROM placeholder_int
    GROUP BY user_id, browser_type
)
SELECT * 
FROM device_activity_datelist 



CREATE TABLE IF NOT EXISTS hosts_cumulated (
    host TEXT,
    start_date DATE,
    host_activity_datelist DATE[],

    PRIMARY KEY (host, start_date)
);



CREATE OR REPLACE FUNCTION insert_host_cummulated(today DATE)
RETURNS VOID AS $$
BEGIN
    WITH previous AS (
        SELECT *
        FROM hosts_cumulated
    ),
    current AS (
        SELECT
            host,
            today AS start_date,
            ARRAY[today] AS host_activity_datelist
        FROM
            events
        WHERE
            event_time::DATE = today
            AND host IS NOT NULL
        GROUP BY host
    ),
    updated_hosts AS (
        SELECT
            p.host,
            p.start_date,
            p.host_activity_datelist || c.host_activity_datelist AS host_activity_datelist
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
        WHERE p.host IS NULL
    )
    INSERT INTO hosts_cumulated
    SELECT * FROM updated_hosts
    UNION ALL
    SELECT * FROM new_hosts
    ON CONFLICT (host, start_date) DO UPDATE SET
        host_activity_datelist = EXCLUDED.host_activity_datelist;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    today DATE := '2023-01-01';
BEGIN
    LOOP
        PERFORM insert_host_cummulated(today);
        today := today + INTERVAL '1 day';
        EXIT WHEN today > '2023-01-31';
    END LOOP;
END;
$$



CREATE TABLE IF NOT EXISTS host_activity_reduced(
    host TEXT,
    month DATE,
    hit_array INT[],
    unique_visitors INT[],

    PRIMARY KEY (host, month)
)



CREATE OR REPLACE FUNCTION insert_host_activity_reduced(start_date DATE, today DATE)
RETURNS VOID AS $$
BEGIN
    WITH previous AS (
        SELECT
            *
        FROM
            host_activity_reduced
        WHERE
            month = start_date
    ),
    current AS (
        SELECT
            host,
            start_date::DATE AS month,
            ARRAY[COUNT(*)] AS hit_array,
            ARRAY[COUNT(DISTINCT user_id)] AS unique_visitors
        FROM
            events
        WHERE
            event_time::DATE = today
            AND host IS NOT NULL
        GROUP BY host
    ),
    updated_hosts AS (
        SELECT
            p.host,
            p.month,
            p.hit_array || c.hit_array AS hit_array,
            p.unique_visitors || c.unique_visitors AS unique_visitors
        FROM previous p
        INNER JOIN current c
            ON p.host = c.host
    ),
    new_hosts AS (
        SELECT
            c.host,
            c.month,
            ARRAY_FILL(0, ARRAY[(today - start_date)::INT])  || c.hit_array AS hit_array,
            ARRAY_FILL(0, ARRAY[(today - start_date)::INT]) || c.unique_visitors AS unique_visitors
        FROM current c
            LEFT JOIN previous p
                ON c.host = p.host
                AND p.month = start_date
        WHERE
            p.host IS NULL
    )
    INSERT INTO host_activity_reduced
    SELECT * FROM updated_hosts
    UNION ALL
    SELECT * FROM new_hosts
    ON CONFLICT (host, month) DO UPDATE SET
        hit_array = EXCLUDED.hit_array,
        unique_visitors = EXCLUDED.unique_visitors;
END;
$$ LANGUAGE plpgsql;


DO $$
DECLARE
    start_date DATE := '2023-01-01';
    today DATE := '2023-01-01';
BEGIN
    LOOP
        PERFORM insert_host_activity_reduced(start_date, today);
        today := today + INTERVAL '1 day';
        EXIT WHEN today > '2023-01-31';
    END LOOP;
END;
$$