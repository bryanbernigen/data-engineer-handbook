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