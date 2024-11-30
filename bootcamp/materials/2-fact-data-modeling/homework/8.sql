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