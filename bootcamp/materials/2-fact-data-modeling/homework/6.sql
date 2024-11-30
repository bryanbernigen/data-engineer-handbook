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