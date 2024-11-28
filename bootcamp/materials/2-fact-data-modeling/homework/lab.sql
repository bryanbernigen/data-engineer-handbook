CREATE TABLE IF NOT EXISTS fct_game_details (
    dim_game_date DATE,
    dim_season INTEGER,
    dim_team_id INTEGER,
    dim_player_id INTEGER,
    dim_player_name TEXT,
    dim_start_position TEXT,
    dim_is_playing_at_home BOOLEAN,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOLEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_fgm INTEGER,
    m_fga INTEGER,
    m_fg3m INTEGER,
    m_fg3a INTEGER,
    m_ftm INTEGER,
    m_fta INTEGER,
    m_oreb INTEGER,
    m_dreb INTEGER,
    m_reb INTEGER,
    m_ast INTEGER,
    m_stl INTEGER,
    m_blk INTEGER,
    m_turnovers INTEGER,
    m_pf INTEGER,
    m_pts INTEGER,
    m_plus_minus INTEGER,

    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
)

WITH deduped AS (
    SELECT 
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER (
            PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details AS gd
        JOIN games AS g
            ON gd.game_id = g.game_id
)
INSERT INTO fct_game_details
SELECT 
    d.game_date_est AS dim_game_date,
    d.season AS dim_season,
    d.team_id AS dim_team_id,
    d.player_id AS dim_player_id,
    d.player_name AS dim_player_name,
    d.start_position AS dim_start_position,
    d.team_id = d.home_team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_with_team,
    CAST(SPLIT_PART(d.min,':',1) AS REAL) +
    (CAST(SPLIT_PART(d.min,':',2) AS REAL) / 60)  AS m_minutes,
    d.fgm AS m_fgm,
    d.fga AS m_fga,
    d.fg3m AS m_fg3m,
    d.fg3a AS m_fg3a,
    d.ftm AS m_ftm,
    d.fta AS m_fta,
    d.oreb AS m_oreb,
    d.dreb AS m_dreb,
    d.reb AS m_reb,
    d.ast AS m_ast,
    d.stl AS m_stl,
    d.blk AS m_blk,
    d."TO" AS m_turnovers,
    d.pf AS m_pf,
    d.pts AS m_pts,
    d.plus_minus AS m_plus_minus
FROM deduped AS d
WHERE row_num = 1;

SELECT 
    dim_player_name,
    COUNT(*) AS num_games,
    COUNT(CASE WHEN dim_did_not_play THEN 1 ELSE NULL END) AS dim_did_not_play,
    CAST(COUNT(CASE WHEN dim_did_not_play THEN 1 ELSE NULL END) AS REAL) / COUNT(*) AS pct_dnp
FROM 
    fct_game_details
GROUP BY 1
ORDER BY 4 DESC;


CREATE TABLE users_cumulated (
    user_id NUMERIC,
    date DATE,
    dates_active DATE[],
    PRIMARY KEY (user_id, date)
);  


CREATE OR REPLACE FUNCTION insert_user_cumulated(yesterday_date DATE, today_date DATE)
RETURNS VOID AS $$
BEGIN
    WITH yesterday AS (
        SELECT
            *
        FROM users_cumulated
        WHERE date = yesterday_date
    ),
    today AS (
        SELECT
            user_id,
            event_time::DATE AS date,
            ARRAY[event_time::DATE] AS dates_active
        FROM events
        WHERE 
            event_time::DATE = today_date
            AND user_id IS NOT NULL
        GROUP BY user_id, event_time::DATE
    )
    INSERT INTO users_cumulated
    SELECT
        COALESCE(y.user_id, t.user_id) AS user_id,
        today_date AS date,
        COALESCE(y.dates_active,ARRAY[]::DATE[]) 
            || COALESCE(t.dates_active,ARRAY[]::DATE[]) AS dates_active
    FROM yesterday AS y
        FULL OUTER JOIN today AS t
            ON y.user_id = t.user_id;
END;
$$ LANGUAGE plpgsql;


DO $$
DECLARE
    yesterday DATE := '2023-01-01';  
    today DATE := '2023-01-01';
BEGIN
    LOOP
        PERFORM insert_user_cumulated(yesterday, today);
        yesterday := today;
        today := today + INTERVAL '1 day';
        EXIT WHEN today > '2023-01-31';
    END LOOP;
END;
$$


WITH users AS (
    SELECT *
    FROM users_cumulated
    WHERE date = '2023-01-31'
),
series AS (
    SELECT *
    FROM generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS series_date
),
placeholder_int AS(
    SELECT
        CASE
            WHEN dates_active @> ARRAY[series_date]::DATE[]
                THEN POW(2, 32 - (date - series_date::DATE))
            ELSE 0
        END AS placeholder_int_values,
        *   
    FROM users
        CROSS JOIN series
),
casted_bit AS (
    SELECT
        user_id,
        CAST(CAST(SUM(placeholder_int_values) AS BIGINT) AS BIT(32)) as casted_bit_value
    FROM placeholder_int
    GROUP BY user_id
)
SELECT
    user_id,
    casted_bit_value,
    BIT_COUNT(casted_bit_value) > 0 AS dim_is_monthly_active,
    CAST(casted_bit_value & CAST('11111110000000000000000000000000' AS BIT(32)) AS BIGINT) > 0 AS dim_is_weekly_active,
    CAST(casted_bit_value & CAST('10000000000000000000000000000000' AS BIT(32)) AS BIGINT) > 0 AS dim_is_daily_active
FROM
    casted_bit;



CREATE TABLE array_metrics(
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT, 
    metric_array REAL[],

    PRIMARY KEY (user_id, month_start, metric_name)
);


CREATE OR REPLACE FUNCTION insert_array_metrics(start_date DATE, today_date DATE)
RETURNS VOID AS $$
BEGIN
    WITH daily_aggregate AS (
        SELECT
            user_id,
            event_time::DATE AS date,
            COUNT(1) AS num_site_hits
        FROM events
        WHERE 
            event_time::DATE = today_date
            AND user_id IS NOT NULL
        GROUP BY user_id, event_time::DATE
    ),
    yesterday AS (
        SELECT
            *
        FROM array_metrics
        WHERE 
            month_start = start_date
    )
    INSERT INTO array_metrics (user_id, month_start, metric_name, metric_array)
    SELECT 
        COALESCE(daily_aggregate.user_id, yesterday.user_id) AS user_id,
        COALESCE(yesterday.month_start, DATE_TRUNC('month', daily_aggregate.date)) AS month_start,
        'site_hits' AS metric_name,
        CASE 
            WHEN yesterday.metric_array IS NOT NULL 
                THEN yesterday.metric_array || ARRAY[COALESCE(daily_aggregate.num_site_hits, 0)]
            ELSE 
                ARRAY_FILL(0, ARRAY[(today_date - start_date)::INT]) 
                || ARRAY[COALESCE(daily_aggregate.num_site_hits, 0)]
        END AS metric_array
    FROM daily_aggregate
        FULL OUTER JOIN yesterday
            ON daily_aggregate.user_id = yesterday.user_id
    ON CONFLICT (user_id, month_start, metric_name) 
    DO 
        UPDATE SET metric_array = EXCLUDED.metric_array;
END;
$$ LANGUAGE plpgsql;


DO $$
DECLARE
    start_date DATE := '2023-01-01';  
    today_date DATE := '2023-01-01';
BEGIN
    LOOP
        PERFORM insert_array_metrics(start_date, today_date);
        today_date := today_date + INTERVAL '1 day';
        EXIT WHEN today_date > '2023-01-31';
    END LOOP;
END;
$$


WITH nested AS
(
    SELECT
        metric_name, 
        month_start,
        (ARRAY[
            SUM(metric_array[1]),
            SUM(metric_array[2]),
            SUM(metric_array[3]),
            SUM(metric_array[4]),
            SUM(metric_array[5]),
            SUM(metric_array[6]),
            SUM(metric_array[7]),
            SUM(metric_array[8]),
            SUM(metric_array[9]),
            SUM(metric_array[10]),
            SUM(metric_array[11]),
            SUM(metric_array[12]),
            SUM(metric_array[13]),
            SUM(metric_array[14]),
            SUM(metric_array[15]),
            SUM(metric_array[16]),
            SUM(metric_array[17]),
            SUM(metric_array[18]),
            SUM(metric_array[19]),
            SUM(metric_array[20]),
            SUM(metric_array[21]),
            SUM(metric_array[22]),
            SUM(metric_array[23]),
            SUM(metric_array[24]),
            SUM(metric_array[25]),
            SUM(metric_array[26]),
            SUM(metric_array[27]),
            SUM(metric_array[28]),
            SUM(metric_array[29]),
            SUM(metric_array[30]),
            SUM(metric_array[31])
        ]) AS metric_array
    FROM array_metrics
    GROUP BY metric_name, month_start
),
unnested AS (
    SELECT
        *
    FROM nested
        CROSS JOIN UNNEST(nested.metric_array)
            WITH ORDINALITY AS a(value, index)
)
SELECT
    metric_name,
    month_start + interval '1 day' * (index - 1) AS date,
    value
FROM unnested;