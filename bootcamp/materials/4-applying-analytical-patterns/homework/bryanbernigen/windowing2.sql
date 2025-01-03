WITH game_data AS (
    SELECT
        game_date_est,
        CASE 
            WHEN pts > 10 THEN TRUE
            ELSE FALSE
        END AS current_more_than_10,
        CASE
            WHEN LAG(pts, 1) OVER (PARTITION BY player_name ORDER BY game_date_est) > 10 THEN TRUE
            ELSE FALSE
        END AS previous_more_than_10
    FROM game_details AS gd
        INNER JOIN games AS g
            ON gd.game_id = g.game_id
    WHERE player_name = 'LeBron James'
        AND pts IS NOT NULL 
), change_identifier AS (
    SELECT
        game_date_est,
        current_more_than_10,
        previous_more_than_10,
        CASE
            WHEN current_more_than_10 <> previous_more_than_10 THEN 1
            ELSE 0
        END AS change_identifier
    FROM game_data
),
streak_identifier AS (
    SELECT
        game_date_est,
        current_more_than_10,
        previous_more_than_10,
        SUM(change_identifier) OVER (ORDER BY game_date_est) AS identifier
    FROM change_identifier
)
SELECT
    MAX(current_more_than_10::INT) AS more_than_10_pts,
    MIN(game_date_est) AS start_date,
    MAX(game_date_est) AS end_date,
    COUNT(1) AS amount_of_games
FROM streak_identifier
GROUP BY identifier
