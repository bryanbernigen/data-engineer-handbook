WITH game_result AS (
    SELECT
        game_date_est AS game_date,
        home_team_id AS team_id,
        CASE
            WHEN home_team_wins = 1 THEN 1
            ELSE 0
        END AS win
    FROM games

    UNION ALL

    SELECT 
        game_date_est AS game_date,
        visitor_team_id AS team_id,
        CASE
            WHEN home_team_wins = 1 THEN 0
            ELSE 1
        END AS win
    FROM games
),
game_result_window AS (
    SELECT
        MIN(game_date) OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS start_date,
        game_date AS end_date,
        team_id,
        SUM(win) OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS total_wins
    FROM game_result
), 
deduped_teams AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id) as row_num
    FROM teams
)
SELECT * 
    FROM game_result_window AS g
        INNER JOIN deduped_teams AS t
            ON g.team_id = t.team_id
    WHERE row_num = 1
ORDER BY total_wins DESC
LIMIT 100;