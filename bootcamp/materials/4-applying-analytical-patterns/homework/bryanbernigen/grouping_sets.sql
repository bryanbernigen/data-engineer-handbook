WITH filtered_data AS (
    SELECT
        season,
        player_name,
        team_abbreviation,
        COALESCE(pts, 0) AS pts,
        CASE
            WHEN gd.team_id = g.home_team_id AND g.home_team_wins = 1 THEN 1
            WHEN gd.team_id = g.visitor_team_id AND NOT g.home_team_wins = 1 THEN 1
            ELSE 0
        END AS win
    FROM games AS g
        INNER JOIN game_details AS gd
            ON g.game_id = gd.game_id
),
aggregated_data AS (
    SELECT
        season,
        player_name,
        team_abbreviation,
        SUM(pts) AS pts,
        SUM(win) AS wins
    FROM filtered_data
    GROUP BY GROUPING SETS (
        (player_name, team_abbreviation),
        (player_name, season),
        (team_abbreviation)
    )
)
SELECT * FROM aggregated_data
LIMIT 100;