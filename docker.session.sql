
WITH a
SELECT
    f1.player_id,
    f1.player_name,
    f2.player_id,
    f2.player_name,
    CASE WHEN f1.team_abbreviation = f2.team_abbreviation
        THEN 'shares_team'::edge_type
        ELSE 'plays_against'::edge_type
    END as edge_type,
    COUNT (*) AS num_games,
    SUM(f1.pts) AS left_points,
    SUM(f2.pts) AS right_points
FROM game_details AS f1
    JOIN game_details AS f2
        ON f1.game_id = f2.game_id
        AND f1.player_name <> f2.player_name
WHERE
    f1.player_id > f2.player_id
GROUP BY
    f1.player_id,
    f1.player_name,
    f2.player_id,
    f2.player_name,
    CASE WHEN f1.team_abbreviation = f2.team_abbreviation
        THEN 'shares_team'::edge_type
        ELSE 'plays_against'::edge_type
    END
LIMIT 10;
    