SELECT * FROM player_seasons;
CREATE TYPE season_stats AS (
    season INTEGER,
    gp INTEGER,
    pts REAL,
    reb REAL,
    ast REAL
);

CREATE TYPE scoring_class AS 
    ENUM ('bad', 'average', 'good', 'star');

CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    seasons season_stats[],
    scorer_class scoring_class,
    is_active BOOLEAN,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);


WITH last_season AS (
    SELECT * FROM players
    WHERE current_season = 1995

), this_season AS (
     SELECT * FROM player_seasons
    WHERE season = 1996
)
INSERT INTO players
SELECT
    COALESCE(ls.player_name, ts.player_name) as player_name,
    COALESCE(ls.height, ts.height) as height,
    COALESCE(ls.college, ts.college) as college,
    COALESCE(ls.country, ts.country) as country,
    COALESCE(ls.draft_year, ts.draft_year) as draft_year,
    COALESCE(ls.draft_round, ts.draft_round) as draft_round,
    COALESCE(ls.draft_number, ts.draft_number)
        as draft_number,
    CASE 
        WHEN ls.seasons IS NULL THEN
            ARRAY[ROW(ts.season, ts.gp, ts.pts, ts.reb, ts.ast)::season_stats]
        WHEN ts.season IS NULL THEN
            ls.seasons
        ELSE
            ls.seasons || ARRAY[ROW(ts.season, ts.gp, ts.pts, ts.reb, ts.ast)::season_stats]
    END as seasons,
    CASE
        WHEN ts.season IS NOT NULL THEN
            (CASE WHEN ts.pts > 20 THEN 'star'
            WHEN ts.pts > 15 THEN 'good'
            WHEN ts.pts > 10 THEN 'average'
            ELSE 'bad' END)::scoring_class
        ELSE ls.scorer_class
    END as scoring_class,
    ts.season IS NOT NULL as is_active,
    COALESCE(ts.season, ls.current_season + 1) AS current_season

FROM last_season ls
    FULL OUTER JOIN this_season ts
        ON ls.player_name = ts.player_name;



WITH unnested AS (
    SELECT
        player_name,
        UNNEST(seasons)::season_stats AS season_stats
    FROM players
    WHERE
        player_name = 'Michael Jordan' AND 
        current_season = 2001
)
SELECT
    player_name,
    (season_stats::season_stats).*
FROM unnested;


SELECT
    player_name,
    (seasons[1]::season_stats).* AS first_season,
    (seasons[cardinality(seasons)]::season_stats).* AS last_season
FROM players
WHERE
    player_name = 'Michael Jordan' AND 
    current_season = 2001

CREATE TABLE players_scd
(
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	current_season INTEGER,
	start_season INTEGER,
	end_season INTEGER,
    PRIMARY KEY (player_name, start_season)
);

INSERT INTO players_scd
WITH previous_season AS (
    SELECT 
        player_name,
        scorer_class,
        current_season,
        is_active,
        LAG(scorer_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scorer_class,
        LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
    FROM players
),
identifier AS (
    SELECT
        player_name,
        scorer_class,
        current_season,
        is_active,
        previous_scorer_class,
        previous_is_active,
        CASE 
            WHEN scorer_class != previous_scorer_class THEN 1
            WHEN is_active != previous_is_active THEN 1
            ELSE 0
        END AS identifier
    FROM previous_season
),
streak_identifier AS (
    SELECT
        player_name,
        scorer_class,
        is_active,
        current_season,
        SUM(identifier) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM identifier
),
aggregated AS (
    SELECT
        player_name,
        scorer_class,
        is_active,
        2001 AS current_season,
        MIN(current_season) AS start_date,
        MAX(current_season) AS end_season
    FROM streak_identifier
    GROUP BY player_name, scorer_class, is_active, streak_identifier
)
SELECT * FROM aggregated
ORDER BY player_name



CREATE TYPE scd_type AS (
    scoring_class scoring_class,
    is_active boolean,
    start_season INTEGER,
    end_season INTEGER
);


WITH last_season_scd AS (
    SELECT *
    FROM players_scd
    WHERE current_season = 2001
        AND end_season = 2001
),
historical_scd AS (
    SELECT
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season
    FROM players_scd
    WHERE current_season = 2001
    AND end_season < 2001
),
this_season_data AS (
    SELECT * FROM players
    WHERE current_season = 2002
),
unchanged_records AS (
    SELECT
        ts.player_name,
        ts.scorer_class,
        ts.is_active,
        ls.start_season,
        ts.current_season as end_season
    FROM this_season_data ts
        JOIN last_season_scd ls
            ON ls.player_name = ts.player_name
    WHERE ts.scorer_class = ls.scoring_class
        AND ts.is_active = ls.is_active
),
changed_records AS (
    SELECT
        ts.player_name,
        UNNEST(ARRAY[
            ROW(
                ls.scoring_class,
                ls.is_active,
                ls.start_season,
                ls.end_season

                )::scd_type,
            ROW(
                ts.scorer_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
                )::scd_type
        ]) as records
    FROM this_season_data ts
        LEFT JOIN last_season_scd ls
            ON ls.player_name = ts.player_name
    WHERE (ts.scorer_class <> ls.scoring_class
        OR ts.is_active <> ls.is_active)
),
unnested_changed_records AS (
    SELECT player_name,
        (records::scd_type).scoring_class,
        (records::scd_type).is_active,
        (records::scd_type).start_season,
        (records::scd_type).end_season
    FROM changed_records
),
new_records AS (
    SELECT
    ts.player_name,
        ts.scorer_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
        ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)
SELECT *, 2002 AS current_season FROM (
    SELECT *
    FROM historical_scd

    UNION ALL

    SELECT *
    FROM unchanged_records

    UNION ALL

    SELECT *
    FROM unnested_changed_records

    UNION ALL

    SELECT *
    FROM new_records
) AS res




CREATE TYPE vertex_type
    AS ENUM('player', 'team', 'game');


CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);

CREATE TYPE edge_type AS
    ENUM ('plays_against',
          'shares_team',
          'plays_in',
          'plays_on'
        );

CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (subject_identifier,
                subject_type,
                object_identifier,
                object_type,
                edge_type)
);

INSERT INTO vertices
SELECT
    game_id AS identifier,
    'game'::vertex_type AS type,
    json_build_object
    (
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team_',
            CASE WHEN home_team_wins = 1 THEN home_team_id
                ELSE visitor_team_id
            END
    ) AS  properties
FROM games;


WITH players_agg AS(
    SELECT
        player_id AS identifier,
        MAX(player_name) AS player_name,
        COUNT(1) AS number_of_games,
        SUM(pts) AS total_points,
        ARRAY_AGG(DISTINCT team_abbreviation) AS teams
    FROM
        game_details
    GROUP BY
        player_id
)
INSERT INTO vertices
SELECT
    identifier,
    'player'::vertex_type AS type,
    json_build_object
    (
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
    ) AS properties
FROM players_agg;


WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id) as row_num
    FROM teams
)
INSERT INTO vertices
SELECT
       team_id AS identifier,
    'team'::vertex_type AS type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
        )
FROM teams_deduped
WHERE row_num = 1;


INSERT INTO edges
SELECT
    player_id AS subject_identifier,
    'player'::vertex_type as subject_type,
    game_id AS object_identifier,
    'game'::vertex_type as object_type,
    'plays_in'::edge_type as edge_type,
    json_build_object(
        'start_posistion', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) as properties
FROM
    game_details;


INSERT INTO edges
WITH aggregated_players AS
(
    SELECT
        f1.player_id AS left_player_id,
        f2.player_id AS right_player_id,
        CASE WHEN f1.team_abbreviation = f2.team_abbreviation
            THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END as edge_type,
        MAX(f1.player_name) AS left_player_name,
        MAX(f2.player_name) AS right_player_name,
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
        f2.player_id,
        CASE WHEN f1.team_abbreviation = f2.team_abbreviation
            THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END
)
SELECT
    left_player_id AS subject_identifier,
    'player'::vertex_type as subject_type,
    right_player_id AS object_identifier,
    'player'::vertex_type as object_type,
    edge_type as edge_type,
    json_build_object(
        'num_games', num_games,
        'subject_points', left_points,
        'object_points', right_points
    ) as properties
FROM aggregated_players


SELECT
    v1.properties->>'player_name' AS player_1_name,
    v2.properties->>'player_name' AS player_2_name,
    (
        CAST(v1.properties->>'total_points' AS REAL)/
        CASE WHEN v1.properties->>'number_of_games' = '0' 
            THEN 1 
            ELSE CAST(v1.properties->>'number_of_games' AS REAL) 
        END
    ) AS player_1_normal_ppg,
    (
        CAST(v2.properties->>'total_points' AS REAL)/
        CASE WHEN v2.properties->>'number_of_games' = '0' 
            THEN 1 
            ELSE CAST(v1.properties->>'number_of_games' AS REAL) 
        END
    ) AS player_2_normal_ppg,
    (
        CAST(e.properties->>'subject_points' AS REAL)/
        CASE WHEN e.properties->>'num_games' = '0'
            THEN 1
            ELSE CAST(e.properties->>'num_games' AS REAL) 
        END
    ) AS player_1_ppg_against_player_2,
    (
        CAST(e.properties->>'object_points' AS REAL)/
        CASE WHEN e.properties->>'num_games' = '0'
            THEN 1
            ELSE CAST(e.properties->>'num_games' AS REAL) 
        END
    ) AS player_2_ppg_against_player_1
FROM vertices v1 
    JOIN edges e
        ON v1.identifier = e.subject_identifier
            AND v1.type = e.subject_type
            AND e.edge_type = 'plays_against'
    JOIN vertices v2
        ON e.object_identifier = v2.identifier
            AND e.object_type = v2.type
WHERE
    v1.type = 'player'
    AND v1.identifier = '1629029'
LIMIT 10;