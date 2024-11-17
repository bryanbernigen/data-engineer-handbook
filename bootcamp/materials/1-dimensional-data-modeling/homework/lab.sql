-- SELECT * FROM player_seasons;
-- CREATE TYPE season_stats AS (
--     season INTEGER,
--     gp INTEGER,
--     pts REAL,
--     reb REAL,
--     ast REAL
-- );

-- CREATE TYPE scoring_class AS 
--     ENUM ('bad', 'average', 'good', 'star');

-- CREATE TABLE players (
--      player_name TEXT,
--      height TEXT,
--      college TEXT,
--      country TEXT,
--      draft_year TEXT,
--      draft_round TEXT,
--      draft_number TEXT,
--      seasons season_stats[],
--      scorer_class scoring_class,
--      is_active BOOLEAN,
--      current_season INTEGER,
--      PRIMARY KEY (player_name, current_season)
--  );


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