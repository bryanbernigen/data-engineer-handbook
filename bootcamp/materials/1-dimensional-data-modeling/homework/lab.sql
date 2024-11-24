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