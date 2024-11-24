CREATE TYPE film AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);


CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');


CREATE TABLE IF NOT EXISTS actors (
    actor TEXT,
    actorid TEXT,
    current_year INTEGER,
    films film[],
    quality quality_class,
    is_active BOOLEAN,
    PRIMARY KEY (actorid, current_year)
);



CREATE OR REPLACE FUNCTION insert_actor_data(last_year_param INT, this_year_param INT)
RETURNS VOID AS $$
BEGIN
    WITH last_year AS (
        SELECT *
        FROM actors
        WHERE current_year = last_year_param
    ),
    this_year AS (
        SELECT 
            actorid,
            actor,
            year,
            array_agg(ROW(film, votes, rating, filmid)::film) AS film
         FROM actor_films
         WHERE year = this_year_param
         GROUP BY actorid, actor, year
    ),
    aggregated_rating AS (
        SELECT
            actorid,
            AVG(rating) AS avg_rating
        FROM actor_films
        WHERE year = this_year_param
        GROUP BY actorid
    )
    -- Perform the insert based on the provided parameters
    INSERT INTO actors (actorid, actor, current_year, films, quality, is_active)
    SELECT
        COALESCE(last_year.actorid, this_year.actorid) as actorid,
        COALESCE(last_year.actor, this_year.actor) as actor,
        this_year_param as current_year,
        COALESCE(last_year.films, ARRAY[]::film[]) || COALESCE(this_year.film, ARRAY[]::film[]),
        (CASE
            WHEN aggregated_rating.avg_rating IS NULL THEN last_year.quality
            WHEN aggregated_rating.avg_rating > 8 THEN 'star'
            WHEN aggregated_rating.avg_rating > 7 THEN 'good'
            WHEN aggregated_rating.avg_rating > 6 THEN 'average'
            ELSE 'bad'
        END)::quality_class,
        this_year.year IS NOT NULL AS is_active
    FROM last_year
    FULL OUTER JOIN this_year
        ON last_year.actorid = this_year.actorid
    LEFT JOIN aggregated_rating
        ON this_year.actorid = aggregated_rating.actorid;
END;
$$ LANGUAGE plpgsql;


-- LOOP THROUGH 1970 - 2021
DO $$
DECLARE
    last_year INT := 1969;
    this_year INT := 1970;
BEGIN
    LOOP
        PERFORM insert_actor_data(last_year, this_year);
        last_year := this_year;
        this_year := this_year + 1;
        EXIT WHEN this_year > 2021;
    END LOOP;
END;
$$


CREATE TABLE actors_history_scd (
    actorid TEXT,
    actor TEXT,
    quality quality_class,
    is_active BOOLEAN,
    start_date INT,
    end_date INT,
    PRIMARY KEY (actorid, start_date)
);


TRUNCATE TABLE actors_history_scd;
INSERT INTO actors_history_scd
WITH previous_year AS (
    SELECT
        actorid,
        actor,
        quality,
        is_active,
        current_year,
        LAG(quality, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality,
        LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
    FROM actors
), change_identifier AS (
    SELECT
        actorid,
        actor,
        quality,
        is_active,
        current_year,
        CASE
            WHEN previous_quality <> quality THEN 1
            WHEN previous_is_active <> is_active THEN 1
            ELSE 0
        END AS change_identifier
    FROM previous_year
), streak_identifier AS (
    SELECT
        actorid,
        actor,
        quality,
        is_active,
        current_year,
        SUM(change_identifier) OVER (PARTITION BY actorid ORDER BY current_year) AS identifier
    FROM change_identifier
), aggregated AS (
    SELECT
        actorid,
        actor,
        quality,
        is_active,
        MIN(current_year) AS start_date,
        MAX(current_year) AS end_date
    FROM streak_identifier
    GROUP BY actorid, actor, quality, is_active, identifier
) SELECT * FROM aggregated
ORDER BY actorid, start_date;


CREATE TYPE actor_scd_type AS (
    quality quality_class,
    is_active BOOLEAN,
    start_date INT,
    end_date INT
);


TRUNCATE TABLE actors_history_scd;
CREATE OR REPLACE FUNCTION insert_actor_scd(last_year_param INT, this_year_param INT)
RETURNS VOID AS $$
BEGIN
    WITH historical_scd AS (
        SELECT * 
        FROM actors_history_scd
        WHERE end_date < last_year_param
    ),
    last_year AS (
        SELECT *
        FROM actors_history_scd
        WHERE end_date = last_year_param
    ),
    this_year AS (
        SELECT * FROM actors
        WHERE current_year = this_year_param
    ),
    unchanged_records AS (
        SELECT
            ly.actorid,
            ly.actor,
            ly.quality,
            ly.is_active,
            ly.start_date,
            this_year_param AS end_date
        FROM last_year ly
            JOIN this_year ty
                ON ly.actorid = ty.actorid
        WHERE ly.quality = ty.quality
            AND ly.is_active = ty.is_active
    ),
    changed_records AS (
        SELECT
            ty.actorid,
            ty.actor,
            UNNEST(
                ARRAY[
                    ROW(
                        ly.quality,
                        ly.is_active,
                        ly.start_date,
                        ly.end_date
                    )::actor_scd_type,
                    ROW(
                        ty.quality,
                        ty.is_active,
                        ty.current_year,
                        ty.current_year
                    )::actor_scd_type
                ]
            ) as records
        FROM this_year ty
            LEFT JOIN last_year ly
                ON ly.actorid = ty.actorid
        WHERE (ty.quality <> ly.quality
            OR ty.is_active <> ly.is_active)  
    ),
    unnested_changed_records AS (
        SELECT
            ty.actorid,
            ty.actor,
            (records::actor_scd_type).*
        FROM changed_records ty
    ),
    new_records AS (
        SELECT
            ty.actorid,
            ty.actor,
            ty.quality,
            ty.is_active,
            ty.current_year AS start_date,
            ty.current_year AS end_date
        FROM this_year ty
            LEFT JOIN last_year ly
                ON ly.actorid = ty.actorid
        WHERE ly.actorid IS NULL
    )
    INSERT INTO actors_history_scd (actorid, actor, quality, is_active, start_date, end_date)
    SELECT * FROM (
        SELECT * FROM historical_scd
        UNION ALL
        SELECT * FROM unchanged_records
        UNION ALL
        SELECT * FROM unnested_changed_records
        UNION ALL
        SELECT * FROM new_records
    ) AS res
    ON CONFLICT (actorid, start_date) DO UPDATE SET
        end_date = EXCLUDED.end_date;
END;
$$ LANGUAGE plpgsql;

-- LOOP THROUGH 1970 - 2021
DO $$
DECLARE
    last_year INT := 1969;
    this_year INT := 1970;
BEGIN
    LOOP
        PERFORM insert_actor_scd(last_year, this_year);
        last_year := this_year;
        this_year := this_year + 1;
        EXIT WHEN this_year > 2021;
    END LOOP;
END;
$$
