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
    -- Perform the insert based on the provided parameters
    INSERT INTO actors (actorid, actor, current_year, films, quality, is_active)
    SELECT
        COALESCE(ly.actorid, ty.actorid) AS actorid,
        COALESCE(ly.actor, ty.actor) AS actor,
        COALESCE(ty.current_year, ly.current_year + 1) AS current_year,
        COALESCE(ly.films, ARRAY[]::film[]) || COALESCE(ty.film, ARRAY[]::film[]) AS films,
        CASE
            WHEN ar.avg_rating IS NOT NULL THEN
                CASE
                    WHEN ar.avg_rating > 8 THEN 'star'
                    WHEN ar.avg_rating > 7 THEN 'good'
                    WHEN ar.avg_rating > 6 THEN 'average'
                    ELSE 'bad' 
                END::quality_class
            ELSE ly.quality
        END AS quality,
        CASE
            WHEN ty.film IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_active
    FROM
        -- Get last year's data
        (SELECT * FROM actors WHERE current_year = last_year_param) AS ly
    FULL OUTER JOIN 
        -- Get this year's data with films aggregated
        (SELECT 
            actorid,
            actor,
            this_year_param AS current_year,
            array_agg(ROW(film, votes, rating, filmid)::film) AS film
         FROM actor_films
         WHERE year = this_year_param
         GROUP BY actorid, actor) AS ty
    ON ly.actorid = ty.actorid
    LEFT JOIN 
        -- Get the average rating for this year
        (SELECT 
            actorid, 
            AVG(rating) AS avg_rating
         FROM actor_films
         WHERE year = this_year_param
         GROUP BY actorid) AS ar
    ON ly.actorid = ar.actorid;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE actors_history_scd (
    actorid TEXT,
    actor TEXT,
    quality quality_class,
    is_active BOOLEAN,
    start_date DATE,
    end_date DATE
    PRIMARY KEY (actorid, start_date)
)

