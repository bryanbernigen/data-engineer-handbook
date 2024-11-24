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