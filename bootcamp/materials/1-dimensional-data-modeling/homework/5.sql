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