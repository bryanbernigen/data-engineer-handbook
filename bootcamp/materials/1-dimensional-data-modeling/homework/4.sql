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
