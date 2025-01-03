CREATE OR REPLACE function insert_player_growth_accounting(last_year_input INT, this_year_input INT)
RETURNS VOID AS $$
BEGIN
    WITH last_year AS (
        SELECT *
        FROM players_growth_accounting
        WHERE year = last_year_input
    ),
    this_year AS (
        SELECT
            player_name,
            ARRAY[season] as year_active
        FROM player_seasons
        WHERE season = this_year_input
    )
    INSERT INTO players_growth_accounting
    SELECT
        COALESCE(ly.player_name, ty.player_name) as player_name,
        this_year_input as year,
        ly.year_active || COALESCE(ty.year_active, ARRAY[]::INT[]) as year_active,
        CASE
            WHEN ly.yearly_active IS NULL THEN 'New'::player_status
            WHEN 
                (ly.yearly_active = 'Retired' OR ly.yearly_active = 'Stayed Retired') 
                AND ty.player_name IS NULL THEN 'Stayed Retired'::player_status
            WHEN 
                (ly.yearly_active = 'New' OR ly.yearly_active = 'Continue Playing' OR ly.yearly_active = 'Returned from Retirement')
                AND ty.player_name IS NULL THEN 'Retired'::player_status
            WHEN 
                (ly.yearly_active = 'Retired' OR ly.yearly_active = 'Stayed Retired')
                AND ty.player_name IS NOT NULL THEN 'Returned from Retirement'::player_status
            ELSE 'Continue Playing'::player_status
        END as yearly_active
    FROM last_year AS ly
        FULL OUTER JOIN this_year AS ty
            ON ly.player_name = ty.player_name;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    start_year INT := 1995;
    end_year INT := 2022;
BEGIN
    FOR year IN start_year..end_year-1 LOOP
        PERFORM insert_player_growth_accounting(year, year + 1);
    END LOOP;
END;
$$