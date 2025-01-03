DO $$ 
BEGIN
    -- Check if the type already exists
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'player_status') THEN
        CREATE TYPE player_status AS ENUM (
            'New',
            'Retired',
            'Continue Playing',
            'Returned from Retirement',
            'Stayed Retired'
        );
    END IF;
END $$;


CREATE TABLE IF NOT EXISTS players_growth_accounting (
    player_name TEXT,
    year INT,
    year_active INT[],
    yearly_active player_status,
    PRIMARY KEY (player_name, year)
);