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
