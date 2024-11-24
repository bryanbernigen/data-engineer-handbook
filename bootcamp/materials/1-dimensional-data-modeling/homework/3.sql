CREATE TABLE actors_history_scd (
    actorid TEXT,
    actor TEXT,
    quality quality_class,
    is_active BOOLEAN,
    start_date INT,
    end_date INT,
    PRIMARY KEY (actorid, start_date)
);
