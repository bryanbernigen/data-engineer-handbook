CREATE TABLE IF NOT EXISTS host_activity_reduced(
    host TEXT,
    month DATE,
    hit_array INT[],
    unique_visitors INT[],

    PRIMARY KEY (host, month)
)