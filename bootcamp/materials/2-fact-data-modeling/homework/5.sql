CREATE TABLE IF NOT EXISTS hosts_cumulated (
    host TEXT,
    start_date DATE,
    host_activity_datelist DATE[],

    PRIMARY KEY (host, start_date)
);