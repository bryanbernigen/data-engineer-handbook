CREATE TABLE IF NOT EXISTS processed_events_sessioned (
    ip VARCHAR,                           -- IP address of the user
    geodata VARCHAR,                      -- Geolocation data
    host VARCHAR,                         -- Hostname or domain of the web server
    session_time TIMESTAMP(3),            -- Start timestamp of the session (with millisecond precision)
    num_hits BIGINT                       -- Number of events (hits) in the session
);
