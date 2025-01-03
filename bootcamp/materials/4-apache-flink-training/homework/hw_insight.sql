-- What is the AVG number of web events of a session from a user on Tech Creator?
SELECT
    ip,
    host,
    AVG(num_hits) AS avg_hits
FROM
    processed_events_sessioned
GROUP BY
    ip, host;

-- What is the AVG number of web events of a session from a country on Tech Creator?
SELECT
    (geodata::jsonb)->>'country' AS country,
    AVG(num_hits) AS avg_hits
FROM
    processed_events_sessioned
WHERE
    (geodata::jsonb)->>'country' IS NOT NULL
GROUP BY
    country;


-- - Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
SELECT
    host,
    AVG(num_hits) AS avg_hits
FROM
    processed_events_sessioned
GROUP BY
    host;