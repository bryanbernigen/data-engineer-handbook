CREATE TABLE IF NOT EXISTS user_devices_cumulated (
    user_id NUMERIC,
    browser_type TEXT,
    valid_date DATE,
    device_activity_datelist DATE[],

    PRIMARY KEY (user_id, browser_type, valid_date)
);
