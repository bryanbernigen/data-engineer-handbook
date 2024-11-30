with rowed_game_details AS (
    SELECT
        *,
        row_number() over (PARTITION BY game_id, player_id) AS row_num
    FROM
        game_details
),
deduped_game_details AS (
    SELECT
        *
    FROM
        rowed_game_details
    WHERE
        row_num = 1
)
SELECT * FROM deduped_game_details;
