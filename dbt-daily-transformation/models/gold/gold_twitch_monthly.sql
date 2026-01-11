WITH silver_twitch_agg AS (
    SELECT * FROM {{ ref('silver_twitch_agg') }}
)

SELECT 
    CAST(date_format(ingested_at_utc, 'yyyy-MM-dd') AS DATE) AS Month,
    twitch_game_id,
    game_name,
    SUM(total_viewer_count) as total_views
FROM silver_twitch_agg
GROUP BY Month, twitch_game_id, game_name