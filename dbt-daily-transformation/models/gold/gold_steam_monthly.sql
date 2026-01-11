WITH steam_silver AS 
(SELECT * FROM {{ ref('silver_steam_agg') }})

SELECT 
    CAST(date_format(ingested_at_utc, 'yyyy-MM-dd') AS DATE) AS Month,
    steam_game_id,
    game_name,
    SUM(peak_players) as peak_players
FROM steam_silver
GROUP BY Month, steam_game_id, game_name