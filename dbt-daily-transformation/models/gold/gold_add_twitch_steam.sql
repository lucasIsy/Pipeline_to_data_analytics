SELECT 
t.Month,
t.twitch_game_id,
s.steam_game_id,
t.game_name,
t.total_views,
s.peak_players
FROM {{ref('gold_twitch_monthly')}} as t
INNER JOIN {{ref('gold_steam_monthly')}} as s
ON t.Month = s.Month AND t.game_name = s.game_name
ORDER BY Month ASC