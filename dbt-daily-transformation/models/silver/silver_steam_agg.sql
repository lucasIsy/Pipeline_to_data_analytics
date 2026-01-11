{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['steam_game_id', 'ingested_at_utc'], 
    file_format='delta'
) }}

-- models/silver/silver_steam_agg.sql
WITH steam_deduplicated AS (
    SELECT * FROM {{ ref('silver_steam_dedupe') }}
),

final_grain AS (
    SELECT 
        -- IDs e Chaves
        steam_game_id, 
        game_name,
        
        -- Métricas
        peak_players,
        
        -- Datas
        ingested_at_utc
    FROM steam_deduplicated
)

SELECT * FROM final_grain