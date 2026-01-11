{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['twitch_game_id', 'ingested_at_utc'], 
    file_format='delta'
) }}

-- 1. CTE de Importação: Organiza as fontes de dados
WITH twitch_deduplicated AS (
    SELECT * FROM {{ ref('silver_twitch_dedupe') }}
),

aggregated_metrics AS (
    SELECT
        -- IDs e Chaves
        twitch_game_id, 
        game_name, 

        -- Métricas
        SUM(viewer_count) as total_viewer_count,

        -- Datas
        ingested_at_utc
    FROM twitch_deduplicated
    GROUP BY 
        twitch_game_id, 
        game_name, 
        ingested_at_utc
)

-- 3. SELECT Final: O que a tabela realmente entrega
SELECT * FROM aggregated_metrics