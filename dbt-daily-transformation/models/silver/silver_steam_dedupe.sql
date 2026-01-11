{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['steam_game_id', 'ingested_at_utc'],
    file_format='delta'
) }}

WITH source_steam AS (
    SELECT * FROM {{ ref('stg_steam') }}
    {% if is_incremental() %}
      -- Filtro incremental para processar apenas os novos lotes
      WHERE ingested_at_utc > (SELECT max(ingested_at_utc) FROM {{ this }})
    {% endif %}
),

-- Distinct fazendo a deduplicação
deduplicated_data AS (
    SELECT DISTINCT
        steam_game_id,
        game_name,
        peak_players,
        ingested_at_utc
    FROM source_steam
)

-- 3. SELECT Final
SELECT * FROM deduplicated_data