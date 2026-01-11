WITH source_data AS (
    SELECT * FROM {{ ref('stg_twitch') }}
    {% if is_incremental() %}
      -- Processa apenas os dados novos para economizar custo no Databricks
      WHERE ingested_at_utc > (SELECT max(ingested_at_utc) FROM {{ this }})
    {% endif %}
),

-- Se utiliza ROW_NUMBER() pois o distinct seria aplicado em diversas colunas(muito processamento)
deduplicado AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY streamer_id, ingested_at_utc 
            ORDER BY ingested_at_utc
        ) as row_num
    FROM source_data
)

SELECT 
    * EXCEPT(row_num)
FROM deduplicado
WHERE row_num = 1