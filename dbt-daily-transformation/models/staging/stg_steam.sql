with
    steam as (
        select * from {{ source("bronze_data", "bronze_steam") }}
    )

select 
    -- IDs e Chaves
    id as steam_game_id,
    jogo as game_name,

    -- Métricas
    qtd_jogadores as peak_players,

    -- Datas
    to_timestamp(ingestion_timestamp_utc) as ingested_at_utc

from steam