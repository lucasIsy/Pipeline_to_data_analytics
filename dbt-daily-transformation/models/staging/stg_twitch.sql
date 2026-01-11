with
    source as (

        select * from {{ source("bronze_data", "bronze_twitch") }}
    ),

    flattened as (
        select
            ingestion_timestamp_utc,
            -- A função explode separa o array em linhas
            explode(twitch_data.data) as stream_info
        from source
    )

select
    -- IDs e Chaves
    stream_info.id as stream_id,
    stream_info.user_id as streamer_id,
    stream_info.game_id as twitch_game_id,

    -- Detalhes
    stream_info.game_name as game_name,
    stream_info.language as stream_language,

    -- Métricas (Cast para garantir o tipo)
    cast(stream_info.viewer_count as int) as viewer_count,

    -- Datas
    to_timestamp(stream_info.started_at) as started_at_utc,
    to_timestamp(ingestion_timestamp_utc) as ingested_at_utc

from flattened