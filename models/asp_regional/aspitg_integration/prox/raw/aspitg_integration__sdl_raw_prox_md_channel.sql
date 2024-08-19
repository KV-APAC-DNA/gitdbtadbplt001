{{
    config(
        materialized="incremental",
        incremental_strategy="append"
        )
}}

with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_prox_md_channel') }}
),
final as
(
    select id::varchar(50) as id,
    channelcode::varchar(50) as channelcode,
    channelnamecn::varchar(50) as channelnamecn,
    channelnameen::varchar(100) as channelnameen,
    parentchannelcode::varchar(50) as parentchannelcode,
    levelid::number(38,0) as levelid,
    status::number(38,0) as status,
    orderno::number(38,0) as orderno,
    channelleader::varchar(50) as channelleader,
    siocode::varchar(50) as siocode,
    applicationid::varchar(50) as applicationid,
    filename::varchar(100) as filename,
    run_id::varchar(50) as run_id,
    crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final