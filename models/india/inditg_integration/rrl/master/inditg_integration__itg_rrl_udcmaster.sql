{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["udccode"]
    )
}}

with source as
(
    select * from {{ source('indsdl_raw', 'sdl_rrl_udcmaster') }}
),
final as
(
    select
    udccode::varchar(50) as udccode,
    udcname::varchar(200) as udcname,
    isactive::boolean as isactive,
    rowid::varchar(40) as rowid,
    filename::varchar(100) as filename,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz as updt_dttm
    from source
)
select * from final