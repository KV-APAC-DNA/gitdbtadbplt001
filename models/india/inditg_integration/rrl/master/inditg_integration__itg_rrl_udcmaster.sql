{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["udccode"]
    )
}}

with source as
(
    select *,dense_rank()over(partition by udccode order by filename desc) rnk from {{ source('indsdl_raw', 'sdl_rrl_udcmaster') }}
     where filename not in (select distinct file_name from {{ source('indwks_integration', 'TRATBL_sdl_rrl_udcmaster__null_test') }})
     qualify rnk =1
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