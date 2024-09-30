{{
    config(
        materialized='incremental',
        incremental_strategy= "append",
        unique_key= ["udccode"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where (upper(distCode),upper(RsdCode),upper(OutletCode),
        upper(UserCode),upper(UdcCode)) in (select distinct upper(distCode),upper(RsdCode),
        upper(OutletCode),upper(UserCode),upper(UdcCode) from {{ source('indsdl_raw', 'sdl_rrl_udcmapping') }}
        where filename not in (select distinct file_name from {{ source('indwks_integration', 'TRATBL_sdl_rrl_udcmapping__null_test') }}))
         ;
        {% endif %}"
    )
}}

with source as
(
    select *, dense_rank() over(partition by upper(distCode),upper(RsdCode),
        upper(OutletCode),upper(UserCode),upper(UdcCode) order by filename desc) rnk from {{ source('indsdl_raw', 'sdl_rrl_udcmapping') }}
    where filename not in (select distinct file_name from {{ source('indwks_integration', 'TRATBL_sdl_rrl_udcmapping__null_test') }})
    qualify rnk = 1
),
final as
(
    select
    distcode::varchar(100) as distcode,
    rsdcode::varchar(50) as rsdcode,
    outletcode::varchar(50) as outletcode,
    usercode::varchar(100) as usercode,
    udccode::varchar(50) as udccode,
    createddate::timestamp_ntz(9) as createddate,
    isactive::boolean as isactive,
    isdelflag::varchar(1) as isdelflag,
    rowid::varchar(40) as rowid,
    filename::varchar(100) as filename,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz as updt_dttm
    from source
)
select * from final