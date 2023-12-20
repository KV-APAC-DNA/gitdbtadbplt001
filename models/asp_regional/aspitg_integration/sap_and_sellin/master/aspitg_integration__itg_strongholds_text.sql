{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key=  ['strongholds'],
        merge_exclude_columns=["crt_dttm"]
    )
}}

--import CTE

with source as (
   select * from {{ ref('aspwks_integration__wks_itg_strongholds_text') }}
),

--Logical CTE

final as(
    select 
        zstrong as strongholds,
        langu as language_key,
        txtsh as short_desc,
        txtmd as medium_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
--final select
select * from final