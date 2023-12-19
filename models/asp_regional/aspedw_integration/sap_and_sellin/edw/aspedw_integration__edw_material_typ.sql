{{
    config(
        alias= "edw_material_typ",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["matl_type","langu"],
        merge_exclude_columns= ["crt_dttm"],
        tags= ["daily"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_edw_material_typ') }}
),

--Logical CTE

--Final CTE
final as (
    select
        matl_type,
        langu,
        txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

--Final select
select * from final