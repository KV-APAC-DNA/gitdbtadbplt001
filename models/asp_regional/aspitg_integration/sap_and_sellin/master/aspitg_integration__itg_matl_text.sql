{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["matl_no"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_matl_text') }}
),

--Logical CTE

--Final CTE
final as (
    select
        matnr as matl_no,
        spras as lang_key,
        txtmd as matl_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

--Final select
select * from final