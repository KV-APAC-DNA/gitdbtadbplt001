{{
    config(
        alias= "itg_needstates_text",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["need_states"],
        merge_exclude_columns= ["crt_dttm"],
        tags= ["daily"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_needstates_text') }}
),

--Logical CTE

--Final CTE
final as (
    SELECT
    zneed as need_states,
    langu as language_key,
    txtsh as short_desc,
    txtmd as medium_desc,
    -- case
    --   when chng_flg = 'i'
    --   then convert_timezone('sgt', current_timestamp())::timestamp_ntz(9)
    --   else tgt_crt_dttm
    --end as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

--Final select
select * from final
