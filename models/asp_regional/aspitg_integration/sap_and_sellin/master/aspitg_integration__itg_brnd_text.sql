{{
    config(
        alias= "itg_brnd_text",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["clnt","lang_key","brnd"],
        merge_exclude_columns= ["crt_dttm"],
        tags= ["daily"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_brnd_text') }}
),

--Logical CTE

--Final CTE
final as (
    select
        mandt as clnt,
        spras as lang_key,
        mvgr5 as brnd,
        bezei as brnd_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

--Final select
select * from final 