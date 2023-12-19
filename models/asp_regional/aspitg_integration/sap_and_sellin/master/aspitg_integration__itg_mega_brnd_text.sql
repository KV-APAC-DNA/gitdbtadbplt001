{{
    config(
        alias= "itg_mega_brnd_text",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["clnt","lang_key","mega_brnd"],
        merge_exclude_columns= ["crt_dttm"],
        tags= ["daily"]
    )
}}

--Import CTE 
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_mega_brnd_text') }}
),

--Logical CTE

--Final CTE
final as (
    select
    mandt as clnt,
    spras as lang_key,
    mvgr4 as mega_brnd,
    bezei as de,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

--Final select
select * from final