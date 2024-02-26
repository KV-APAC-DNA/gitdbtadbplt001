{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["clnt","lang_key","mega_brnd"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE 
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_mega_brnd_text') }}
),

--Logical CTE

--Final CTE
trans as (
    select
        mandt as clnt,
        spras as lang_key,
        mvgr4 as mega_brnd,
        bezei as de,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
),
final as(
    select 
        clnt::number(18,0) as clnt,
        lang_key::varchar(1) as lang_key,
        mega_brnd::varchar(3) as mega_brnd,
        de::varchar(40) as de,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans

)

--Final select
select * from final