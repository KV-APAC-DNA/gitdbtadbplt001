{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["clnt","lang_key","put_up"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_put_up_text') }}
),

--Logical CTE

--Final CTE
trans as (
    select
        mandt as clnt,
        spras as lang_key,
        mvgr3 as put_up,
        bezei as put_up_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
),
final as(
    select
        clnt::number(18,0) as clnt,
        lang_key::varchar(1) as lang_key,
        put_up::varchar(3) as put_up,
        put_up_desc::varchar(40) as put_up_desc,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)

--Final select
select * from final