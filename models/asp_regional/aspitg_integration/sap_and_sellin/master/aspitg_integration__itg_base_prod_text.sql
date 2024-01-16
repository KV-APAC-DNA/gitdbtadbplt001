{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["clnt","lang_key","base_prod"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_base_prod_text') }}
),

--Logical CTE

--Final CTE
final as (
    select
        mandt as clnt,
        spras as lang_key,
        mvgr1 as base_prod,
        bezei as base_prod_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

--Final select
select * from final