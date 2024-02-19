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
trans as (
    select
        mandt as clnt,
        spras as lang_key,
        mvgr1 as base_prod,
        bezei as base_prod_desc,
        current_timestamp()as crt_dttm,
        current_timestamp() as updt_dttm
    from source
),
final as(
    select 
        clnt::number(18,0) as clnt,
        lang_key::varchar(1) as lang_key,
        base_prod::varchar(3) as base_prod,
        base_prod_desc::varchar(40) as base_prod_desc,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)

--Final select
select * from final