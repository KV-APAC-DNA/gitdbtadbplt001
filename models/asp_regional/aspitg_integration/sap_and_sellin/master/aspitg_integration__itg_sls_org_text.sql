{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["clnt","lang_key","sls_org"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_sales_org_text') }}
),

trans as (
    select
        mandt as clnt,
        spras as lang_key,
        vkorg as sls_org,
        vtext as sls_org_nm,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  FROM source
),
final as(
    select 
        clnt::varchar(3) as clnt,
        lang_key::varchar(1) as lang_key,
        sls_org::varchar(4) as sls_org,
        sls_org_nm::varchar(20) as sls_org_nm,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)

select * from final