{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
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

final as (
    select
        mandt as clnt,
        spras as lang_key,
        vkorg as sls_org,
        vtext as sls_org_nm,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  FROM source
)

select * from final