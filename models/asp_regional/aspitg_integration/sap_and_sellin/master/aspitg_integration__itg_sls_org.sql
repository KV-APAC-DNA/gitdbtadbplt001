{{
    config(
        alias="itg_sls_org",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["sls_org"],
        merge_exclude_columns = ["crt_dttm"],
        tags=["daily"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_sales_org') }}
),

final as (
    select
    mandt as clnt,
    vkorg as sls_org,
    waers as stats_crncy,
    bukrs as sls_org_co_cd,
    kunnr as cust_no_intco_bill,
    land1 as ctry_key,
    waers1 as crncy_key,
    periv as fisc_yr_vrnt,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  FROM source
)

select * from final