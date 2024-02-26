{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["sls_org"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_sales_org') }}
),

trans as (
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
),

final as(
    select 
        clnt::number(18,0) as clnt,
        sls_org::varchar(4) as sls_org,
        stats_crncy::varchar(5) as stats_crncy,
        sls_org_co_cd::varchar(4) as sls_org_co_cd,
        cust_no_intco_bill::varchar(10) as cust_no_intco_bill,
        ctry_key::varchar(3) as ctry_key,
        crncy_key::varchar(5) as crncy_key,
        fisc_yr_vrnt::varchar(2) as fisc_yr_vrnt,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans 
)
select * from final