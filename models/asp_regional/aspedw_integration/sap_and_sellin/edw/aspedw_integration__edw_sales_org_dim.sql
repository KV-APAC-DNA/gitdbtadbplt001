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

    select * from {{ ref('aspwks_integration__wks_edw_sales_org_dim') }}
),

final as (
    select
        clnt,
        sls_org,
        sls_org_nm,
        stats_crncy,
        sls_org_co_cd,
        cust_no_intco_bill as cust_num_intco_bill,
        ctry_key,
        crncy_key,
        fisc_yr_vrnt,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final