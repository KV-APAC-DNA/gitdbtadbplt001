with source as (
    select * from {{ ref('ntaedw_integration__v_rpt_onpack') }}
),
final as (
    select
        fisc_yr as "fisc_yr",
        fisc_mo as "fisc_mo",
        ctry_cd as "ctry_cd",
        ctry_nm as "ctry_nm",
        matl_num as "matl_num",
        matl_desc as "matl_desc",
        mega_brnd_desc as "mega_brnd_desc",
        sls_grp as "sls_grp",
        sls_grp_desc as "sls_grp_desc",
        channel as "channel",
        store_type as "store_type",
        sales_office as "sales_office",
        invoice_qty as "invoice_qty",
        target_qty as "target_qty",
        curr_inventory_qty as "curr_inventory_qty",
        bonus_desc as "bonus_desc"
    from source
)
select * from final