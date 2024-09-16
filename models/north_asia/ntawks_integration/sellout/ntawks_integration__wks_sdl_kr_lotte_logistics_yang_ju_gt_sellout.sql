with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_lotte_logistics_yang_ju_gt_sellout') }}
),
final as (
    select 
        dstr_nm,
        ims_txn_dt,
        heavy_classification,
        sub_classification,
        code,
        ean,
        product_name,
        account_name,
        ldu,
        supply_division,
        sls_qty,
        sls_amt,
        sales_priority,
        sales_stores,
        sales_rate::numeric(21,5) as sales_rate,
        cust_cd
 from source
)
select * from final