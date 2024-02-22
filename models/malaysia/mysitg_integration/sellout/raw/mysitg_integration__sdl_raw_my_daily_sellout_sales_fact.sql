{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as (
     select * from {{ ref('mysitg_integration__sdl_my_daily_sellout_sales_fact') }}
),
     
final as (
    select 
        dstrbtr_id,
        sls_ord_num,
        sls_ord_dt,
        type,
        cust_cd,
        dstrbtr_wh_id,
        item_cd,
        dstrbtr_prod_cd,
        ean_num,
        dstrbtr_prod_desc,
        grs_prc,
        qty,
        uom,
        qty_pc,
        qty_aft_conv,
        subtotal_1,
        discount,
        subtotal_2,
        bottom_line_dscnt,
        total_amt_aft_tax,
        total_amt_bfr_tax,
        sls_emp,
        custom_field1,
        custom_field2,
        custom_field3,
        filename,
        curr_dt,
        cdl_dttm
    from source
)

select * from final