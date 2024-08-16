{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="delete from {{this}} where filename in (select distinct filename from {{ ref('mysitg_integration__sdl_my_daily_sellout_sales_fact') }});"
    )}}
with source as (
     select * from {{ ref('mysitg_integration__sdl_my_daily_sellout_sales_fact') }}
),
final as(
    select
        iff(trim(dstrbtr_id)= '', NULL, trim(dstrbtr_id))::varchar(255) as distributor_id,
        iff(trim(sls_ord_num)= '', NULL, trim(sls_ord_num))::varchar(255) as sales_order_number,
        iff(trim(sls_ord_dt)= '', NULL, trim(sls_ord_dt))::varchar(255) as sales_order_date,
        iff(trim(type)= '', NULL, trim(type))::varchar(255) as type,
        iff(trim(cust_cd)= '', NULL, trim(cust_cd))::varchar(255) as customer_code,
        iff(trim(dstrbtr_wh_id)= '', NULL, trim(dstrbtr_wh_id))::varchar(255) as distributor_wh_id,
        iff(trim(item_cd)= '', NULL, trim(item_cd))::varchar(255) as sap_material_id,
        iff(trim(dstrbtr_prod_cd)= '', NULL, trim(dstrbtr_prod_cd))::varchar(255) as product_code,
        iff(trim(ean_num)= '', NULL, trim(ean_num))::varchar(255) as product_ean_code,
        iff(trim(dstrbtr_prod_desc)= '', NULL, trim(dstrbtr_prod_desc))::varchar(255) as product_description,
        iff(trim(grs_prc)= '', NULL, trim(grs_prc))::varchar(255) as gross_item_price,
        iff(trim(qty)= '', NULL, trim(qty))::varchar(255) as quantity,
        iff(trim(uom)= '', NULL, trim(uom))::varchar(255) as uom,
        iff(trim(qty_pc)= '', NULL, trim(qty_pc))::varchar(255) as quantity_in_pieces,
        iff(trim(qty_aft_conv)= '', NULL, trim(qty_aft_conv))::varchar(255) as quantity_after_conversion,
        iff(trim(subtotal_1)= '', NULL, trim(subtotal_1))::varchar(255) as sub_total_1,
        iff(trim(discount)= '', NULL, trim(discount))::varchar(255) as discount,
        iff(trim(subtotal_2)= '', NULL, trim(subtotal_2))::varchar(255) as sub_total_2,
        iff(trim(bottom_line_dscnt)= '', NULL, trim(bottom_line_dscnt))::varchar(255) as bottom_line_discount,
        iff(trim(total_amt_aft_tax)= '', NULL, trim(total_amt_aft_tax))::varchar(255) as total_amt_after_tax,
        iff(trim(total_amt_bfr_tax)= '', NULL, trim(total_amt_bfr_tax))::varchar(255) as total_amt_before_tax,
        iff(trim(sls_emp)= '', NULL, trim(sls_emp))::varchar(255) as sales_employee,
        iff(trim(filename)= '', NULL, trim(filename))::varchar(255) as filename,        
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final