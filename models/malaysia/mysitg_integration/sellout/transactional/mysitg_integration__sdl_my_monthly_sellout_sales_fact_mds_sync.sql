{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="delete from {{this}} where filename in (select distinct filename from {{ source('myssdl_raw','sdl_my_monthly_sellout_sales_fact') }});"
    )}}

with source as (
     select * from {{ source('myssdl_raw','sdl_my_monthly_sellout_sales_fact') }} ),
     
final as (
    select 
        dstrbtr_id::varchar(255) as distributor_id,
        sls_ord_num::varchar(255) as sales_order_number,
        sls_ord_dt::varchar(255) as sales_order_date,
        type::varchar(255) as type,
        cust_cd::varchar(255) as customer_code,
        dstrbtr_wh_id::varchar(255) as distributor_wh_id,
        item_cd::varchar(255) as sap_material_id,
        dstrbtr_prod_cd::varchar(255) as product_code,
        ean_num::varchar(255) as product_ean_code,
        dstrbtr_prod_desc::varchar(255) as product_description,
        grs_prc::varchar(255) as gross_item_price,
        qty::varchar(255) as quantity,
        uom::varchar(255) as uom,
        qty_pc::varchar(255) as quantity_in_pieces,
        qty_aft_conv::varchar(255) as quantity_after_conversion,
        subtotal_1::varchar(255) as sub_total_1,
        discount::varchar(255) as discount,
        subtotal_2::varchar(255) as sub_total_2,
        bottom_line_dscnt::varchar(255) as bottom_line_discount,
        total_amt_aft_tax::varchar(255) as total_amt_after_tax,
        total_amt_bfr_tax::varchar(255) as total_amt_before_tax,
        sls_emp::varchar(255) as sales_employee,
        filename::varchar(100) as filename,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)

select * from final