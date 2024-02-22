{{
    config(
        materialized="incremental",
        incremental_strategy = "delete+insert",
        unique_key=["filename"]
    )}}

with source as (
     select * from {{ ref('mysitg_integration__sdl_my_daily_sellout_sales_fact') }} 
),
final as(
    select distributor_id::varchar(255) as distributor_id,
    sales_order_number::varchar(255) as sales_order_number,
    sales_order_date::varchar(255) as sales_order_date,
    type::varchar(255) as type,
    customer_code::varchar(255) as customer_code,
    distributor_wh_id::varchar(255) as distributor_wh_id,
    sap_material_id::varchar(255) as sap_material_id,
    product_code::varchar(255) as product_code,
    product_ean_code::varchar(255) as product_ean_code,
    product_description::varchar(255) as product_description,
    gross_item_price::varchar(255) as gross_item_price,
    quantity::varchar(255) as quantity,
    uom::varchar(255) as uom,
    quantity_in_pieces::varchar(255) as quantity_in_pieces,
    quantity_after_conversion::varchar(255) as quantity_after_conversion,
    sub_total_1::varchar(255) as sub_total_1,
    discount::varchar(255) as discount,
    sub_total_2::varchar(255) as sub_total_2,
    bottom_line_discount::varchar(255) as bottom_line_discount,
    total_amt_after_tax::varchar(255) as total_amt_after_tax,
    total_amt_before_tax::varchar(255) as total_amt_before_tax,
    sales_employee::varchar(255) as sales_employee,
    filename::varchar(200) as filename,
    current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final

     