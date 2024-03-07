with source as(
    select * from {{ source('myssdl_raw', 'sdl_so_sales_130516') }}
),
final as
(
    select 
        distributor_id,
        sales_order_number,
        sales_order_date,
        type,
        customer_code,
        distributor_wh_id,
        sap_material_id,
        product_code,
        product_ean_code,
        product_description,
        REPLACE(gross_item_price, ',', '') as gross_item_price,
        REPLACE(quantity, ',', '') as quantity,
        uom,
        REPLACE(quantity_in_pieces, ',', '') as quantity_in_pieces,
        REPLACE(quantity_after_conversion, ',', '') as quantity_after_conversion,
        REPLACE(sub_total_1, ',', '') as sub_total_1,
        discount,
        REPLACE(sub_total_2, ',', '') as sub_total_2,
        REPLACE(bottom_line_discount, ',', '') as bottom_line_discount,
        REPLACE(total_amt_after_tax, ',', '') as total_amt_after_tax,
        REPLACE(total_amt_before_tax, ',', '') as total_amt_before_tax,
        sales_employee,
        custom_field_1,
        custom_field_2,
        custom_field_3,
        file_name 
    from source
)
select * from final