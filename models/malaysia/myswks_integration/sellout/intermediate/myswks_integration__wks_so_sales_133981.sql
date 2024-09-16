with source as(
    select * from {{ source('myssdl_raw', 'sdl_so_sales_133981') }} where file_name not in
    ( select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_so_sales_133981__lookup_test') }}
    )
),
final as(
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
        replace(gross_item_price, ',', '') as gross_item_price,
        replace(quantity, ',', '') as quantity,
        uom,
        replace(quantity_in_pieces, ',', '') as quantity_in_pieces,
        replace(quantity_after_conversion, ',', '') as quantity_after_conversion,
        replace(sub_total_1, ',', '') as sub_total_1,
        discount,
        replace(sub_total_2, ',', '') as sub_total_2,
        replace(bottom_line_discount, ',', '') as bottom_line_discount,
        replace(total_amt_after_tax, ',', '') as total_amt_after_tax,
        replace(total_amt_before_tax, ',', '') as total_amt_before_tax,
        sales_employee,
        custom_field_1,
        custom_field_2,
        custom_field_3,
        file_name 
    from source
)
select * from final