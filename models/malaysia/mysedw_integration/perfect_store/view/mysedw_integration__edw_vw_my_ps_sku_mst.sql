with source as 
(
    select * from snaposeitg_integration.itg_my_perfectstore_sku_mst
),

final as 
(
    SELECT 
        sku_no,
        description,
        client,
        manufacture,
        category,
        brand,
        sub_catgory,
        sub_brand,
        packsize,
        other,
        product_barcode,
        list_price_fib,
        list_price_unit,
        rcp,
        packing_config
    FROM source
)
select * from final