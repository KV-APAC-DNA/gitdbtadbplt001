with itg_pop6_products as 
(
    select * from snapntaitg_integration.itg_pop6_products
),
final as
(    
    SELECT 
        prod.cntry_cd,
        prod.src_file_date,
        prod.status,
        prod.productdb_id,
        prod.barcode,
        prod.sku,
        prod.unit_price,
        prod.display_order,
        prod.launch_date,
        prod.largest_uom_quantity,
        prod.middle_uom_quantity,
        prod.smallest_uom_quantity,
        prod.company,
        prod.sku_english,
        prod.sku_code,
        prod.ps_category,
        prod.ps_segment,
        prod.ps_category_segment,
        prod.country_l1,
        prod.regional_franchise_l2,
        prod.franchise_l3,
        prod.brand_l4,
        prod.sub_category_l5,
        prod.platform_l6,
        prod.variance_l7,
        prod.pack_size_l8
    FROM itg_pop6_products prod
    WHERE ((prod.active)::text = 'Y'::text)
)
select * from final