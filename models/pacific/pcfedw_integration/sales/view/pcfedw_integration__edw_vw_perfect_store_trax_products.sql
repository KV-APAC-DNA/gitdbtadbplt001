with
itg_trax_md_product as (
select * from {{ source('aspacshare_integration','trax_md_product') }}
where businessunitid = 'PC'
),
final as (
SELECT 
  'TRAX_PRODUCTS' AS data_source, 
  itg_trax_md_product.product_client_code AS ean_number, 
  itg_trax_md_product.product_name, 
  itg_trax_md_product.product_local_name, 
  itg_trax_md_product.product_short_name, 
  itg_trax_md_product.brand_name, 
  itg_trax_md_product.brand_local_name, 
  itg_trax_md_product.manufacturer_name, 
  itg_trax_md_product.manufacturer_local_name, 
  itg_trax_md_product.size, 
  itg_trax_md_product.unit_measurement, 
  itg_trax_md_product.category_name, 
  itg_trax_md_product.category_local_name, 
  itg_trax_md_product.subcategory_local_name 
FROM 
  itg_trax_md_product 
WHERE 
  (
    (
      (
        itg_trax_md_product.businessunitid
      ):: text = 'PC' :: text
    ) 
    AND (
      (
        itg_trax_md_product.manufacturer_name
      ):: text = 'JOHNSON & JOHNSON' :: text
    )
  )
)
select * from final
