with itg_xdm_product as
(
    select * from {{ ref('inditg_integration__itg_xdm_product') }}
),
itg_xdm_productuom as
(
    select * from {{ ref('inditg_integration__itg_xdm_productuom') }}
),
edw_product_dim as
(
    select * from {{ source('indedw_integration', 'edw_product_dim') }}
),
final as
(
    SELECT src.product_code,
       product_name,
       product_desc,
       franchise_code,
       franchise_name,
       brand_code,
       brand_name,
       product_category_code,
       product_category_name,
       variant_code,
       variant_name,
       mothersku_code,
       mothersku_name,
       uom,
       std_nr,
       case_lot,
       sale_uom,
       sale_conversion_factor,
       base_uom,
       int_uom,
       gross_wt,
       net_wt,
       active_flag,
       delete_flag,
       shelf_life,
       CASE
         WHEN tgt.crt_dttm IS NULL THEN src.crt_dttm
         ELSE tgt.crt_dttm
       END AS crt_dttm,
       CASE
         WHEN tgt.crt_dttm IS NULL THEN NULL
         ELSE convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)
       END AS updt_dttm,
       CASE
         WHEN tgt.crt_dttm IS NULL THEN 'I'
         ELSE 'U'
       END AS chng_flg
FROM (SELECT prd.ProductCode AS Product_Code,
             COALESCE(prd.ProductName,'Unknown') AS Product_Name,
             COALESCE(prd.ProductName,'Unknown') AS Product_Desc,
             CASE
               WHEN prd.FranchiseCode IS NULL THEN '-1'
               ELSE prd.FranchiseCode
             END AS Franchise_Code,
             COALESCE(prd.FranchiseName,'Unknown') AS Franchise_Name,
             CASE
               WHEN prd.BrandCode IS NULL THEN '-1'
               ELSE prd.BrandCode
             END AS Brand_Code,
             COALESCE(prd.BrandName,'Unknown') AS Brand_Name,
             CASE
               WHEN prd.Product_Code IS NULL THEN '-1'
               ELSE prd.Product_Code
             END AS Product_Category_Code,
             COALESCE(prd.Product_Name,'Unknown') AS Product_Category_Name,
             CASE
               WHEN prd.VariantCode IS NULL THEN '-1'
               ELSE prd.VariantCode
             END AS Variant_Code,
             COALESCE(prd.VariantName,'Unknown') AS Variant_Name,
             CASE
               WHEN prd.MOTHERSKUCode IS NULL THEN '-1'
               ELSE prd.MOTHERSKUCode
             END AS MotherSKU_code,
             COALESCE(prd.MOTHERSKUName,'Unknown') AS MotherSKU_Name,
             NULL AS uom,             
             NULL AS std_nr,
             NULL AS case_lot,
             NULL AS sale_uom,
             puom.uomconvfactor AS sale_conversion_factor,
             NULL AS base_uom,
             NULL AS int_uom,
             NULL AS gross_wt,
             NULL AS Net_wt,
             COALESCE(prd.ProductStatus,'Unknown') AS Active_Flag,
             NULL AS delete_flag,
             CASE
               WHEN prd.ShelfLife IS NULL THEN 0
               ELSE prd.ShelfLife
             END AS Shelf_Life,
             convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS crt_dttm,
             convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS updt_dttm
      FROM itg_xdm_product prd
        LEFT JOIN itg_xdm_productuom puom ON prd.ProductCode = puom.ProdCode
      WHERE puom.UOMCODE = 'PC'
	  ) src
  LEFT OUTER JOIN (SELECT product_code, crt_dttm FROM edw_product_dim) tgt ON src.product_code = tgt.product_code
)

select product_code::varchar(50) as product_code,
    product_name::varchar(400) as product_name,
    product_desc::varchar(400) as product_desc,
    franchise_code::varchar(50) as franchise_code,
    franchise_name::varchar(100) as franchise_name,
    brand_code::varchar(50) as brand_code,
    brand_name::varchar(100) as brand_name,
    product_category_code::varchar(50) as product_category_code,
    product_category_name::varchar(100) as product_category_name,
    variant_code::varchar(50) as variant_code,
    variant_name::varchar(100) as variant_name,
    mothersku_code::varchar(50) as mothersku_code,
    mothersku_name::varchar(100) as mothersku_name,
    uom::varchar(1) as uom,
    std_nr::varchar(1) as std_nr,
    case_lot::varchar(1) as case_lot,
    sale_uom::varchar(1) as sale_uom,
    sale_conversion_factor::number(18,0) as sale_conversion_factor,
    base_uom::varchar(1) as base_uom,
    int_uom::varchar(1) as int_uom,
    gross_wt::varchar(1) as gross_wt,
    net_wt::varchar(1) as net_wt,
    active_flag::varchar(7) as active_flag,
    delete_flag::varchar(1) as delete_flag,
    shelf_life::number(18,0) as shelf_life,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    chng_flg::varchar(1) as chng_flg
 from final