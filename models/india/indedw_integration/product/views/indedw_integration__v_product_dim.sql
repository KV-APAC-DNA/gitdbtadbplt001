with edw_material_dim as 
(
    select * from {{ ref('aspedw_access__edw_material_dim') }}
),
edw_product_dim as
(
    select * from {{ ref('indedw_integration__edw_product_dim') }} 
),
final as 
(
SELECT b.product_code,
    b.product_name,
    b.product_desc,
    b.franchise_code,
    b.franchise_name,
    b.brand_code,
    b.brand_name,
    b.product_category_code,
    b.product_category_name,
    b.variant_code,
    b.variant_name,
    b.mothersku_code,
    b.mothersku_name,
    b.uom,
    b.std_nr,
    b.case_lot,
    b.sale_uom,
    b.sale_conversion_factor,
    b.base_uom,
    b.int_uom,
    b.gross_wt,
    b.net_wt,
    b.active_flag,
    b.delete_flag,
    b.shelf_life,
    a.crt_dttm,
    a.updt_dttm
FROM (
    edw_material_dim a JOIN edw_product_dim b ON ((ltrim((a.matl_num)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = (b.product_code)::TEXT))
    )

UNION ALL

SELECT '-1' AS product_code,
    'Unknown' AS product_name,
    'Unknown' AS product_desc,
    '-1' AS franchise_code,
    'Unknown' AS franchise_name,
    '-1' AS brand_code,
    'Unknown' AS brand_name,
    '-1' AS product_category_code,
    'Unknown' AS product_category_name,
    '-1' AS variant_code,
    'Unknown' AS variant_name,
    '-1' AS mothersku_code,
    'Unknown' AS mothersku_name,
    0 AS uom,
    0 AS std_nr,
    0 AS case_lot,
    0 AS sale_uom,
    0 AS sale_conversion_factor,
    0 AS base_uom,
    0 AS int_uom,
    0 AS gross_wt,
    0 AS net_wt,
    'Y' AS active_flag,
    'N' AS delete_flag,
    0 AS shelf_life,
    getdate() AS crt_dttm,
    getdate() AS updt_dttm
)
select * from final
