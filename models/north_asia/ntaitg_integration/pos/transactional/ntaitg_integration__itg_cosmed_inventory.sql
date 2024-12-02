WITH sdl_tw_pos_cosmed_store_inventory AS 
(SELECT * FROM {{ ref('ntawks_integration__wks_itg_pos_cosmed_store_inventory') }}),

sdl_tw_pos_cosmed_dc_inventory AS 
(SELECT * FROM {{ ref('ntawks_integration__wks_itg_pos_cosmed_dc_inventory') }}),

final as (
SELECT 
    t2.BARCODE,
    t2.PRODUCT_CODE,
    t2.PRODUCT_NAME,
    TO_VARCHAR(TO_DATE(t2.inventory_date, 'YYYY/MM/DD'), 'YYYYMM') as MNTH_ID,
    (COALESCE(t1.INVENTORY_QTY, 0) + COALESCE(t2.INVENTORY_QTY, 0)) AS TOTAL_INVENTORY_QTY
FROM 
    sdl_tw_pos_cosmed_dc_inventory t2
LEFT JOIN 
    sdl_tw_pos_cosmed_store_inventory t1
    ON t2.PRODUCT_CODE = t1.PRODUCT_CODE
    AND TO_VARCHAR(TO_DATE(t2.inventory_date, 'YYYY/MM/DD'), 'YYYYMM') = t1.MNTH_ID
WHERE
    t2.PRODUCT_CODE IS NOT NULL
GROUP BY 
    t2.BARCODE,
    t2.PRODUCT_CODE,
    t2.PRODUCT_NAME,
    TO_VARCHAR(TO_DATE(t2.inventory_date, 'YYYY/MM/DD'), 'YYYYMM'),
    t1.INVENTORY_QTY,
    t2.INVENTORY_QTY)

select 
'Cosmed 康是美'::VARCHAR(50) as src_sys_cd,
BARCODE::VARCHAR(50) as barcode,
PRODUCT_CODE::VARCHAR(50) as PRODUCT_CODE,
PRODUCT_NAME::VARCHAR(255) as product_name,
MNTH_ID::VARCHAR(50) as mnth_id,
TOTAL_INVENTORY_QTY::NUMERIC(18,2) as TOTAL_INVENTORY_QTY
from final