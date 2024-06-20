--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),

--final cte
singapore_regional_sellout_mapped_sku_cd  as (
SELECT *
FROM (SELECT DISTINCT LTRIM(MSL_PRODUCT_CODE,'0') AS MASTER_CODE,
             LTRIM(SKU_CODE,'0') AS MAPPED_SKU_CD,
             SKU_DESCRIPTION,
             ROW_NUMBER() OVER (PARTITION BY LTRIM(MSL_PRODUCT_CODE,0) ORDER BY cal_date DESC,LENGTH(LTRIM(SKU_CODE,'0')) DESC) AS RNO
      FROM EDW_RPT_REGIONAL_SELLOUT_OFFTAKE
      WHERE COUNTRY_CODE = 'SG'
      AND   DATA_SOURCE = 'POS')
WHERE RNO = 1

),
final as(

    select 
    master_code::varchar(150) AS master_code,
mapped_sku_cd::varchar(40) AS mapped_sku_cd,
sku_description::varchar(150) AS sku_description,
rno::numeric(38,0) AS rno
from singapore_regional_sellout_mapped_sku_cd
)

--final select
select * from final