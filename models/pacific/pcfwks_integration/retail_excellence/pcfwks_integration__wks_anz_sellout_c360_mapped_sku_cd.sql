with EDW_RPT_REGIONAL_SELLOUT_OFFTAKE as(
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),

transformation as (
select * from (SELECT DISTINCT ltrim(msl_product_code,'0') AS EAN,
       LTRIM(sku_code,'0') AS sku_code,
	   msl_product_desc as sku_description,COUNTRY_CODE ,
       ROW_NUMBER() OVER (PARTITION BY ltrim(msl_product_code,0),COUNTRY_CODE ORDER BY cal_date DESC, LENGTH(LTRIM(sku_code,'0')) DESC) AS rno
FROM EDW_RPT_REGIONAL_SELLOUT_OFFTAKE   --//RG_EDW.EDW_RPT_REGIONAL_SELLOUT_OFFTAKE
WHERE COUNTRY_CODE  in ('AU','NZ')
AND   data_source in ('SELL-OUT')) WHERE rno = 1),

final as (
select 
ean::VARCHAR(150) AS ean,
sku_code::VARCHAR(40) AS sku_code,
sku_description::VARCHAR(225) AS sku_description,
country_code::VARCHAR(2) as country_code,
rno::numeric(38,0) as rno
from transformation
)

select * from final