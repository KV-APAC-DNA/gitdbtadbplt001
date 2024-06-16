--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),

--final cte
korea_regional_sellout_mapped_sku_cd  as (
select * from (SELECT DISTINCT ltrim(msl_product_code,'0') AS ean_num,
       LTRIM(sku_code,'0') AS sku_code,
	   msl_product_desc as sku_description,
       ROW_NUMBER() OVER (PARTITION BY ltrim(msl_product_code,0) ORDER BY cal_date DESC, LENGTH(LTRIM(sku_code,'0')) DESC) AS rno
FROM edw_rpt_regional_sellout_offtake	--// FROM RG_EDW.EDW_RPT_REGIONAL_SELLOUT_OFFTAKE
WHERE COUNTRY_CODE = 'KR'
AND   data_source in ('SELL-OUT','POS')) WHERE rno = 1
),
final as(

    ean_num::varchar(150) AS ean_num,
sku_code::varchar(40) AS sku_code,
sku_description::varchar(300) AS sku_description,
rno::numeric(38,0) AS rno,
from korea_regional_sellout_mapped_sku_cd
)

--final select
select * from final