--Import CTE   
with edw_rpt_regional_sellout_offtake as (
    select * from {{ ref('aspedw_integration__edw_rpt_regional_sellout_offtake') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
--Final CTE
TW_360_SELLOUT_MAPPED_SKU_CD as (

select ean_num,sku_code,msl_product_desc from (SELECT DISTINCT ltrim(msl_product_code,'0') AS ean_num,
       LTRIM(sku_code,'0') AS sku_code,
	   msl_product_desc as msl_product_desc, 
       ROW_NUMBER() OVER (PARTITION BY ltrim(msl_product_code,0) ORDER BY cal_date DESC, LENGTH(LTRIM(sku_code,'0')) DESC) AS rno
FROM EDW_RPT_REGIONAL_SELLOUT_OFFTAKE
WHERE COUNTRY_CODE = 'TW'
AND   data_source in ('SELL-OUT', 'POS')
and  MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  and mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric) WHERE rno = 1
	  ),

final as(
    select
ean_num::VARCHAR(100) as ean_num,
sku_code::VARCHAR(100) AS sku_code,
msl_product_desc::VARCHAR(500) as msl_product_desc
from TW_360_SELLOUT_MAPPED_SKU_CD
)				

select * from final	  