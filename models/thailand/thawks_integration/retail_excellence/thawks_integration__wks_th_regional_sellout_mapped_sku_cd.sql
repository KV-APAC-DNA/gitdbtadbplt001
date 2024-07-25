with EDW_RPT_REGIONAL_SELLOUT_OFFTAKE as(
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),
edw_vw_cal_Retail_excellence_Dim as(
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
transformation as (
SELECT ean,
       sku_code,
       sku_description
FROM (SELECT DISTINCT LTRIM(MSL_PRODUCT_CODE,'0') AS ean,
             LTRIM(sku_code,'0') AS sku_code,
             UPPER(sku_description) AS sku_description,
             ROW_NUMBER() OVER (PARTITION BY LTRIM(MSL_PRODUCT_CODE,'0') ORDER BY crtd_dttm DESC,mnth_id DESC) AS rno
      FROM EDW_RPT_REGIONAL_SELLOUT_OFFTAKE		--//       FROM RG_EDW.EDW_RPT_REGIONAL_SELLOUT_OFFTAKE
      WHERE COUNTRY_NAME = 'Thailand'
      AND   DATA_SOURCE IN ('SELL-OUT','POS')
      AND   sku_code IS NOT NULL
      and MNTH_ID >= (select last_27mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	           and mnth_id <= (select  prev_mnth from edw_vw_cal_Retail_excellence_Dim)::numeric)
WHERE rno = 1),

final as (
select 
ean::VARCHAR(150) AS ean,
sku_code::VARCHAR(40) AS sku_code,
sku_description::VARCHAR(225) AS sku_description
from transformation
)

select * from final
