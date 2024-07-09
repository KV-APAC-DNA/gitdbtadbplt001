--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),

--final cte
wks_philippines_regional_sellout_mapped_sku_cd as 
(
    SELECT *
	FROM (SELECT DISTINCT LTRIM(MSL_PRODUCT_CODE,'0') AS MASTER_CODE,
				 LTRIM(SKU_CODE,'0') AS MAPPED_SKU_CD,
				 MSL_PRODUCT_DESC,
				 ROW_NUMBER() OVER (PARTITION BY LTRIM(MSL_PRODUCT_CODE,0) ORDER BY cal_date DESC,LENGTH(LTRIM(SKU_CODE,'0')) DESC) AS RNO
		  FROM EDW_RPT_REGIONAL_SELLOUT_OFFTAKE 
		  WHERE COUNTRY_CODE = 'PH'
		  AND   DATA_SOURCE IN ('SELL-OUT','POS'))
	WHERE RNO = 1
)

--final select
select * from wks_philippines_regional_sellout_mapped_sku_cd
