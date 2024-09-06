--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),

--final cte
wks_vn_regional_sellout_mapped_sku_cd as 
(
    SELECT *
	FROM (SELECT DISTINCT LTRIM(MSL_PRODUCT_CODE,'0') AS EAN_NUM,
				 LTRIM(SKU_CODE,'0') AS SKU_CODE,
				 MSL_PRODUCT_DESC AS SKU_DESCRIPTION,
				 ROW_NUMBER() OVER (PARTITION BY LTRIM(MSL_PRODUCT_CODE,0) ORDER BY cal_date DESC,LENGTH(LTRIM(SKU_CODE,'0')) DESC) AS RNO
		  FROM EDW_RPT_REGIONAL_SELLOUT_OFFTAKE 
		  WHERE COUNTRY_CODE = 'VN'
		  AND   DATA_SOURCE in ('SELL-OUT', 'POS'))
	WHERE RNO = 1
),

final as 
(
    select ean_num :: varchar(150) as ean_num,
    sku_code :: varchar(40) as sku_code,
    sku_description :: varchar(500) as sku_description,
    rno :: numeric(18,0) as rno
    from wks_vn_regional_sellout_mapped_sku_cd
)

--final select
select * from final 