--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),

--final cte
wks_vn_regional_sellout_region as 
(
    SELECT *
	FROM (SELECT DISTINCT
				 UPPER(LTRIM(STORE_CODE,'0')) AS STORE_CODE,
				 REGION,
				 ROW_NUMBER() OVER (PARTITION BY UPPER(LTRIM(STORE_CODE,'0')) ORDER BY cal_date DESC) AS RNO
		  FROM EDW_RPT_REGIONAL_SELLOUT_OFFTAKE  
		  WHERE COUNTRY_CODE = 'VN'
		  AND   DATA_SOURCE in ('SELL-OUT', 'POS'))
	WHERE RNO = 1
),

final as 
(
    select store_code :: varchar(150) as store_code,
    region :: varchar(150) as region,
    rno :: integer as rno
    from wks_vn_regional_sellout_region
)

--final select
select * from final 