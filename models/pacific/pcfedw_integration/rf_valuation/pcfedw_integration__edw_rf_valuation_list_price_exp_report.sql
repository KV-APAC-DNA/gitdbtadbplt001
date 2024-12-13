with VW_DEMAND_FORECAST_ANALYSIS as
(
  select * from {{ source('pcfedw_integration','vw_demand_forecast_analysis') }}
),
itg_list_price_sap as
(
  select * from prod_dna_core.dbt_cloud_pr_5458_1542.aspitg_integration__itg_list_price_sap
),
sdl_pcf_price_list_exception as
(
  select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_rf_valuation_exception_list_price_adftemp') }}
),


--APO 

VW_APO as
(select distinct
sum(  apo_tot_frcst  ) as  apo_tot_frcst ,
  country  ,
  fcst_chnl  ,
  jj_year  ,
  matl_desc  ,
  matl_no ,
  master_code ,
  jj_period 
from VW_DEMAND_FORECAST_ANALYSIS
 where   pac_subsource_type  ='SAPBW_APO_FORECAST' AND   jj_year  >=EXTRACT(YEAR FROM CURRENT_DATE)
 group by 
  country  ,
  fcst_chnl  ,
  jj_year  ,
  matl_desc  ,
  matl_no ,
  master_code ,
  jj_period )


--LIST PRICE MAIN TABLE INTEGRATION FOR APO

,VW_LIST_PRICE AS
(select distinct ltrim(ITG_LIST.material,'0') AS material,ITG_LIST. VALID_TO ,ITG_LIST. DT_FROM ,
CASE
        WHEN cast(LEFT(ITG_LIST.DT_FROM, 6)as number) < APO. jj_period 
             AND cast(LEFT(ITG_LIST.VALID_TO, 6)as number) >= APO.  jj_period   THEN ITG_LIST.AMOUNT
             ELSE NULL END AS  AMOUNT 
		,ITG_LIST.currency 
		from VW_APO APO
	LEFT JOIN itg_list_price_sap  ITG_LIST

ON apo.  matl_no  =ltrim(ITG_LIST.material,'0') and case when apo.  country  ='Australia' then 'AUD' when apo.  country  ='New Zealand' then 'NZD'
else null end =ITG_LIST.currency
where 
    cast(LEFT(ITG_LIST.DT_FROM, 6)as number) < APO.  jj_period  
             AND cast(LEFT(ITG_LIST.VALID_TO, 6)as number) >= APO.  jj_period   AND ITG_LIST.sls_org in ('3410','3300')
)

--EXCEPTION LIST PRICE INTEGRATION FOR APO

,VW_EXP_LIST AS 
( select distinct concat(e_list.country,'D') AS COUNTRY,  e_list.material_code AS product,e_list.list_price_local 
from  VW_APO APO
LEFT JOIN sdl_pcf_price_list_exception e_list

on  apo.  matl_no  =e_list.material_code and case when apo.  country  ='Australia' then 'AUD' when apo.  country  ='New Zealand' then 'NZD'
else null end =concat(e_list.country,'D')
WHERE e_list.list_price_local IS NOT NULL
)

--FINAL LIST PRICE CALCULATION FOR APO
 
,LIST_PRICE_CAL AS
(
select distinct
  apo.country  ,
  jj_year  ,
  matl_desc  ,
  matl_no ,
  master_code ,
	CURRENT_DATE as update_date
from VW_APO APO
LEFT JOIN VW_LIST_PRICE LIST_PRICE
ON APO.  matl_no  =LIST_PRICE.material
and cast(LEFT(DT_FROM, 6)as number) < APO.  jj_period  
AND cast(LEFT(VALID_TO, 6)as number) >= APO.  jj_period 
and case when apo.  country  ='Australia' then 'AUD' when apo.  country  ='New Zealand' then 'NZD'
else null end =LIST_PRICE.currency

LEFT JOIN VW_EXP_LIST EXP_LIST
ON APO.  matl_no  =EXP_LIST.PRODUCT 
and  case when apo.  country  ='Australia' then 'AUD' when apo.  country  ='New Zealand' then 'NZD'
else null end =EXP_LIST.country

where  fcst_chnl  not in ('AUS','NZS') and  fcst_chnl  is not null and  apo_tot_frcst <>0 and CASE
        WHEN LIST_PRICE.AMOUNT IS NOT NULL THEN LIST_PRICE.AMOUNT
             WHEN EXP_LIST.list_price_local IS NOT NULL THEN EXP_LIST.list_price_local
			 ELSE '0.00'
    END = '0.00')

select * from LIST_PRICE_CAL
