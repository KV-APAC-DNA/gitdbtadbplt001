{{ 
    config(
    sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )}}
with WKS_INDIA_BASE_RETAIL_EXCELLENCE as 
(
select * from {{ ref('indwks_integration__wks_india_base_retail_excellence') }}
),
edw_vw_cal_Retail_excellence_Dim as 
(
select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
edw_vw_os_time_dim as
(
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
WKS_INDIA_REGIONAL_SELLOUT_ALLMONTHS as(
SELECT all_months.cntry_cd,all_months.sellout_dim_key, all_months.month, base.SO_SLS_QTY, base.SO_SLS_VALUE, base.SO_AVG_QTY,BASE.SALES_VALUE_LIST_PRICE
FROM 
(
SELECT distinct cntry_cd,sellout_dim_key, month 
FROM    
     (SELECT distinct cntry_cd,sellout_dim_key 
        FROM WKS_INDIA_BASE_RETAIL_EXCELLENCE
      ) a
     ,
      (select distinct "year",mnth_id as month
          from edw_vw_os_time_dim 
		  where MNTH_ID >= (select last_27mnths from edw_vw_cal_Retail_excellence_Dim)
                  --where mnth_id >=(select to_char(add_months((SELECT to_date(MAX(mnth_id),'YYYYMM') FROM rg_edw.edw_rpt_regional_sellout_offtake where country_code='IN' and data_source='SELL-OUT'  ),-(select cast(parameter_value as int) from in_itg.itg_query_parameters where parameter_name='RETAIL_EXCELLENCE_ACTUALS_NO_OF_MONTHS')),'yyyymm'))
                  and  mnth_id <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM'))
       ) b
) all_months

LEFT JOIN WKS_INDIA_BASE_RETAIL_EXCELLENCE base 
ON   all_months.cntry_cd          =  base.cntry_cd
AND  all_months.sellout_dim_key   =  base.sellout_dim_key    
AND  all_months.MONTH             =  base.MNTH_ID
),
final as(
select 
	cntry_cd::varchar(2) AS cntry_cd,
	sellout_dim_key::varchar(32) AS sellout_dim_key,
	month::varchar(23) AS month,
	so_sls_qty::numeric(38,6) AS so_sls_qty,
	so_sls_value::numeric(38,6) AS so_sls_value,
	so_avg_qty::numeric(38,6) AS so_avg_qty,
	sales_value_list_price::numeric(38,12) AS sales_value_list_price

from WKS_INDIA_REGIONAL_SELLOUT_ALLMONTHS
)
select * from final 