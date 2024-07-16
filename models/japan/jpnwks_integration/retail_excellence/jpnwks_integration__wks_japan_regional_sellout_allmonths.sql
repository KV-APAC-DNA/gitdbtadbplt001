 with WKS_JAPAN_BASE_RETAIL_EXCELLENCE as (
    select * from {{ ref('jpnwks_integration__wks_japan_base_retail_excellence') }}
 ),
 edw_vw_cal_Retail_excellence_Dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
 ),

EDW_VW_OS_TIME_DIM as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),

transformation as (
SELECT ALL_MONTHS.CNTRY_CD,
       ALL_MONTHS.SELLOUT_DIM_KEY,
       ALL_MONTHS.MONTH,
       BASE.SO_SLS_QTY,
       BASE.SO_SLS_VALUE,
       BASE.SO_AVG_QTY,
       BASE.SALES_VALUE_LIST_PRICE
FROM (SELECT DISTINCT CNTRY_CD,
             SELLOUT_DIM_KEY,
             MONTH
      FROM (SELECT DISTINCT CNTRY_CD,
                   SELLOUT_DIM_KEY
            FROM WKS_JAPAN_BASE_RETAIL_EXCELLENCE where  MNTH_ID >= (select last_27mnths from edw_vw_cal_Retail_excellence_Dim)
	  and mnth_id <= (select prev_mnth from edw_vw_cal_Retail_excellence_Dim)) A,
           (SELECT DISTINCT "year",
                   MNTH_ID AS MONTH
            FROM EDW_VW_OS_TIME_DIM
            WHERE MNTH_ID >= (select last_27mnths from edw_vw_cal_Retail_excellence_Dim)
            ---MNTH_ID >= TO_CHAR(ADD_MONTHS((SELECT to_date(MAX(mnth_id),'YYYYMM') FROM rg_edw.edw_rpt_regional_sellout_offtake where country_code='JP' and data_source='SELL-OUT'  ),-36),'YYYYMM')
            AND   MNTH_ID <= (SELECT TO_CHAR(DATEADD(DAY, 90, SYSDATE()), 'yyyymm'))) B) ALL_MONTHS
             --(SELECT TO_CHAR(DATEADD(DAY, 90, SYSDATE()), 'yyyymm')))) b)
  LEFT JOIN (select * from WKS_JAPAN_BASE_RETAIL_EXCELLENCE where  MNTH_ID >= (select last_27mnths from edw_vw_cal_Retail_excellence_Dim)
	  and mnth_id <= (select prev_mnth from edw_vw_cal_Retail_excellence_Dim))  BASE
         ON ALL_MONTHS.CNTRY_CD = BASE.CNTRY_CD
        AND ALL_MONTHS.SELLOUT_DIM_KEY = BASE.SELLOUT_DIM_KEY
        AND ALL_MONTHS.MONTH = BASE.MNTH_ID
),

final as (
    select 
    *
    from transformation
)

select * from final