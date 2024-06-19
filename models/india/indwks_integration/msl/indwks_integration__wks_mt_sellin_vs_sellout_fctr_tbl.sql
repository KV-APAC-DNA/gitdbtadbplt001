with itg_query_parameters as(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
edw_retailer_calendar_dim as(
    select * from DEV_DNA_CORE.INDEDW_INTEGRATION.edw_retailer_calendar_dim
),
wks_jnj_calendar as(
    select * from DEV_DNA_CORE.INDWKS_INTEGRATION.WKS_JNJ_CALENDAR
),
int_fact AS
(
  SELECT account_name,
         mth_mm,
         factor,
         rnk
  FROM (SELECT account_name,
               mth_mm,
               factor,
               ROW_NUMBER() OVER (PARTITION BY account_name ORDER BY mth_mm DESC) AS rnk
        FROM (SELECT SPLIT_PART(parameter_name,'-',1) AS account_name,
                     SPLIT_PART(parameter_name,'-',2) AS mth_mm,
                     parameter_value AS factor
              FROM itg_query_parameters
              WHERE country_code = 'IN'
                AND parameter_type = 'IN_MT_PERIODIC_SALES_FACTOR'
              GROUP BY 1,
                       2,
                       3)) innq
  --WHERE innq.rnk <=3
),
jnj_cal_months AS(
    SELECT DISTINCT mth_mm
    FROM edw_retailer_calendar_dim
    WHERE fisc_yr >= EXTRACT(YEAR FROM current_timestamp()::timestamp_ntz(9)) -3
    AND day <= TO_CHAR(current_timestamp()::timestamp_ntz(9),'YYYYMMDD')
),
transformed as(
SELECT mth_mm_cal,
       account_name,
       mth_mm_fi,
       factor,
       rnk
FROM (SELECT cal.mth_mm AS mth_mm_cal,
             fi.account_name,
             fi.mth_mm AS mth_mm_fi,
             fi.factor,
             fi.rnk
      FROM wks_jnj_calendar cal
        CROSS JOIN int_fact fi)
)
select * from transformed