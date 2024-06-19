with 
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
edw_rpt_sales_details as 
(
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}
),
trans as 
(
    SELECT zone_name,
       mth_mm,
       fisc_yr,
       month
FROM edw_rpt_sales_details,
     (SELECT EXTRACT(YEAR FROM (CURRENT_DATE-INTERVAL '25 months')) AS strt_year,
             EXTRACT(MONTH FROM (CURRENT_DATE-INTERVAL '25 months')) AS start_mnth,
             EXTRACT(YEAR FROM (CURRENT_DATE-INTERVAL '1 months')) AS end_year,
             EXTRACT(MONTH FROM (CURRENT_DATE-INTERVAL '1 months')) AS end_mnth) AS inn
WHERE zone_name IN (SELECT PARAMETER_VALUE
                    FROM   itg_query_parameters
                    WHERE  UPPER(COUNTRY_CODE) = 'IN'
                    AND    UPPER(PARAMETER_TYPE) = 'SSS_ZONAL_PARM'
                    AND    UPPER(PARAMETER_NAME) = 'SSS_ZONAL_ZONE_LIST')
AND   mth_mm > CASE WHEN start_mnth < 10 THEN strt_year||0||start_mnth ELSE strt_year||start_mnth END
AND   mth_mm <= CASE WHEN end_mnth < 10 THEN end_year||0||end_mnth ELSE end_year||end_mnth END
GROUP BY zone_name,
         mth_mm,
         fisc_yr,
         month
ORDER BY mth_mm DESC,
         zone_name
),
final as 
(
    select
    zone_name::varchar(50) as zone_name,
	mth_mm::number(18,0) as mth_mm,
	fisc_yr::number(18,0) as fisc_yr,
	month::varchar(3) as month
    from trans
)
select * from final