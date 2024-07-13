with 
wks_geotag_salescube_base as 
(
    select * from {{ ref('indwks_integration__wks_geotag_salescube_base') }}
),
wks_geotag_udc_program as 
(
    select * from {{ ref('indwks_integration__wks_geotag_udc_program') }}
),
itg_program_store_target as 
(
    select * from {{ source('inditg_integration', 'itg_program_store_target') }}
), 
edw_retailer_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
itg_mds_in_geo_tracker_coordinates as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_geo_tracker_coordinates') }}
),
pst_mth as 
(
        SELECT 
                  customer_code,
                  retailer_code,
                  year_month,
                  TO_CHAR(year_month,'YYYYMM') AS mth_mm,
                  SUM(value) AS month_target
           FROM itg_program_store_target
           GROUP BY customer_code,
                    retailer_code,
                    year_month,
                    TO_CHAR(year_month,'YYYYMM')),
pst_qtr as 
(
        SELECT 
            customer_code,
            COALESCE(program_name,'NA') AS program_name,
            CASE WHEN program_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS loyalty_program_flag,
            retailer_code,
            TO_CHAR(year_month,'YYYYQ') AS mth_qtr,
            SUM(value) AS qtr_target 
        FROM itg_program_store_target
        GROUP BY customer_code,
            program_name,
            retailer_code,
            TO_CHAR(year_month,'YYYYQ')
),
qtr_sales as 
(SELECT fisc_yr,
                  qtr,
                  customer_code,
                  retailer_code,
                  rtruniquecode,
                  channel_name, 
                  SUM(mthly_achievement_nr) as qrtrly_achievement_nr
           FROM wks_geotag_salescube_base
           GROUP BY 1,2,3,4,5,6
),
rd as 
(
            SELECT customer_code,
                  retailer_code,
				  CASE
					WHEN TRIM(rtrlatitude) like '^[0-9]+(\.[0-9]+)?$' THEN rtrlatitude
				    ELSE NULL
				  END AS rtrlatitude,
				  CASE
					WHEN TRIM(rtrlongitude) like '^[0-9]+(\.[0-9]+)?$' THEN rtrlongitude
				    ELSE NULL
				  END AS rtrlongitude,
                  end_date,
                  ROW_NUMBER() OVER (PARTITION BY customer_code,retailer_code ORDER BY end_date DESC) AS row_num
           FROM edw_retailer_dim
           GROUP BY 1,
                    2,
                    3,
                    4,
                    5
),
final as 
(
    SELECT sales.fisc_yr,
       sales.qtr,
       sales.mth_mm,
       sales.customer_code,
       sales.retailer_code,
       sales.retailer_name,
       sales.rtruniquecode,
       sales.region_name,
       sales.zone_name,
       sales.territory_name,
       sales.channel_name,
       sales.retailer_channel_3,
       sales.latest_customer_code,
       sales.latest_customer_name,
       sales.latest_region_name,
       sales.latest_zone_name,
       sales.latest_territory_name,
       sales.latest_salesman_code,
       sales.latest_salesman_name,
       sales.latest_uniquesalescode,
       sales.nielsen_popstrata,
       sales.nielsen_statename,
       udc.columnname AS loyalty_program_name,
       pst_mth.month_target,
       pst_qtr.qtr_target,
       CASE WHEN sales.channel_name IN ('Institutional Sale','CSD')  THEN 0 ELSE sales.mthly_achievement_nr END AS month_actuals,
       CASE WHEN sales.channel_name IN ('Institutional Sale','CSD')  THEN 0 ELSE qtr_sales.qrtrly_achievement_nr END AS qtr_actuals,
       NULL::text AS os_flag,
       NULL::integer AS msl_recom,
       NULL::integer AS msl_lines_sold_p3m,
       rd.rtrlatitude,
       rd.rtrlongitude,
       rgc.lat_zone,
       rgc.long_zone,
       rgc.lat_territory,
       rgc.long_territory,
       rgc.lat_customer,
       rgc.long_customer
FROM  wks_geotag_salescube_base sales
LEFT JOIN wks_geotag_udc_program udc
       ON  sales.retailer_code::TEXT = udc.retailer_code::TEXT
       AND sales.customer_code::TEXT = udc.distcode::TEXT
       AND LTRIM("left" (sales.mth_mm::CHARACTER VARYING::TEXT,4),'0'::CHARACTER VARYING::TEXT) = udc."year"::CHARACTER VARYING::TEXT 
       AND LTRIM("right" (sales.mth_mm::CHARACTER VARYING::TEXT,2),'0'::CHARACTER VARYING::TEXT) = udc."month"::CHARACTER VARYING::TEXT 
       AND sales.qtr = udc.quarter
LEFT JOIN  pst_mth
       ON sales.retailer_code = pst_mth.retailer_code
      AND sales.customer_code = pst_mth.customer_code
      AND sales.mth_mm = pst_mth.mth_mm 
LEFT JOIN  pst_qtr
       ON sales.retailer_code = pst_qtr.retailer_code
      AND sales.customer_code = pst_qtr.customer_code
      AND (sales.fisc_yr)::varchar(20) = LEFT(pst_qtr.mth_qtr,4)
      AND (sales.qtr)::varchar(20) = RIGHT(pst_qtr.mth_qtr,1)
LEFT JOIN  qtr_sales
       ON sales.rtruniquecode = qtr_sales.rtruniquecode
      AND sales.customer_code = qtr_sales.customer_code
      AND sales.retailer_code = qtr_sales.retailer_code
      AND sales.channel_name = qtr_sales.channel_name
      AND sales.qtr = qtr_sales.qtr
      AND sales.fisc_yr = qtr_sales.fisc_yr
      LEFT JOIN rd
         ON sales.customer_code = rd.customer_code
        AND sales.retailer_code = rd.retailer_code
        AND rd.row_num = 1
LEFT JOIN itg_mds_in_geo_tracker_coordinates rgc
       ON sales.zone_name = rgc.zone_name
      AND sales.latest_territory_name = rgc.territory_name
      AND sales.latest_customer_code = rgc.customer_code
)
select * from final