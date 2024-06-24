with 
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
itg_hcp360_in_ventasys_pob_data as 
(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_pob_data
), --hcp ref
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
final as 
(
    SELECT pobu.v_custid_rtl,
       CASE
         WHEN pmap.prod_vent_old IS NOT NULL AND pmap.prod_vent_old <> pmap.prod_vent THEN pmap.prod_vent
         ELSE pobu.pob_product
       END AS pob_product,
       pobu.year,
       pobu.quarter,
       pobu.month,
       pobu.week,
       pobu.dcr_dt,
       SUM(pobu.pob_units) AS pob_units
FROM (SELECT v_custid_rtl,
             pob_product,
             cal.fisc_yr AS YEAR,
             cal.qtr AS quarter,
             cal.mth_yyyymm AS MONTH,
             cal.week AS week,
             TO_CHAR(dcr_dt,'YYYYMMDD') AS dcr_dt,
             SUM(pob_units) AS pob_units
      FROM itg_hcp360_in_ventasys_pob_data pob
        INNER JOIN edw_retailer_calendar_dim cal ON TO_CHAR (dcr_dt,'YYYYMMDD') = cal.day::TEXT
      WHERE cal.fisc_yr >= EXTRACT(YEAR FROM current_timestamp()) - 2
      GROUP BY 1,
               2,
               3,
               4,
               5,
               6,
               7) pobu
  LEFT JOIN (SELECT SPLIT_PART(parameter_name,'-',1) AS prod_vent,
                    SPLIT_PART(parameter_name,'-',2) AS prod_vent_old
             FROM itg_query_parameters
             WHERE parameter_type = 'Rx_to_Cx_to_Pob_Product_Mapping'
             AND   SPLIT_PART(parameter_name,'-',3) = 'Y'
             GROUP BY 1,
                      2) pmap 
	     ON UPPER (pobu.pob_product) = UPPER (pmap.prod_vent_old)
GROUP BY 1,
         2,
		 3,
         4,
         5,
         6,
         7
)
select * from final