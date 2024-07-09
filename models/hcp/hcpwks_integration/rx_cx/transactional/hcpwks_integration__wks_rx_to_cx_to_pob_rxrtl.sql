with 
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_hcp360_in_ventasys_rxrtl as 
(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_rxrtl
), --hcp
itg_query_parameters as
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
final as 
(
    SELECT rxrtl.v_custid_rtl,
       CASE WHEN pmap.prod_vent_old IS NOT NULL AND pmap.prod_vent_old <> pmap.prod_vent THEN pmap.prod_vent
            ELSE rxrtl.rx_product END AS rx_product,
       SUM(rxrtl.rx_units) AS rx_units,
       rxrtl.quarter,
       rxrtl.MONTH,
       rxrtl.YEAR
FROM (SELECT v_custid_rtl,
             rx_product,
             SUM(rx_units) AS rx_units,
             cal.qtr AS quarter,
             cal.mth_yyyymm AS MONTH,
             cal.fisc_yr AS YEAR
      FROM itg_hcp360_in_ventasys_rxrtl rxrtl
        INNER JOIN edw_retailer_calendar_dim cal ON REPLACE (rxrtl.dcr_dt,'-','') = cal.day
      WHERE UPPER(rx_product) LIKE 'ORSL%'
      AND   cal.fisc_yr >= EXTRACT(YEAR FROM current_timestamp()) - 2
      GROUP BY v_custid_rtl,
               rx_product,
               quarter,
               MONTH,
               YEAR) rxrtl
LEFT JOIN (SELECT SPLIT_PART(parameter_name,'-',1) AS prod_vent,
                  SPLIT_PART(parameter_name,'-',2) AS prod_vent_old
           FROM itg_query_parameters
           WHERE parameter_type = 'Rx_to_Cx_to_Pob_Product_Mapping'
             AND SPLIT_PART(parameter_name,'-',3) = 'Y'
           GROUP BY 1,2) pmap
       ON UPPER(rxrtl.rx_product) = UPPER(pmap.prod_vent_old)
GROUP BY 1,2,4,5,6
)
select * from final