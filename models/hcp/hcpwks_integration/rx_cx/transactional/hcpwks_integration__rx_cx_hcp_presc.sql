with 
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
edw_hcp360_in_ventasys_prescription_fact as 
(
    select * from snapindedw_integration.edw_hcp360_in_ventasys_prescription_fact
), --hcp
final as 
(
    SELECT fisc_yr,
       qtr,
       month,
       hcp_id,
       product,
       SUM(no_of_prescription_units) hcp_presc
FROM (SELECT hcp.hcp_id,
             hcp.prescription_date,
             hcp.product,
             hcp.no_of_prescription_units,
             cal.fisc_yr,
             cal.qtr,
             cal.mth_yyyymm as month
      FROM edw_hcp360_in_ventasys_prescription_fact hcp
        INNER JOIN edw_retailer_calendar_dim cal 
                ON REPLACE (hcp.prescription_date,'-','') = cal.day
      WHERE (UPPER(hcp.product) LIKE 'ORSL (CORE)%' OR UPPER(hcp.product) LIKE 'ORSL (PLUS)%' OR UPPER(hcp.product) LIKE 'ORSL (REHYDRATE)%')
        AND cal.fisc_yr > 2020                       
        AND cal.fisc_yr||cal.mth_yyyymm <= 20224      
        AND 'Y' = (SELECT parameter_value
                   FROM   itg_query_parameters
                   WHERE  UPPER(country_code) = 'IN'
                   AND    UPPER(parameter_type) = 'HISTORY_LOAD_FLAG'
                   AND    UPPER(parameter_name) = 'IN_RX_CX_HISTORY_LOAD_FLAG')
      )
GROUP BY 1,
         2,
         3,
         4,
         5
)
select * from final