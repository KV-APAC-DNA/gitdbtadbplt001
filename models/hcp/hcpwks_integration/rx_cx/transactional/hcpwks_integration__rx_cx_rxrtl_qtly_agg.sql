with 
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
itg_hcp360_in_ventasys_rxrtl as 
(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_rxrtl
), --hcp
rx_cx_presc_rtl_hist as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_presc_rtl_hist') }}
),
temp1 as 
(
    SELECT 
       v_custid_rtl::varchar(50) as v_custid_rtl,
       rx_product,
       SUM(rx_units) AS rx_units,
       cal.qtr AS quarter,
       cal.mth_yyyymm AS month,
       cal.fisc_yr AS year
FROM itg_hcp360_in_ventasys_rxrtl rxrtl
INNER JOIN edw_retailer_calendar_dim cal 
        ON REPLACE(rxrtl.dcr_dt,'-','') = cal.day
WHERE  (UPPER(rx_product) LIKE 'ORSL (CORE)%' OR UPPER(rx_product) LIKE 'ORSL (PLUS)%' OR UPPER(rx_product) LIKE 'ORSL (REHYDRATE)%')
AND    cal.fisc_yr||cal.mth_yyyymm NOT IN (20223,20224)  
AND    cal.fisc_yr > EXTRACT(YEAR FROM current_timestamp()) - (SELECT CAST(PARAMETER_VALUE AS INTEGER) AS PARAMETER_VALUE
                                                   FROM   itg_query_parameters
                                                   WHERE  UPPER(COUNTRY_CODE) = 'IN'
                                                     AND  UPPER(PARAMETER_TYPE) = 'YEAR_RANGE'
                                                     AND  UPPER(PARAMETER_NAME) = 'IN_RX_CX_DATA_RETENSION_PERIOD')
GROUP BY v_custid_rtl::varchar(50),
         rx_product,
         quarter,
         month,
         year
),
temp2 as 
(
    select
    v_custid_rtl::varchar(50) as v_custid_rtl,
    product::varchar(200) as rx_product,
    rx_units::number(38,2) as rx_units,
    qtr::number(18,0) as quarter,
    month::number(18,0) as month,
    fisc_yr::number(18,0) as year
from  rx_cx_presc_rtl_hist
),
final as 
(
    select * from temp1
    union all
    select * from temp2
)
select * from final