with 
itg_hcp360_in_ventasys_rtlmaster as 
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_rtlmaster') }}
),
itg_hcp360_in_ventasys_hcprtl as 
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcprtl') }}
),
itg_hcp360_in_ventasys_rxrtl as 
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_rxrtl') }}
),
edw_hcp360_in_ventasys_hcp_dim_latest as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_hcp_dim_latest') }}
),
edw_hcp360_in_ventasys_employee_dim as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_employee_dim') }}
),
edw_rpt_pob_cx_final as 
(
    select * from {{ ref('hcpedw_integration__edw_rpt_pob_cx_final') }}
),
edw_product_dim as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_ventasys_jnj_prod_mapping as 
(
    select * from {{ source('inditg_integration', 'itg_ventasys_jnj_prod_mapping') }}
),
edw_rpt_sales_details as 
(
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}
),
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_retailer_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
itg_in_rcustomerroute as 
(
    select * from {{ ref('inditg_integration__itg_in_rcustomerroute') }}
),
itg_in_rroute as 
(
    select * from {{ ref('inditg_integration__itg_in_rroute') }}
),
itg_in_rsalesmanroute as 
(
    select * from {{ ref('inditg_integration__itg_in_rsalesmanroute') }}
),
itg_in_rsalesman as 
(
    select * from {{ ref('inditg_integration__itg_in_rsalesman') }}
),
wks_rx_to_cx_to_pob_base_rtl as 
(
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_base_rtl') }}
),
wks_rx_to_cx_to_pob_salesman_master as 
(
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_salesman_master') }}
),
wks_rx_to_cx_to_pob_sales_cube as 
(
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_sales_cube') }}
),
wks_rx_to_cx_to_pob_rtl_dim as 
(
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_rtl_dim') }}
),
wks_rx_to_cx_to_pob_rxrtl as 
(
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_rxrtl') }}
),
itg_query_parameters as
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
final as
(
    SELECT cal."year",
       cal."month",
       rtl.urc,
       rtl.is_active AS "URC Active Flag Ventasys",
       mapp.prod_vent AS ventasys_product,
       prod.franchise_name,
       prod.brand_name,
       sales.quantity AS sales_qty,
       sales.achievement_nr AS sales_ach_nr,
       pob.pob_units,
       hcprtl.v_custid_dr AS hcp_id,
       rxrtl.rx_units AS rx_factorized,
       rxrtl.rx_units * 4 AS rx_units,
       NVL(rd.retailer_name,sd.retailer_name) AS urc_name,
       NVL(rd.customer_code,sd.latest_customer_code) AS distributor_code,
       NVL(rd.customer_name,sd.customer_name) AS distributor_name,
       NVL(rd.region_name,sd.region_name) AS region_name,
       NVL(rd.zone_name,sd.zone_name) AS zone_name,
       NVL(rd.territory_name,sd.territory_name) AS territory_name,
       NVL(rd.channel_name,sd.channel_name) AS channel_name,
       NVL(rd.class_desc,sd.class_desc) AS class_desc,
       NVL(rd.retailer_category_name,sd.retailer_category_name) AS retailer_category_name,
       NVL(rd.retailer_channel_level1,sd.retailer_channel_1) AS retailer_channel_1,
       NVL(rd.retailer_channel_level2,sd.retailer_channel_2) AS retailer_channel_2,
       NVL(rd.retailer_channel_level3,sd.retailer_channel_3) AS retailer_channel_3,
       NVL(rd.urc_active_flag,'N') AS "URC Active Flag",
       sm.smcode AS salesman_code_sales,
       sm.smname AS salesman_name_sales,
       hcpdim.customer_name AS hcp_name,
       hcpdim.name AS emp_name,
       hcpdim.employee_id AS emp_id,
       hcpdim.region AS region_vent,
       hcpdim.territory AS territory_vent,
       hcpdim.zone AS zone_vent    
FROM (SELECT * 
      FROM wks_rx_to_cx_to_pob_base_rtl) rtl
  CROSS JOIN (SELECT LEFT(mth_mm,4) AS "year", RIGHT(mth_mm,2) AS "month"
              FROM edw_retailer_calendar_dim
              WHERE fisc_yr >= EXTRACT(YEAR FROM current_timestamp()) - 2
                AND day <= TO_CHAR(current_timestamp(),'YYYYMMDD')
              GROUP BY 1,2) cal
  CROSS JOIN (SELECT prod_vent 
              FROM (SELECT  SPLIT_PART(parameter_name,'-',1) AS prod_vent
                    FROM itg_query_parameters
                    WHERE parameter_type = 'Rx_to_Cx_to_Pob_Product_Mapping'
                    GROUP BY 1)
              WHERE UPPER(prod_vent) LIKE 'ORSL%'
              GROUP BY 1) mapp
  LEFT JOIN (SELECT franchise_name, brand_name, pmap.prod_vent
             FROM edw_product_dim pd
             INNER JOIN (SELECT SPLIT_PART(parameter_name,'-',1) AS prod_vent,
                                SPLIT_PART(parameter_value,'-',1) AS product_code
                         FROM itg_query_parameters
                         WHERE parameter_type = 'Rx_to_Cx_to_Pob_Product_Mapping'
                         GROUP BY 1,2) pmap
                     ON pd.product_code = pmap.product_code
             GROUP BY 1,2,3) prod
         ON UPPER(mapp.prod_vent) = UPPER(prod.prod_vent)
  LEFT JOIN (SELECT LEFT(sd.mth_mm,4) AS year,
                    RIGHT(sd.mth_mm,2) AS month,
                    sd.rtruniquecode,
                    pmap.prod_vent,
                    SUM(quantity) AS quantity,
                    SUM(achievement_nr) AS achievement_nr
             FROM edw_rpt_sales_details sd
             INNER JOIN (SELECT  SPLIT_PART(parameter_name,'-',1) AS prod_vent,
                                 SPLIT_PART(parameter_value,'-',1) AS product_code
                         FROM itg_query_parameters
                         WHERE parameter_type = 'Rx_to_Cx_to_Pob_Product_Mapping'
                         GROUP BY 1,2) pmap
                     ON sd.product_code = pmap.product_code
                    AND UPPER(prod_vent) LIKE 'ORSL%'
             WHERE sd.fisc_yr >= EXTRACT(YEAR FROM current_timestamp()) - 2
             GROUP BY 1,2,3,4) sales         
         ON cal."year" = sales.year
        AND cal."month" = sales.month
        AND rtl.urc::text = sales.rtruniquecode
        AND mapp.prod_vent = sales.prod_vent
  LEFT JOIN (SELECT YEAR,
                    MONTH,
                    urc,
                    ventasys_product,
                    SUM(sales_qty) AS sales_qty,
                    SUM(sales_ach_nr) AS sales_ach_nr,
                    SUM(pob_units) AS pob_units
             FROM edw_rpt_pob_cx_final
             WHERE year >= EXTRACT(YEAR FROM current_timestamp()) - 2
             AND UPPER(ventasys_product) LIKE 'ORSL%'
             GROUP BY 1,
                      2,
                      3,
                      4) pob
          ON cal."year" = pob.year
         AND LTRIM(cal."month",0) = pob.month
         AND rtl.urc = pob.urc
         AND mapp.prod_vent = pob.ventasys_product 
  LEFT JOIN (SELECT v_custid_rtl, v_custid_dr, ROW_NUMBER() OVER (PARTITION BY v_custid_rtl ORDER BY filename DESC) AS rnk
             FROM itg_hcp360_in_ventasys_hcprtl) hcprtl
         ON rtl.v_custid_rtl = hcprtl.v_custid_rtl
        AND hcprtl.rnk = 1
  LEFT JOIN (SELECT * FROM wks_rx_to_cx_to_pob_rxrtl) rxrtl
         ON cal."year" = rxrtl.year
        AND LTRIM(cal."month",0) = LTRIM(rxrtl.month,0)
        AND rtl.v_custid_rtl = rxrtl.v_custid_rtl
        AND mapp.prod_vent = rxrtl.rx_product
  LEFT JOIN (SELECT *
             FROM wks_rx_to_cx_to_pob_rtl_dim) rd
          ON rd.rtruniquecode = rtl.urc::text
  LEFT JOIN (SELECT * 
             FROM wks_rx_to_cx_to_pob_sales_cube) sd
         ON rtl.urc::text = sd.rtruniquecode
  LEFT JOIN (SELECT * 
             FROM wks_rx_to_cx_to_pob_salesman_master) sm
          ON NVL(rd.customer_code,sd.latest_customer_code) = sm.distcode
         AND rd.retailer_code = sm.rtrcode
  LEFT JOIN (SELECT hcp.hcp_id,
                  hcp.customer_name,
                  emp.name,
                  emp.employee_id,
                  emp.region,
                  emp.territory,
                  emp.zone
           FROM edw_hcp360_in_ventasys_hcp_dim_latest hcp
           LEFT JOIN (SELECT emp_terrid,name,employee_id,region,territory,zone, row_number() over(partition by emp_terrid order by join_date desc) AS rnk
                      FROM   edw_hcp360_in_ventasys_employee_dim
                      WHERE  is_active = 'Y' ) emp
                  ON hcp.territory_id = emp.emp_terrid
                 AND emp.rnk = 1 
           WHERE  hcp.valid_to > current_date) hcpdim
       ON hcprtl.v_custid_dr = hcpdim.hcp_id
)
select * from final