with edw_rpt_rx_to_cx as(
    select * from {{ ref('hcpedw_integration__edw_rpt_rx_to_cx') }}
),
itg_mds_in_vent_prod_msku_mapping as(
    select * from {{ ref('hcpitg_integration_itg_mds_in_vent_prod_msku_mapping') }}
),
EDW_RETAILER_CALENDAR_DIM as(
    select * from {{ ref('hcpedw_integration_edw_retailer_calendar_dim') }}
),
itg_query_parameters as(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
final as(
SELECT inn.urc,
       inn.mother_sku_cd,
       inn.year,
       inn.quarter,
       inn.month,
       round((inn.quarterly_target_qty / mapp1.unique_mother_sku_cd_count),2) AS quarterly_soq,       
       round((inn.quarterly_target_qty / 3) / mapp1.unique_mother_sku_cd_count,2) AS monthly_soq    
FROM (SELECT rx_cx.urc,
             rx_cx.product_vent,
             mapp.mother_sku_cd,
             rx_cx.year,
             rx_cx.quarter,
             cal.mth_yyyymm AS MONTH,
             rx_cx.target_qty AS quarterly_target_qty
      FROM edw_rpt_rx_to_cx rx_cx
        LEFT JOIN itg_mds_in_vent_prod_msku_mapping mapp
               ON rx_cx.product_vent = mapp.product_vent_code
        LEFT JOIN (SELECT qtr,
                          mth_yyyymm
                   FROM edw_retailer_calendar_dim
                   GROUP BY 1,
                            2) cal 
               ON  TRIM (rx_cx.quarter,'Q')::INTEGER = cal.qtr
      WHERE rx_cx.year = (SELECT CAST(parameter_value AS INTEGER) AS parameter_value
                          FROM itg_query_parameters
                          WHERE UPPER(country_code) = 'IN'
                          AND   UPPER(parameter_type) = 'YEAR'
                          AND   UPPER(parameter_name) = 'IN_RX_CX_TARGET_GEN_YEAR')              
        AND TRIM(rx_cx.quarter,'Q')::INTEGER = (SELECT CAST(parameter_value AS INTEGER) AS parameter_value
                                                FROM itg_query_parameters
                                                WHERE UPPER(country_code) = 'IN'
                                                AND   UPPER(parameter_type) = 'QUARTER'
                                                AND   UPPER(parameter_name) = 'IN_RX_CX_TARGET_GEN_QTR') 
        AND rx_cx.target_qty IS NOT NULL
      ) inn
INNER JOIN (SELECT product_vent_code,
                   COUNT(DISTINCT mother_sku_cd) AS unique_mother_sku_cd_count
            FROM itg_mds_in_vent_prod_msku_mapping       
            GROUP BY 1) mapp1
        ON  inn.product_vent = mapp1.product_vent_code
)
select * from final