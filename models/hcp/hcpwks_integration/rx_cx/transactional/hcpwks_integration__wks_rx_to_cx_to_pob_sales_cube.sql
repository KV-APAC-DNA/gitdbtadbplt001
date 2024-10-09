with 
edw_rpt_sales_details as 
(
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}
),
wks_rx_to_cx_to_pob_base_rtl as 
(
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_base_rtl') }}
),
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
final as 
(
    SELECT *
    FROM (SELECT rtruniquecode,
             ersd.customer_code,
             latest_customer_code,
             latest_customer_name,
             latest_salesman_code,
             latest_salesman_name,
             retailer_name,
             ersd.customer_name,
             ersd.region_name,
             ersd.zone_name,
             ersd.territory_name,
             ecd.region_name as latest_region,
             ecd.zone_name as latest_zone,
             ecd.territory_name as latest_territory,
             channel_name,
             class_desc,
             retailer_category_name,
             retailer_channel_1,
            retailer_channel_2,
            retailer_channel_3,
             row_number() OVER (PARTITION BY rtruniquecode ORDER BY invoice_date DESC) AS rn
      FROM edw_rpt_sales_details ersd
      LEFT JOIN edw_customer_dim ecd ON
            ecd.customer_code=ersd.latest_customer_code
      WHERE rtruniquecode IN (SELECT DISTINCT urc::TEXT
                              FROM wks_rx_to_cx_to_pob_base_rtl))
WHERE rn = 1 limit 10
)
select * from final