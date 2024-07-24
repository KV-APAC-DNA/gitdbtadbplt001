with 
edw_rpt_sales_details as 
(
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}
),
wks_rx_to_cx_to_pob_base_rtl as 
(
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_base_rtl') }}
),
final as 
(
    SELECT *
    FROM (SELECT rtruniquecode,
             latest_customer_code,
             retailer_name,
             customer_name,
             region_name,
             zone_name,
             territory_name,
             channel_name,
             class_desc,
             retailer_category_name,
             retailer_channel_1,
            retailer_channel_2,
            retailer_channel_3,
             row_number() OVER (PARTITION BY rtruniquecode ORDER BY invoice_date DESC) AS rn
      FROM edw_rpt_sales_details
      WHERE rtruniquecode IN (SELECT DISTINCT urc::TEXT
                              FROM wks_rx_to_cx_to_pob_base_rtl))
WHERE rn = 1
)
select * from final