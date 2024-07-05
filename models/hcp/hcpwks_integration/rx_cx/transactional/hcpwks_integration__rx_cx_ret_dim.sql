with 
rx_cx_achnr_qty as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_achnr_qty') }}
),
edw_retailer_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
final as 
(
    SELECT tmp.*, 
       ret.urc_name,
       ret.region_sales,
       ret.territory_sales,
       ret.zone_sales,
       ret.distributor_code,       
       ret.distributor_name      
FROM  rx_cx_achnr_qty tmp
LEFT JOIN (SELECT retailer_name AS urc_name,
                  region_name AS region_sales,
                  territory_name AS territory_sales,
                  zone_name AS zone_sales,
                  rtruniquecode AS urc,
                  customer_code AS distributor_code,
                  customer_name AS distributor_name,
                  ROW_NUMBER() OVER (PARTITION BY rtruniquecode ORDER BY start_date DESC) AS rnk
           FROM edw_retailer_dim
           WHERE actv_flg = 'Y') ret
        ON tmp.urc::text = ret.urc
        AND ret.rnk = 1
)
select * from final