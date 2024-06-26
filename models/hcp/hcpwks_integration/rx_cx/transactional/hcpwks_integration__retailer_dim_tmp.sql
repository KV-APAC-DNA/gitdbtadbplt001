with 
edw_retailer_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
final as 
(
    SELECT urc_name,
       region_sales,
       territory_sales,
       zone_sales,
       urc,
       distributor_code,
       distributor_name
FROM (SELECT retailer_name AS urc_name,
             region_name AS region_sales,
             territory_name AS territory_sales,
             zone_name AS zone_sales,
             rtruniquecode AS urc,
             customer_code AS distributor_code,
             customer_name AS distributor_name,
             ROW_NUMBER() OVER (PARTITION BY rtruniquecode ORDER BY start_date DESC) AS rnk
      FROM edw_retailer_dim
      WHERE actv_flg = 'Y') ret
WHERE ret.rnk = 1
)
select * from final