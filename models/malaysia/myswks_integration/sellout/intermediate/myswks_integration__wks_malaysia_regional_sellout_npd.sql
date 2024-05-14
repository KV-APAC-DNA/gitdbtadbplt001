with WKS_MALAYSIA_REGIONAL_SELLOUT as (
select * from Snap
)
final as (	
SELECT *,
Min(cal_date) Over (Partition By sap_parent_customer_key) as Customer_Min_Date,
Min(cal_date) Over (Partition By country_name) as Market_Min_Date,
RANK() OVER (PARTITION BY sap_parent_customer_key,pka_product_key ORDER BY cal_date) AS rn_cus,
RANK() OVER (PARTITION BY country_name,pka_product_key ORDER BY cal_date) AS rn_mkt,
Min(cal_date) Over (Partition By sap_parent_customer_key,pka_product_key) as Customer_Product_Min_Date, 
Min(cal_date) Over (Partition By country_name,pka_product_key) as Market_Product_Min_Date
FROM WKS_MALAYSIA_REGIONAL_SELLOUT
)
select * from final