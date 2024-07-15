with 
v_ecomm_sales_inv as 
(
    select * from {{ ref('indedw_integration__v_ecomm_sales_inv') }}
),
itg_mds_in_key_accounts_mapping as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_key_accounts_mapping') }}
),
final as 
(
    SELECT 
     ecomm.data_source
	,ecomm.fisc_yr
	,ecomm.mth_mm
	,ecomm.week
	,ecomm.qtr
	,upper((ecomm.month)::TEXT) AS month
	,upper((ecomm.region_name)::TEXT) AS region_name
	,upper((ecomm.zone_name)::TEXT) AS zone_name
	,upper((ecomm.territory_name)::TEXT) AS territory_name
	,ecomm.customer_code
	,COALESCE(upper((ecomm.customer_name)::TEXT), upper((acc.name)::TEXT)) AS customer_name
	,upper((ecomm.udc_keyaccountname)::TEXT) AS udc_keyaccountname
	,upper((ecomm.franchise_name)::TEXT) AS franchise_name
	,upper((ecomm.brand_name)::TEXT) AS brand_name
	,upper((ecomm.variant_name)::TEXT) AS variant_name
	,upper((ecomm.product_category_name)::TEXT) AS product_category_name
	,upper((ecomm.mothersku_name)::TEXT) AS mothersku_name
	,upper((ecomm.product_name)::TEXT) AS product_name
	,ecomm.product_code
	,upper((acc.channel_name_code)::TEXT) AS channel_name_inv
	,upper((ecomm.channel_name)::TEXT) AS channel_name_sales
	,ecomm.invoice_quantity
	,ecomm.invoice_value
	,ecomm.bill_type
	,upper((acc.account_name_code)::TEXT) AS account_name
FROM (
	v_ecomm_sales_inv ecomm LEFT JOIN itg_mds_in_key_accounts_mapping acc ON (((ecomm.customer_code)::TEXT = (acc.code)::TEXT))
	)
)
select * from final