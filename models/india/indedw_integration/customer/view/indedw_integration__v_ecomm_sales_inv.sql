with 
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
v_rpt_sales_details as 
(
    select * from {{ ref('indedw_integration__v_rpt_sales_details') }}
),
edw_product_dim as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
itg_invoicing_calendar_2022 as 
(
    select * from {{ source('inditg_integration', 'itg_invoicing_calendar_2022') }}
),
final as 
(
    SELECT 'INVOICE_CUBE'::CHARACTER VARYING AS data_source
	,(
		"substring" (
			(cal.DATE)::TEXT
			,1
			,4
			)
		)::INTEGER AS fisc_yr
	,(
		"substring" (
			(cal.DATE)::TEXT
			,1
			,6
			)
		)::INTEGER AS mth_mm
	,(
		"substring" (
			(cal.week)::TEXT
			,1
			,1
			)
		)::INTEGER AS week
	,(cal.qtr)::INTEGER AS qtr
	,(
		"substring" (
			(cal.month)::TEXT
			,1
			,3
			)
		)::CHARACTER VARYING AS month
	,customer_dim.region_name
	,customer_dim.zone_name
	,customer_dim.territory_name
	,customer_dim.customer_code
	,customer_dim.customer_name
	,NULL AS udc_keyaccountname
	,product_dim.franchise_name
	,product_dim.brand_name
	,product_dim.variant_name
	,product_dim.product_category_name
	,product_dim.mothersku_name
	,product_dim.product_name
	,product_dim.product_code
	,customer_dim.type_name AS channel_name
	,sum(bill.inv_qty) AS invoice_quantity
	,sum(CASE 
			WHEN ((bill.doc_currcy)::TEXT <> ('INR'::CHARACTER VARYING)::TEXT)
				THEN (bill.subtotal_4 * bill.exrate_acc)
			ELSE bill.subtotal_4
			END) AS invoice_value
	,bill.bill_type
FROM (
	(
		(
			itg_invoicing_calendar_2022 cal JOIN edw_billing_fact bill ON (
					(
						(cal.DATE)::TEXT = (
							(
								"substring" (
									((bill.created_on)::CHARACTER VARYING)::TEXT
									,1
									,4
									) || "substring" (
									((bill.created_on)::CHARACTER VARYING)::TEXT
									,6
									,2
									)
								) || "substring" (
								((bill.created_on)::CHARACTER VARYING)::TEXT
								,9
								,2
								)
							)
						)
					)
			) LEFT JOIN edw_customer_dim customer_dim ON (
				(
					"substring" (
						(bill.sold_to)::TEXT
						,5
						,6
						) = (customer_dim.customer_code)::TEXT
					)
				)
		) LEFT JOIN edw_product_dim product_dim ON ((ltrim((bill.material)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = (product_dim.product_code)::TEXT))
	)
 WHERE ((bill.bill_type)::TEXT IN ('S1'::text, 'S2'::text, 'ZC2D'::text, 'ZC3D'::text, 'ZC22'::text, 'ZF2D'::text, 'ZF2E'::text, 'ZG2D'::text, 'ZG3D'::text, 'ZG22'::text, 'ZL2D'::text, 'ZL3D'::text, 'ZL22'::text, 'ZRSM'::text, 'ZSMD'::text))
GROUP BY 1,
        (
		"substring" (
			(cal.DATE)::TEXT
			,1
			,4
			)
		)::INTEGER
	,(
		"substring" (
			(cal.DATE)::TEXT
			,1
			,6
			)
		)::INTEGER
	,(
		"substring" (
			(cal.week)::TEXT
			,1
			,1
			)
		)::INTEGER
	,(cal.qtr)::INTEGER
	,(
		"substring" (
			(cal.month)::TEXT
			,1
			,3
			)
		)::CHARACTER VARYING
	,customer_dim.region_name
	,customer_dim.zone_name
	,customer_dim.territory_name
	,customer_dim.customer_code
	,customer_dim.customer_name
	,product_dim.franchise_name
	,product_dim.brand_name
	,product_dim.variant_name
	,product_dim.product_category_name
	,product_dim.mothersku_name
	,product_dim.product_name
	,product_dim.product_code
	,customer_dim.type_name
	,bill.bill_type

UNION ALL

SELECT 'SALES_CUBE' AS data_source
	,edw_rpt_sales_details.fisc_yr
	,edw_rpt_sales_details.mth_mm
	,edw_rpt_sales_details.week
	,edw_rpt_sales_details.qtr
	,edw_rpt_sales_details.month
	,edw_rpt_sales_details.region_name
	,edw_rpt_sales_details.zone_name
	,edw_rpt_sales_details.territory_name
	,edw_rpt_sales_details.customer_code
	,edw_rpt_sales_details.customer_name
	,(edw_rpt_sales_details.udc_keyaccountname)::CHARACTER VARYING AS udc_keyaccountname
	,edw_rpt_sales_details.franchise_name
	,edw_rpt_sales_details.brand_name
	,edw_rpt_sales_details.variant_name
	,edw_rpt_sales_details.product_category_name
	,edw_rpt_sales_details.mothersku_name
	,edw_rpt_sales_details.product_name
	,edw_rpt_sales_details.product_code
	,edw_rpt_sales_details.channel_name
	,sum(edw_rpt_sales_details.quantity) AS invoice_quantity
	,sum(edw_rpt_sales_details.achievement_nr) AS invoice_value
	,NULL AS bill_type
FROM v_rpt_sales_details edw_rpt_sales_details
WHERE (
		(
			((edw_rpt_sales_details.channel_name)::TEXT = ('E-Commerce'::CHARACTER VARYING)::TEXT)
			OR ((edw_rpt_sales_details.channel_name)::TEXT = ('E-Retail'::CHARACTER VARYING)::TEXT)
			)
		OR ((edw_rpt_sales_details.channel_name)::TEXT = ('National Key Accounts'::CHARACTER VARYING)::TEXT)
		)
GROUP BY edw_rpt_sales_details.fisc_yr
	,edw_rpt_sales_details.mth_mm
	,edw_rpt_sales_details.week
	,edw_rpt_sales_details.qtr
	,edw_rpt_sales_details.month
	,edw_rpt_sales_details.region_name
	,edw_rpt_sales_details.zone_name
	,edw_rpt_sales_details.territory_name
	,edw_rpt_sales_details.customer_code
	,edw_rpt_sales_details.customer_name
	,edw_rpt_sales_details.udc_keyaccountname
	,edw_rpt_sales_details.franchise_name
	,edw_rpt_sales_details.brand_name
	,edw_rpt_sales_details.variant_name
	,edw_rpt_sales_details.product_category_name
	,edw_rpt_sales_details.mothersku_name
	,edw_rpt_sales_details.product_name
	,edw_rpt_sales_details.product_code
	,edw_rpt_sales_details.channel_name
)
select * from final