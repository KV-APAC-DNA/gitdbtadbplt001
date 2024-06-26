with sdl_tw_bu_forecast_sku as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_bu_forecast_sku') }}
),
edw_customer_attr_hier_dim as (
select * from {{ ref('aspedw_integration__edw_customer_attr_hier_dim') }}
),
edw_customer_attr_flat_dim as (
select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
cust_attr as(
	SELECT sold_to_party,
		max(strategy_customer_hierachy_name) as strategy_customer_hierachy_name
	FROM edw_customer_attr_hier_dim
	WHERE cntry = 'Taiwan'
	GROUP BY sold_to_party
),
cust_flat as(
    SELECT sold_to_party,
		max(sls_grp) sls_grp,
		max(channel) channel,
		max(sls_ofc) sls_ofc,
		max(sls_ofc_desc) sls_ofc_desc
	FROM edw_customer_attr_flat_dim
	WHERE cntry = 'Taiwan'
	GROUP BY sold_to_party
),
transformed as(
    SELECT bu_frcst_sku.bu_version,
        bu_frcst_sku.forecast_on_year,
        bu_frcst_sku.forecast_on_month,
        bu_frcst_sku.forecast_for_year,
        bu_frcst_sku.forecast_for_mnth,
        coalesce(cust_flat.sls_grp, '#N/A') AS sls_grp,
        coalesce(cust_flat.channel, '#N/A') AS channel,
        coalesce(cust_flat.sls_ofc, '#N/A') AS sls_ofc,
        coalesce(cust_flat.sls_ofc_desc, '#N/A') AS sls_ofc_desc,
        coalesce(cust_attr.strategy_customer_hierachy_name, '#N/A') AS strategy_customer_hierachy_name,
        bu_frcst_sku.sap_code,
        bu_frcst_sku.system_list_price,
        bu_frcst_sku.gross_invoice_price,
        bu_frcst_sku.gross_invoice_price_less_terms,
        bu_frcst_sku.rf_sell_out_qty,
        bu_frcst_sku.rf_sell_in_qty,
        bu_frcst_sku.price_off,
        bu_frcst_sku.pre_sales_before_returns,
        bu_frcst_sku.load_date
    FROM sdl_tw_bu_forecast_sku bu_frcst_sku
    LEFT JOIN cust_attr ON bu_frcst_sku.representative_cust_no = cust_attr.sold_to_party
    LEFT JOIN cust_flat ON bu_frcst_sku.representative_cust_no = cust_flat.sold_to_party
)
select * from transformed
