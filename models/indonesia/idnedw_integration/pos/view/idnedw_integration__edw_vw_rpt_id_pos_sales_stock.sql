with 
edw_vw_id_pos_stock as 
(
	select * from {{ ref('idnedw_integration__edw_vw_id_pos_stock') }}
),
edw_vw_id_pos_sellout as 
(
	select * from {{ ref('idnedw_integration__edw_vw_id_pos_sellout') }}
),
vw_edw_reg_exch_rate as
(
    select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),

ex_rt as 
(
	SELECT *
		FROM vw_edw_reg_exch_rate
		WHERE cntry_key = 'ID'
		AND   TO_CCY = 'USD'
		AND   JJ_MNTH_ID = (SELECT MAX(JJ_MNTH_ID) FROM vw_edw_reg_exch_rate)
),
final as 
(
SELECT edw_vw_id_pos_sellout.sap_cntry_cd
	,edw_vw_id_pos_sellout.sap_cntry_nm
	,edw_vw_id_pos_sellout.dataset
	,edw_vw_id_pos_sellout.dstrbtr_grp_cd
	,edw_vw_id_pos_sellout."year"
	,edw_vw_id_pos_sellout.yearmonth
	,edw_vw_id_pos_sellout.customer_brnch_code
	,edw_vw_id_pos_sellout.customer_brnch_name
	,edw_vw_id_pos_sellout.customer_store_code
	,edw_vw_id_pos_sellout.customer_store_name
	,edw_vw_id_pos_sellout.customer_franchise
	,edw_vw_id_pos_sellout.customer_brand
	,edw_vw_id_pos_sellout.customer_product_code
	,edw_vw_id_pos_sellout.customer_product_desc
	,edw_vw_id_pos_sellout.jj_sap_prod_id
	,edw_vw_id_pos_sellout.brand
	,edw_vw_id_pos_sellout.brand2
	,edw_vw_id_pos_sellout.sku_sales_cube
	,edw_vw_id_pos_sellout.customer_product_range
	,edw_vw_id_pos_sellout.customer_product_group
	,edw_vw_id_pos_sellout.customer_store_class
	,edw_vw_id_pos_sellout.customer_store_channel
	,edw_vw_id_pos_sellout.sales_qty
	,edw_vw_id_pos_sellout.sales_value
	,edw_vw_id_pos_sellout.service_level
	,edw_vw_id_pos_sellout.sales_order
	,edw_vw_id_pos_sellout.share
	,NULL AS store_stock_qty
	,NULL AS store_stock_value
	,NULL AS branch_stock_qty
	,NULL AS branch_stock_value
	,NULL AS stock_uom
	,NULL AS stock_days
	,edw_vw_id_pos_sellout.crtd_dttm,
	(ex_rt.EXCH_RATE/(ex_rt.from_ratio*ex_rt.to_ratio))::NUMERIC(15,5) AS usd_conversion_rate
FROM edw_vw_id_pos_sellout
LEFT JOIN ex_rt 
ON   UPPER(edw_vw_id_pos_sellout.sap_cntry_nm) = UPPER(ex_rt.CNTRY_NM) where ex_rt.from_ccy = 'IDR'

UNION ALL

SELECT edw_vw_id_pos_stock.sap_cntry_cd
	,edw_vw_id_pos_stock.sap_cntry_nm
	,edw_vw_id_pos_stock.dataset
	,edw_vw_id_pos_stock.dstrbtr_grp_cd
	,edw_vw_id_pos_stock."year"
	,edw_vw_id_pos_stock.yearmonth
	,edw_vw_id_pos_stock.customer_brnch_code
	,edw_vw_id_pos_stock.customer_brnch_name
	,edw_vw_id_pos_stock.customer_store_code
	,edw_vw_id_pos_stock.customer_store_name
	,edw_vw_id_pos_stock.customer_franchise
	,edw_vw_id_pos_stock.customer_brand
	,edw_vw_id_pos_stock.customer_product_code
	,edw_vw_id_pos_stock.customer_product_desc
	,edw_vw_id_pos_stock.jj_sap_prod_id
	,edw_vw_id_pos_stock.brand
	,edw_vw_id_pos_stock.brand2
	,edw_vw_id_pos_stock.sku_sales_cube
	,edw_vw_id_pos_stock.customer_product_range
	,edw_vw_id_pos_stock.customer_product_group
	,edw_vw_id_pos_stock.customer_store_class
	,edw_vw_id_pos_stock.customer_store_channel
	,NULL AS sales_qty
	,NULL AS sales_value
	,NULL AS service_level
	,NULL AS sales_order
	,NULL AS "share"
	,edw_vw_id_pos_stock.store_stock_qty
	,edw_vw_id_pos_stock.store_stock_value
	,edw_vw_id_pos_stock.branch_stock_qty
	,edw_vw_id_pos_stock.branch_stock_value
	,edw_vw_id_pos_stock.stock_uom
	,edw_vw_id_pos_stock.stock_days
	,edw_vw_id_pos_stock.crtd_dttm,
	(ex_rt.EXCH_RATE/(ex_rt.from_ratio*ex_rt.to_ratio))::NUMERIC(15,5) AS usd_conversion_rate
FROM edw_vw_id_pos_stock
LEFT JOIN ex_rt 
ON   UPPER(edw_vw_id_pos_stock.sap_cntry_nm) = UPPER(ex_rt.CNTRY_NM) where ex_rt.from_ccy = 'IDR'
)
select * from final