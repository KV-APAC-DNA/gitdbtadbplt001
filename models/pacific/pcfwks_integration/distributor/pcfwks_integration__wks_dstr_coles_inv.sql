with sdl_dstr_coles_inv as (
    select * from {{ source('pcfsdl_raw', 'sdl_dstr_coles_inv') }}
),
edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_base_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_code_descriptions as
(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
itg_parameter_reg_inventory as (
    select * from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
), 
inv as(
	SELECT
		TO_DATE(TRIM(inv.inv_date), 'YYYY-MM-DD') AS inv_date,
		inv.order_item AS article_code,
		inv.order_item_desc AS article_desc,
		CAST(inv.closing_soh_qty_unit AS DECIMAL(16, 4)) AS soh_qty,
		CAST(closing_soh_nic AS DECIMAL(16, 4)) AS soh_price
	FROM sdl_dstr_coles_inv AS inv
),
cust as (
	select distinct
		ecsd.prnt_cust_key as sap_prnt_cust_key,
		cddes_pck.code_desc as sap_prnt_cust_desc
	from edw_customer_sales_dim as ecsd, edw_customer_base_dim as ecbd, edw_code_descriptions as cddes_pck
	where
	ecsd.cust_num = ecbd.cust_num
	and ecsd.sls_org in ('3300', '330b', '330h', '3410', '341b')
	and cddes_pck.code_type(+) = 'Parent Customer Key'
	and cddes_pck.code(+) = ecsd.prnt_cust_key
),
transformed as(
	SELECT
		SAP_PRNT_CUST_KEY,
		UPPER(SAP_PRNT_CUST_DESC) AS SAP_PRNT_CUST_DESC,
		inv_date,
		article_code,
		article_desc,
		soh_qty,
		soh_price
	FROM inv, cust
	WHERE
	UPPER(TRIM(SAP_PRNT_CUST_DESC)) = (
	SELECT
	  TRIM(parameter_value)	
	FROM itg_parameter_reg_inventory
	WHERE
	  parameter_name = 'parent_desc_Grocery'
	)
)
select * from transformed