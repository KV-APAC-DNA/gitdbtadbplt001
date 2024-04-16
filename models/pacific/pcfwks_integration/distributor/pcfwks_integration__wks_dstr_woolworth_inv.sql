with sdl_dstr_woolworth_inv as (
    select * from {{ source('pcfsdl_raw', 'sdl_dstr_woolworth_inv') }}
),
itg_dstr_woolworth_sap_mapping as (
    select * from {{ ref('pcfitg_integration__itg_dstr_woolworth_sap_mapping') }}
),
edw_customer_sales_dim as (
    select * from dev_dna_core.snapaspedw_integration.edw_customer_sales_dim
),
edw_customer_base_dim as (
    select * from dev_dna_core.snapaspedw_integration.edw_customer_base_dim
),
edw_code_descriptions as (
    select * from dev_dna_core.snapaspedw_integration.edw_code_descriptions
),
itg_parameter_reg_inventory as (
    select * from DEV_DNA_CORE.ASPITG_INTEGRATION.ITG_PARAMETER_REG_INVENTORY
), 
inv as(
	select
    TO_DATE(inv.inv_date, 'YYYY-MM-DD') AS inv_date,
    inv.article_code,
    inv.articledesc as article_desc,
    cast(inv.soh_oms as decimal(10, 4)) * cast(inv.om as decimal(10, 4)) as soh_qty,
    cast(soh_price as decimal(16, 4)) as soh_price
  from sdl_dstr_woolworth_inv as inv
  left join (
    select distinct
      article_code,
      sap_code
    from itg_dstr_woolworth_sap_mapping
    where
      sap_code <> ''
  ) as map
    on ltrim(inv.article_code, 0) = ltrim(map.article_code, 0)
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
	select
        sap_prnt_cust_key,
        upper(sap_prnt_cust_desc) as sap_prnt_cust_desc,
        inv_date,
        article_code,
        article_desc,
        cast(soh_qty as decimal(16, 4)) as soh_qty,
        soh_price
	from inv, cust where 
	upper(trim(cust.sap_prnt_cust_desc)) = (
    select
      trim(parameter_value) as parameter_value
    from itg_parameter_reg_inventory
    where
      parameter_name = 'parent_desc_Grocery_WW'
  )
)
select * from transformed