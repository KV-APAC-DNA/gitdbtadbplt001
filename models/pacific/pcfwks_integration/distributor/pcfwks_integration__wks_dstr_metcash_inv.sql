with sdl_ig_inventory_data as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_ig_inventory_data') }}
),
edw_customer_sales_dim as
(
     select * from snapaspedw_integration.edw_customer_sales_dim
),
edw_customer_base_dim as
(
     select * from snapaspedw_integration.edw_customer_base_dim
),
edw_code_descriptions as
(
     select * from snapaspedw_integration.edw_code_descriptions
),
itg_parameter_reg_inventory as
(
    select * from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
),
c as
(
    select 
        distinct ecsd.prnt_cust_key as sap_prnt_cust_key,
        cddes_pck.code_desc as sap_prnt_cust_desc
    from edw_customer_sales_dim ecsd,
        edw_customer_base_dim ecbd,
        edw_code_descriptions cddes_pck
    where ecsd.cust_num = ecbd.cust_num
        and ecsd.sls_org in ('3300', '330B', '330H', '3410', '341B')
        and cddes_pck.code_type(+) = 'Parent Customer Key'
        and cddes_pck.code(+) = ecsd.prnt_cust_key
),
final as
(
    select 
        c.sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
        upper(c.sap_prnt_cust_desc)::varchar(75) as sap_prnt_cust_desc,
        cast(pe_item_no as varchar(100)) as prod_cd,
        item_description::varchar(30) as prod_desc,
        try_to_date(trim(inv_dt), 'yyyymmdd') as inv_date,
        cast((stock_details_soh * nvl(item_details_pack_size, 0)) as numeric(16, 4)) as inventory_qty,
        0:: number(18,0) as inventory_amt
    from sdl_ig_inventory_data a
    join c on upper(c.sap_prnt_cust_desc) = 
        (
            select distinct trim(parameter_value) from itg_parameter_reg_inventory
            where parameter_name = 'parent_desc_IG'and country_name = 'AUSTRALIA'
        )
)
select * from final