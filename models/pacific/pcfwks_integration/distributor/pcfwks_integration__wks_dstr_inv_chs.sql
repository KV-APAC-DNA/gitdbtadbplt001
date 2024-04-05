with wks_au_dstr_chs_header as 
(
    select * from snappcfwks_integration.wks_au_dstr_chs_header
),
sdl_chs_dstr as
(
    select * from {{ source('pcfsdl_raw', 'sdl_chs_dstr') }}
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
a as 
(
    SELECT 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        ty_to_date(inv_dt, 'DD-MM-YYYY') AS inv_date,
        b.soh_qty AS inventory_qty,
        b.soh_amt AS inventory_amt,
        b.back_order_qty as back_order_qty,
        b.last_cost
    FROM wks_au_dstr_chs_header a,
        sdl_chs_dstr as b
),
b as
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
        b.sap_prnt_cust_key::varchar(12) as sap_parent_customer_key,
        a.dstr_nm::varchar(35) as sap_parent_customer_desc,
        a.product_code::varchar(255) as dstr_prod_cd,
        a.product_desc::varchar(255) as dstr_product_desc,
        a.supplier_product_code::varchar(255) as matl_num,
        a.primary_gtin::varchar(255) as ean,
        a.inv_date::date as inv_date,
        cast(a.inventory_qty as numeric(16, 4)) as inventory_qty,
        cast(a.inventory_amt as numeric(16, 4)) as inventory_amt,
        cast(a.back_order_qty as numeric(16, 4)) as back_order_qty,
        cast(a.last_cost as numeric(10, 4)) as std_cost,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from  a
    left join b 
    on upper(a.dstr_nm) = upper(b.sap_prnt_cust_desc)
)
select * from final