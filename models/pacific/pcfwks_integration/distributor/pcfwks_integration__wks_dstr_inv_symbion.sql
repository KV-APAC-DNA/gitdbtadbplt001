with sdl_au_dstr_symbion_header as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_au_dstr_symbion_header') }}
),
sdl_symbion_dstr as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_symbion_dstr') }}
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
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        b.global_std_cost,
        to_date(trim(inv_dt), 'MON DD YYYY') as inv_date,
        b.soh_qty AS inventory_qty,
        b.soh_amt AS inventory_amt,
        b.back_order as back_order_qty
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
),
final as 
(
    select 
        b.sap_prnt_cust_key::varchar(12) as sap_parent_customer_key,
        upper(a.dstr_nm)::varchar(7) as sap_parent_customer_desc,
        a.symbion_product_no::varchar(255) as dstr_prod_cd,
        a.symbion_product_desc::varchar(255) as dstr_product_desc,
        a.supplier_part_no::varchar(255) as matl_num,
        a.ean::varchar(255) as ean,
        a.inv_date::date as inv_date,
        cast(a.inventory_qty as numeric(16, 4)) as inventory_qty,
        cast(a.inventory_amt as numeric(16, 4)) as inventory_amt,
        cast(a.back_order_qty as numeric(16, 4)) as back_order_qty,
        cast(a.global_std_cost as numeric(10, 4)) as std_cost,
        current_timestamp::timestamp_ntz(9) as crt_dttm
    from  a
    left join 
    (
        select distinct ecsd.prnt_cust_key as sap_prnt_cust_key,
            cddes_pck.code_desc as sap_prnt_cust_desc
        from edw_customer_sales_dim ecsd,
            edw_customer_base_dim ecbd,
            edw_code_descriptions cddes_pck
        where ecsd.cust_num = ecbd.cust_num
            and ecsd.sls_org in ('3300', '330B', '330H', '3410', '341B')
            and cddes_pck.code_type(+) = 'Parent Customer Key'
            and cddes_pck.code(+) = ecsd.prnt_cust_key
    ) b on upper(a.dstr_nm) = upper(b.sap_prnt_cust_desc)
)
select * from final