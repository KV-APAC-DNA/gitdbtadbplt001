with sdl_au_dstr_api_header as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_au_dstr_api_header') }}
),
sdl_api_dstr as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_api_dstr') }}
    where file_name not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_api_dstr__null_test')}}
    )
),
edw_customer_sales_dim as
(
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
a as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        invt_dt AS inv_date,
        b.soh_qty AS inventory_qty,
        (b.soh_qty * b.cost_price) AS inventory_amt,
        b.so_backorder_qty as back_order_qty,
        b.cost_price
    from sdl_au_dstr_api_header a,
        sdl_api_dstr b
),
final as
(
    select 
        b.sap_prnt_cust_key::varchar(12) as sap_parent_customer_key,
        a.dstr_nm::varchar(3) as sap_parent_customer_desc,
        a.article_id::varchar(50) as dstr_prod_cd,
        a.article_desc::varchar(100) as dstr_product_desc,
        a.product_sap_id::varchar(50) as matl_num,
        a.product_ean::varchar(50) as ean,
        a.inv_date::varchar(50) as inv_date,
        cast(a.inventory_qty as numeric(16, 4)) AS inventory_qty,
        cast(a.inventory_amt as numeric(16, 4)) as inventory_amt,
        cast(a.back_order_qty as numeric(16, 4)) as back_order_qty,
        cast(a.cost_price as numeric(10, 4)) as std_cost,
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
