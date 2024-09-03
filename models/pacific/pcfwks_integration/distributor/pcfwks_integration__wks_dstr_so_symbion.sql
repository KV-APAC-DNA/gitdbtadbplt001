with sdl_au_dstr_symbion_header as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_au_dstr_symbion_header') }}
),
sdl_symbion_dstr as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_symbion_dstr') }}
    where file_name not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_symbion_dstr__null_test')}}
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
combined as
(
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        date_trunc('MONTH',to_date(right(a.mtd, 9), 'DD MON YY')) AS so_date,
        b.month_01 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_01,'mon yy') as so_date,
        b.month_01 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_02,'mon yy') as so_date,
        b.month_02 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_03,'mon yy') as so_date,
        b.month_03 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_04,'mon yy') as so_date,
        b.month_04 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_05,'mon yy') as so_date,
        b.month_05 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_06,'mon yy') as so_date,
        b.month_06 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_07,'mon yy') as so_date,
        b.month_07 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_08,'mon yy') as so_date,
        b.month_08 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_09,'mon yy') as so_date,
        b.month_09 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_10,'mon yy') as so_date,
        b.month_10 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_11,'mon yy') as so_date,
        b.month_11 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_12,'mon yy') as so_date,
        b.month_12 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
    union all
    select 
        'SYMBION' Dstr_nm,
        b.symbion_product_no,
        b.symbion_product_desc,
        b.supplier_part_no,
        b.ean,
        to_date(a.month_13,'mon yy') as so_date,
        b.month_13 AS sellout_qty,
        b.global_std_cost
    from sdl_au_dstr_symbion_header a,
        sdl_symbion_dstr b
),
final as
(
    select 
        b.sap_prnt_cust_key::varchar(12) as sap_parent_customer_key,
        upper(a.dstr_nm)::varchar(10) as sap_parent_customer_desc,
        a.symbion_product_no::varchar(255) as dstr_prod_cd,
        a.symbion_product_desc::varchar(255) as dstr_product_desc,
        a.supplier_part_no::varchar(255) AS matl_num,
        a.ean::varchar(255) AS ean,
        a.so_date::date as so_date,
        cast(a.sellout_qty as numeric(16, 4)) AS so_qty,
        cast(a.global_std_cost as numeric(10, 4)) as std_cost,
        current_timestamp::timestamp_ntz(9) as crt_dttm
    from combined as a
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