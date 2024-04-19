with wks_au_dstr_chs_header as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_au_dstr_chs_header') }}
),
sdl_chs_dstr as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_chs_dstr') }}
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
month_1 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_01, '-', 1)) || '-' || trim(split_part(a.month_01, '-', 2)),'YYYY-MON') AS so_date,
        b.month_01 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b
),
month_2 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_02, '-', 1)) || '-' || trim(split_part(a.month_02, '-', 2)),'YYYY-MON') AS so_date,
        b.month_02 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b
),
month_3 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_03, '-', 1)) || '-' || trim(split_part(a.month_03, '-', 2)),'YYYY-MON') AS so_date,
        b.month_03 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b
),
month_4 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_04, '-', 1)) || '-' || trim(split_part(a.month_04, '-', 2)),'YYYY-MON') AS so_date,
        b.month_04 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b
),
month_5 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_05, '-', 1)) || '-' || trim(split_part(a.month_05, '-', 2)),'YYYY-MON') AS so_date,
        b.month_05 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b   
),
month_6 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_06, '-', 1)) || '-' || trim(split_part(a.month_06, '-', 2)),'YYYY-MON') AS so_date,
        b.month_06 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b 
),
month_7 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_07, '-', 1)) || '-' || trim(split_part(a.month_07, '-', 2)),'YYYY-MON') AS so_date,
        b.month_07 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b    
),
month_8 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_08, '-', 1)) || '-' || trim(split_part(a.month_08, '-', 2)),'YYYY-MON') AS so_date,
        b.month_08 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
    sdl_chs_dstr as b 
),
month_9 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_09, '-', 1)) || '-' || trim(split_part(a.month_09, '-', 2)),'YYYY-MON') AS so_date,
        b.month_09 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b
),
month_10 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_10, '-', 1)) || '-' || trim(split_part(a.month_10, '-', 2)),'YYYY-MON') AS so_date,
        b.month_10 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b
),
month_11 as
(
     select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_11, '-', 1)) || '-' || trim(split_part(a.month_11, '-', 2)),'YYYY-MON') AS so_date,
        b.month_11 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b    
),
month_12 as
(
    select 
        'CENTRAL HEALTHCARE SERVICES PTY LTD' Dstr_nm,
        b.product_code,
        b.product_desc,
        b.supplier_product_code,
        b.primary_gtin,
        to_date(trim(split_part(a.month_12, '-', 1)) || '-' || trim(split_part(a.month_12, '-', 2)),'YYYY-MON') AS so_date,
        b.month_12 AS sellout_qty,
        b.last_cost
    from wks_au_dstr_chs_header a,
        sdl_chs_dstr as b 
),
combined as
(
    select * from month_1          
    UNION ALL
    select * from month_2 
    UNION ALL
    select * from month_3  
    UNION ALL
    select * from month_4  
    UNION ALL
    select * from month_5  
    UNION ALL
    select * from month_6  
    UNION ALL
    select * from month_7  
    UNION ALL
    select * from month_8  
    UNION ALL
    select * from month_9  
    UNION ALL
    select * from month_10 
    UNION ALL
    select * from month_11
    UNION ALL
    select * from month_12
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
        a.so_date::date as so_date,
        cast(a.sellout_qty as numeric(16, 4)) as so_qty,
        cast(a.last_cost as numeric(10, 4)) as std_cost,
        current_timestamp::timestamp_ntz(9) as crt_dttm
    from combined a
    left join 
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
    ) b on upper(a.dstr_nm) = upper(b.sap_prnt_cust_desc)
)
select * from final