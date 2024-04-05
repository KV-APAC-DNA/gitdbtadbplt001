with wks_au_dstr_api_header as 
(
    select * from snappcfwks_integration.wks_au_dstr_api_header
),
sdl_api_dstr as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_api_dstr') }}
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
month_1 as
(
    select 
        
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_01), 'MON-YY') AS so_date,
        b.month_01 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_2 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_02), 'MON-YY') AS so_date,
        b.month_02 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_3 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_03), 'MON-YY') AS so_date,
        b.month_03 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_4 as
(
    select 
        
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_04), 'MON-YY') AS so_date,
        b.month_04 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_5 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_05), 'MON-YY') AS so_date,
        b.month_05 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_6 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_06), 'MON-YY') AS so_date,
        b.month_06 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_7 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_07), 'MON-YY') AS so_date,
        b.month_07 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_8 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_08), 'MON-YY') AS so_date,
        b.month_08 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_9 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_09), 'MON-YY') AS so_date,
        b.month_09 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_10 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_10), 'MON-YY') AS so_date,
        b.month_10 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_11 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_11), 'MON-YY') AS so_date,
        b.month_11 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_12 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_12), 'MON-YY') AS so_date,
        b.month_12 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
month_13 as
(
    select 
        'API' Dstr_nm,
        b.article_id,
        b.article_desc,
        b.product_sap_id,
        b.product_ean,
        TO_DATE(Trim(a.month_13), 'MON-YY') AS so_date,
        b.month_13 AS sellout_qty,
        b.cost_price
    from wks_au_dstr_api_header a,
        sdl_api_dstr b
),
combined as
(
    select * from month_1
    union all
    select * from month_2
    union all
    select * from month_3
    union all
    select * from month_4
    union all
    select * from month_5
    union all
    select * from month_6
    union all
    select * from month_7
    union all
    select * from month_8
    union all
    select * from month_9
    union all
    select * from month_10
    union all
    select * from month_11
    union all
    select * from month_12
    union all
    select * from month_13
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
        a.so_date::date as so_date,
        coalesce(cast(a.sellout_qty as numeric(16, 4)), 0) as so_qty,
        cast(a.cost_price as numeric(10, 4)) as std_cost,
        current_timestamp::timestamp_ntz(9) as crt_dttm
    from combined as a
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