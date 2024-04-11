with sdl_au_dstr_sigma_header as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_au_dstr_sigma_header') }}
),
sdl_sigma_dstr as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_sigma_dstr') }}
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
sdl_sigma_temp as 
(
    select 
        null_mat.product_code,
        null_mat.product_desc,
        all_mat.supplier_product_code,
        all_mat.ean,
        all_mat.cost_price,
        null_mat.month_01,
        null_mat.month_02,
        null_mat.month_03,
        null_mat.month_04,
        null_mat.month_05,
        null_mat.month_06,
        null_mat.month_07,
        null_mat.month_08,
        null_mat.month_09,
        null_mat.month_10,
        null_mat.month_11,
        null_mat.month_12,
        null_mat.month_13,
        null_mat.month_14,
        null_mat.month_15,
        null_mat.month_16
    from
    (
        select 
            substring(product_code, 1, length(product_code) -1) as prod_prefix,
            *
        from sdl_sigma_dstr
        where supplier_product_code is null
            and not (
                substring(product_code, length(product_code)) >= 0
                and substring(product_code, length(product_code)) <= 9
            )
    ) null_mat
    left join 
    (
        select 
            substring(product_code, 1, length(product_code) -1) as prod_prefix,
            *,
            row_number() over (
                partition by product_code
                order by inv_date desc
            ) as rn
        from sdl_sigma_dstr
        where supplier_product_code != ''
    ) all_mat on null_mat.prod_prefix = all_mat.prod_prefix
    and all_mat.rn = 1
),
sdl_sigma_detail as 
(
    select 
        product_code,
        product_desc,
        supplier_product_code,
        ean,
        cost_price,
        month_01,
        month_02,
        month_03,
        month_04,
        month_05,
        month_06,
        month_07,
        month_08,
        month_09,
        month_10,
        month_11,
        month_12,
        month_13,
        month_14,
        month_15,
        month_16
    from sdl_sigma_dstr
    where product_code not in 
    (
        select distinct product_code from sdl_sigma_temp
    )
    union all
    select 
        product_code,
        product_desc,
        supplier_product_code,
        ean,
        cost_price,
        month_01,
        month_02,
        month_03,
        month_04,
        month_05,
        month_06,
        month_07,
        month_08,
        month_09,
        month_10,
        month_11,
        month_12,
        month_13,
        month_14,
        month_15,
        month_16
    from sdl_sigma_temp
),
month_1 as
(
    select 
        a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_01, '-', 1)) || '-' || left(trim(split_part(b.month_01, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_01 as so_qty
    from sdl_sigma_detail a,
    sdl_au_dstr_sigma_header b
),
month_2 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_02, '-', 1)) || '-' || left(trim(split_part(b.month_02, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_02 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_3 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_03, '-', 1)) || '-' || left(trim(split_part(b.month_03, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_03 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_4 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_04, '-', 1)) || '-' || left(trim(split_part(b.month_04, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_04 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_5 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_05, '-', 1)) || '-' || left(trim(split_part(b.month_05, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_05 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_6 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_06, '-', 1)) || '-' || left(trim(split_part(b.month_06, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_06 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_7 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_07, '-', 1)) || '-' || left(trim(split_part(b.month_07, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_07 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_8 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_08, '-', 1)) || '-' || left(trim(split_part(b.month_08, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_08 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_9 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_09, '-', 1)) || '-' || left(trim(split_part(b.month_09, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_09 as so_qty
    from sdl_sigma_detail a,
    sdl_au_dstr_sigma_header b
),
month_10 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_10, '-', 1)) || '-' || left(trim(split_part(b.month_10, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_10 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_11 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_11, '-', 1)) || '-' || left(trim(split_part(b.month_11, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_11 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_12 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_12, '-', 1)) || '-' || left(trim(split_part(b.month_12, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_12 as so_qty
    from sdl_sigma_detail a,
    sdl_au_dstr_sigma_header b
),
month_13 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_13, '-', 1)) || '-' || left(trim(split_part(b.month_13, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_13 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_14 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_14, '-', 1)) || '-' || left(trim(split_part(b.month_14, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_14 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_15 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_15, '-', 1)) || '-' || left(trim(split_part(b.month_15, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_15 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
month_16 as
(
    select a.product_code,
        a.product_desc,
        a.supplier_product_code,
        a.ean,
        a.cost_price,
        to_date(
            trim(split_part(b.month_16, '-', 1)) || '-' || left(trim(split_part(b.month_16, '-', 2)),3),
            'YYYY-MON'
        ) as so_date,
        a.month_16 as so_qty
    from sdl_sigma_detail a,
        sdl_au_dstr_sigma_header b
),
combined as
(
    Select 
        'SIGMA' as dstr_nm,
        product_code,
        product_desc,
        supplier_product_code,
        ean,
        so_date,
        so_qty,
        cost_price
    from
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
        union all
        select * from month_14
        union all
        select * from month_15
        union all
        select * from month_16
    )
), 
final as
(
    select 
        b.sap_prnt_cust_key::varchar(12) as sap_parent_customer_key,
        upper(dstr_nm)::varchar(5) as sap_parent_customer_desc,
        product_code::varchar(50) as dstr_prod_cd,
        product_desc::varchar(255) as dstr_product_desc,
        supplier_product_code::varchar(50) as matl_num,
        ean::varchar(50) as ean,
        so_date::date as so_date,
        cost_price::number(10,4) as std_cost,
        so_qty::number(10,4) as so_qty,
        current_timestamp::timestamp_ntz(9) as crtd_dttm
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