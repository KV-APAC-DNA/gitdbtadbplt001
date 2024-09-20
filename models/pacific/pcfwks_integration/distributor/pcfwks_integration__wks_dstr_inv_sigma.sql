with sdl_sigma_dstr as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_sigma_dstr') }}
    where file_name not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_sigma_dstr__null_test')}}
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
sigmar as 
(
    select 
        'SIGMA' as Dstr_nm,
        null_mat.product_code,
        null_mat.product_desc,
        all_mat.supplier_product_code,
        all_mat.ean,
        null_mat.inv_date,
        null_mat.SOH_QTY,
        null_mat.SOH_AMT,
        null_mat.back_order_qty,
        all_mat.cost_price as std_cost
    from 
    (
        select 
            substring(product_code, 1, length(product_code) -1) as prod_prefix,
            *
        from sdl_sigma_dstr
        where supplier_product_code is null
            AND not (
                substring(product_code, length(product_code)) >= 0
                and substring(product_code, length(product_code)) <= 9
            )
    ) null_mat
    left join 
    (
        select substring(product_code, 1, length(product_code) -1) as prod_prefix,
            *,
            row_number() over (
                partition by product_code
                order by inv_date desc
            ) AS rn
        from sdl_sigma_dstr
        where supplier_product_code != ''
    ) all_mat on null_mat.prod_prefix = all_mat.prod_prefix
    and all_mat.rn = 1
),
a as
(
    select * from sigmar
    union all
    select 
        'SIGMA' as Dstr_nm,
        product_code,
        product_desc,
        supplier_product_code,
        ean,
        inv_date,
        soh_qty,
        soh_amt,
        back_order_qty,
        cost_price as std_cost
    from sdl_sigma_dstr
    where supplier_product_code != ''
    union all
    select 
        'SIGMA' as Dstr_nm,
        product_code,
        product_desc,
        supplier_product_code,
        ean,
        inv_date,
        soh_qty,
        soh_amt,
        back_order_qty,
        cost_price as std_cost
    from sdl_sigma_dstr
    where supplier_product_code is null
        and (
            substring(product_code, length(product_code)) >= 0
            and substring(product_code, length(product_code)) <= 9
        )
),
final as 
(
    select 
        b.sap_prnt_cust_key::varchar(12) as sap_parent_customer_key,
        upper(b.sap_prnt_cust_desc)::varchar(75) as sap_parent_customer_desc,
        product_code::varchar(50) as dstr_prod_cd,
        product_desc::varchar(255) as dstr_product_desc,
        supplier_product_code::varchar(50) as matl_num,
        ean::varchar(50) as ean,
        try_to_date(inv_date, 'YYYYMMDD') as inv_date,
        back_order_qty::number(10,4) as back_order_qty,
        std_cost::number(10,4) as std_cost,
        soh_qty::number(10,4) as inventory_qty,
        soh_amt::number(10,4) as inventory_amt,
        current_timestamp::timestamp_ntz(9) as crtd_dttm
    from a
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