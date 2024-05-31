with itg_pos as
(
    select * from {{ ref('ntaitg_integration__itg_pos') }}
),
itg_pos_store_product as
(
    select * from {{ ref('ntaitg_integration__itg_pos_store_product') }}
),
edw_product_attr_dim as
(
    select * from {{ source('aspedw_integration', 'edw_product_attr_dim') }}
),
itg_sales_cust_prod_master as
(
    select * from {{ ref('ntaitg_integration__itg_sales_cust_prod_master') }}
),
v_kr_ecommerce_sellout as
(
    select * from {{ ref('ntaedw_integration__v_kr_ecommerce_sellout') }}
),
union_1 as
(
    select
        p.src_sys_cd,
        ean_num,
        case
            when p.src_sys_cd = 'Costco' then product_nm
            else vend_prod_nm
        end as vend_prod_nm,
        count(*) record_count,
        min(pos_dt) min_pos_dt,
        max(pos_dt) max_pos_dt
    from itg_pos p
        left outer join itg_pos_store_product m on ltrim(ean_num, 0) = ltrim(product_cd, 0)
        and p.src_sys_cd = m.src_sys_cd
    where p.ctry_cd = 'KR'
        and ltrim(ean_num, 0) not in (
            select ltrim(ean, 0)
            from edw_product_attr_dim
            where cntry = 'KR'
            union
            select ltrim(cust_prod_cd, 0)
            from itg_sales_cust_prod_master
            where ctry_cd = 'KR'
        )
        and ean_num not in (
            select ltrim(cust_prod_cd, 0)
            from itg_sales_cust_prod_master
        )
    group by p.src_sys_cd,
        ean_num,
        vend_prod_nm,
        product_nm
    order by p.src_sys_cd,
        ean_num,
        vend_prod_nm,
        product_nm
),
union_2 as
(
    select distinct ecomm.distributor_name src_sys_cd,
        ecomm.ean ean_num,
        ecomm.product_name vend_prod_nm,
        count(1) record_count,
        min(to_date(ecomm.fisc_day)) min_pos_dt,
        max(to_date(ecomm.fisc_day)) max_pos_dt
    from v_kr_ecommerce_sellout ecomm
    where ecomm.product_name is null
        or ecomm.product_name = ''
    group by ecomm.distributor_name,
        ecomm.ean,
        ecomm.product_name
    order by ecomm.distributor_name,
        ecomm.ean,
        ecomm.product_name
),
final as
(
    select
        src_sys_cd,
        ean_num,
        vend_prod_nm,
        record_count,
        min_pos_dt,
        max_pos_dt
    from
        (
            select * from union_1
            union all
            select * from union_2
        )
    order by src_sys_cd,
        ean_num,
        vend_prod_nm
)
select * from final