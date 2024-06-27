with itg_pos as
(
    select * from {{ ref('ntaitg_integration__itg_pos') }}
),
itg_pos_store_product as
(
    select * from {{ ref('ntaitg_integration__itg_pos_store_product') }}
),
itg_sales_store_master as
(
    select * from {{ ref('ntaitg_integration__itg_sales_store_master') }}
),
edw_customer_attr_flat_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
final as
(
    select p.src_sys_cd,
        str_cd,
        case
            when p.src_sys_cd = 'Costco' then store_nm
            else str_nm
        end as str_nm,
        count(*) record_count,
        min(pos_dt) min_pos_dt,
        max(pos_dt) max_pos_dt
    from itg_pos p
        left outer join itg_pos_store_product m on ltrim(str_cd, 0) = ltrim(store_cd, 0)
        and p.src_sys_cd = m.src_sys_cd
    where p.ctry_cd = 'KR'
        and str_cd not like 'VV%'
        and not exists (
            select 1
            from itg_sales_store_master m,
                (
                    select distinct sls_grp,
                        sls_grp_cd,
                        case
                            when lower(sls_grp) like 'e%mart%' then 'Emart'
                            when lower(sls_grp) like 'lotte%vic%' then 'Lotte mart'
                            when lower(sls_grp) like 'homeplus%' then 'Homeplus'
                            when lower(sls_grp) like 'tesco%' then 'Homeplus'
                            else sls_grp
                        end file
                    from edw_customer_attr_flat_dim
                    where cntry = 'Korea'
                ) c
            where c.sls_grp_cd = m.sales_grp_cd
                and c.file = p.src_sys_cd
                and m.cust_store_cd = ltrim(p.str_cd, 0)
        )
    group by p.src_sys_cd,
        str_cd,
        str_nm,
        store_nm
    order by p.src_sys_cd,
        str_cd,
        str_nm,
        store_nm
)
select * from final