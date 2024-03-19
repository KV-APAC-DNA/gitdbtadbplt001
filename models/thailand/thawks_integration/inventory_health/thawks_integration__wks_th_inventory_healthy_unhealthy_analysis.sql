with 
wks_thailand_siso_propagate_final as 
(
    select * from {{ ref('thawks_integration__wks_thailand_siso_propagate_final') }}
),
edw_vw_th_material_dim as 
(
    select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
),

edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),

t4 as 
(
    select *
    from 
    (
    select *,
        row_number() over (
            partition by sku_cd
            order by sku_cd desc
        ) as rank
    from (
            select 
                emd.pka_product_key as pka_product_key,
                emd.pka_product_key_description as pka_product_key_description,
                emd.pka_product_key as product_key,
                emd.pka_product_key_description as product_key_description,
                emd.pka_size_desc as pka_size_desc,
                t4.gph_prod_frnchse,
                t4.gph_prod_brnd,
                t4.gph_prod_sub_brnd,
                t4.gph_prod_vrnt,
                t4.gph_prod_sgmnt,
                t4.gph_prod_subsgmnt,
                t4.gph_prod_ctgry,
                t4.gph_prod_subctgry,
                ltrim(t4.sap_matl_num) as sku_cd,
                sap_mat_desc
            from (
                    select *
                    from edw_vw_th_material_dim
                    where cntry_key = 'TH'
                ) t4,
                (
                    select *
                    from edw_material_dim
                ) emd where ltrim(t4.sap_matl_num, 0) = ltrim(emd.matl_num(+), 0)
                  )
            )
        where rank = 1
),
final  as
(
select 
    month::numeric(18,0) as month,
    t4.gph_prod_brnd::varchar(30) as brand,
    t4.gph_prod_vrnt::varchar(100) as variant,
    t4.gph_prod_sgmnt::varchar(50) as segment,
    t4.gph_prod_ctgry::varchar(50) as prod_category,
    trim(nvl(nullif(t4.pka_size_desc, ''), 'NA'))::varchar(30) as pka_size_desc,
    trim(nvl(nullif(t4.pka_product_key, ''), 'NA'))::varchar(68) as pka_product_key,
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    sum(last_3months_so_value)::float as last_3months_so_val,
    sum(last_6months_so_value)::float as last_6months_so_val,
    sum(last_12months_so_value)::float as last_12months_so_val,
    sum(last_36months_so_value)::float as last_36months_so_val,
    case
        when coalesce(last_36months_so_val, 0) > 0
        and coalesce(last_12months_so_val, 0) <= 0 then 'N'
        else 'Y'
    end::varchar(1) as healthy_inventory
from  wks_thailand_siso_propagate_final as siso,t4
where ltrim(siso.matl_num, 0) = ltrim(t4.sku_cd(+), 0)
group by month,
    gph_prod_brnd,
    gph_prod_vrnt,
    gph_prod_sgmnt,
    gph_prod_ctgry,
    pka_size_desc,
    pka_product_key,
    sap_parent_customer_key
)

select * from final