with edw_my_siso_analysis as
(
    select * from {{ ref('mysedw_integration__edw_my_siso_analysis') }}
),
edw_vw_my_si_pos_inv_analysis as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_si_pos_inv_analysis') }}
),
wks_my_siso_propagate_final as
(
    select * from {{ ref('myswks_integration__wks_my_siso_propagate_final') }}
),
siso as
(
  select * from wks_my_siso_propagate_final
  where sap_parent_customer_desc <> ''

),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),

siso_analysis as
(
    select distinct
          global_prod_franchise,
          global_prod_brand,
          global_prod_sub_brand,
          global_prod_variant,
          global_prod_segment,
          global_prod_subsegment,
          global_prod_category,
          global_prod_subcategory,
          global_put_up_desc,
          nvl(nullif(sku, ''), 'NA') as sku,
          sku_desc
        from edw_my_siso_analysis
        where
          to_ccy = 'MYR'
),
si_pos_inv_analysis as 
(
    select 
        distinct
        global_prod_franchise,
        global_prod_brand,
        global_prod_sub_brand,
        global_prod_variant,
        global_prod_segment,
        global_prod_subsegment,
        global_prod_category,
        global_prod_subcategory,
        global_put_up_desc,
        nvl(nullif(sku, ''), 'NA') as sku,
        sku_desc
    from edw_vw_my_si_pos_inv_analysis
),
product as
(
      select * from
      (
        select * from siso_analysis
        union
        select * from si_pos_inv_analysis
        )
      where sku <> 'NA'
),
product1 as
(
    select
    *
    from (
        select
            product.*,
            EMD.pka_product_key as pka_product_key,
            EMD.pka_product_key_description as pka_product_key_description,
            EMD.pka_product_key as product_key,
            EMD.pka_product_key_description as product_key_description,
            EMD.pka_size_desc as pka_size_desc,
            row_number() over (partition by sku order by sku) as rnk
        from  product
    left join ( Select *
                        from edw_material_dim) as emd
    on product.sku = LTRIM(EMD.MATL_NUM, '0')
    )
    where
    rnk = 1
),
final as
(
    select
        month,
        trim(nvl(nullif(siso.dstrbtr_grp_cd, ''), 'NA')) as dstrbtr_grp_cd,
        trim(nvl(nullif(siso.distributor, ''), 'NA')) as distributor_id_name,
        trim(nvl(nullif(product.global_prod_brand, ''), 'NA')) as global_prod_brand,
        trim(nvl(nullif(product.global_prod_variant, ''), 'NA')) as global_prod_variant,
        trim(nvl(nullif(product.global_prod_segment, ''), 'NA')) as global_prod_segment,
        trim(nvl(nullif(product.global_prod_category, ''), 'NA')) as global_prod_category,
        trim(nvl(nullif(product.pka_size_desc, ''), 'NA')) as pka_size_desc,
        trim(nvl(nullif(product.pka_product_key, ''), 'NA')) as pka_product_key,
        trim(nvl(nullif(siso.sap_parent_customer_key, ''), 'NA')) as sap_parent_customer_key,
        sum(last_3months_so_value) as last_3months_so_val,
        sum(last_6months_so_value) as last_6months_so_val,
        sum(last_12months_so_value) as last_12months_so_val,
        sum(last_36months_so_value) as last_36months_so_val,
        case
        when nvl(last_36months_so_val, 0) > 0 and nvl(last_12months_so_val, 0) <= 0
        then 'N'
        else 'Y'
        end as healthy_inventory
    from siso
    left join  product1 as product
    on
    siso.matl_num = product.sku
    group by
    month,
    dstrbtr_grp_cd,
    distributor,
    global_prod_brand,
    global_prod_variant,
    global_prod_segment,
    global_prod_category,
    pka_size_desc,
    pka_product_key,
    sap_parent_customer_key
)
select 
    month::number(18,0) as month,
    dstrbtr_grp_cd::varchar(30) as dstrbtr_grp_cd,
    distributor_id_name::varchar(40) as distributor_id_name,
    global_prod_brand::varchar(30) as global_prod_brand,
    global_prod_variant::varchar(100) as global_prod_variant,
    global_prod_segment::varchar(50) as global_prod_segment,
    global_prod_category::varchar(50) as global_prod_category,
    pka_size_desc::varchar(30) as pka_size_desc,
    pka_product_key::varchar(68) as pka_product_key,
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    last_3months_so_val::number(38,17) as last_3months_so_val,
    last_6months_so_val::number(38,17) as last_6months_so_val,
    last_12months_so_val::number(38,17) as last_12months_so_val,
    last_36months_so_val::number(38,17) as last_36months_so_val,
    healthy_inventory::varchar(1) as healthy_inventory
from final
