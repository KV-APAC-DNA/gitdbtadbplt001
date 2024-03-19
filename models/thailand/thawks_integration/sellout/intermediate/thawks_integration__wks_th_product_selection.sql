
with edw_vw_th_material_dim as (
select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
),
edw_material_dim as (
select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_vw_greenlight_skus as (
select * {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
final as (
select 
  * 
from 
  (
    select 
      *, 
      row_number() over (
        partition by sku_cd 
        order by 
          sku_cd desc
      ) as rank 
    from 
      (
        select 
          --emd.greenlight_sku_flag as greenlight_sku_flag,
          emd.pka_product_key as pka_product_key, 
          emd.pka_product_key_description as pka_product_key_description, 
          --emd.sls_org as sls_org,
          t4.gph_prod_frnchse, 
          t4.gph_prod_brnd, 
          t4.gph_prod_sub_brnd, 
          t4.gph_prod_vrnt, 
          t4.gph_prod_sgmnt, 
          t4.gph_prod_subsgmnt, 
          t4.gph_prod_ctgry, 
          t4.gph_prod_subctgry, 
          t4.gph_prod_put_up_desc, 
          ltrim(t4.sap_matl_num) as sku_cd, 
          sap_mat_desc 
        from 
          (
            select 
              * 
            from 
              edw_vw_th_material_dim 
            where 
              cntry_key = 'TH'
          ) t4 
          left join edw_material_dim --(select *
          --from edw_vw_greenlight_skus
          --where sls_org in ('2400','2500')) 
          emd on ltrim (t4.sap_matl_num, 0) = ltrim (
            emd.matl_num , 
            0
          )
      )
  ) 
where 
  rank = 1
  )
select * from final 