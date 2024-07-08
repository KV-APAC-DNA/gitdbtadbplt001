with itg_perenso_product_group_reln as(
    select * from {{ ref('pcfitg_integration__itg_perenso_product_group_reln') }}
),
itg_perenso_product as(
    select * from {{ ref('pcfitg_integration__itg_perenso_product') }}
), 
itg_perenso_product_fields as(
    select * from {{ ref('pcfitg_integration__itg_perenso_product_fields') }}
), 
itg_perenso_product_reln_id as(
    select * from {{ ref('pcfitg_integration__itg_perenso_product_reln_id') }}
), 
itg_perenso_product_group_lvl as(
    select * from {{ ref('pcfitg_integration__itg_perenso_product_group_lvl') }}
), 
itg_perenso_product_group as(
    select * from {{ ref('pcfitg_integration__itg_perenso_product_group') }}
), 
union1 as(
      select
    ipp.prod_key,
    case
      when ippgl.field_key in ('66', '67', '70', '78', '79')
      then null
      else ippgr.prod_grp_key
    end as prod_grp_key,
    ippg.prod_grp_desc,
    ippg.prod_grp_lev_key,
    ippgl.prod_lev_desc,
    ippgl.field_key,
    ippf.field_desc
  from itg_perenso_product as ipp, itg_perenso_product_group_reln as ippgr, itg_perenso_product_group as ippg, itg_perenso_product_group_lvl as ippgl, itg_perenso_product_fields as ippf
  where
    ipp.prod_key = ippgr.prod_key
    and ippgr.prod_grp_key = ippg.prod_grp_key
    and ippg.prod_grp_lev_key = ippgl.prod_grp_lev_key
    and ippgl.field_key = ippf.field_key
),
ippri as(
    select * from itg_perenso_product_reln_id
    where
      (prod_key, field_key) in (
        select
          prod_key,
          field_key
        from itg_perenso_product_reln_id
        group by
          prod_key,
          field_key
        having
          count(*) = 1
      )
),
transformed as(
    select distinct
  prod_key,
  coalesce(field_key, '999999') as field_key,
  coalesce(prod_grp_lev_key, '999999') as prod_grp_lev_key,
  coalesce(prod_grp_key, '999999') as prod_grp_key,
  field_desc,
  prod_lev_desc,
  prod_grp_desc
from (
  select * from union1
  union
  select
    ipp.prod_key,
    null as prod_grp_key,
    ippri.id as prod_grp_desc,
    null as prod_grp_lev_key,
    null as prod_lev_desc,
    ippri.field_key,
    ippf.field_desc
  from itg_perenso_product as ipp, ippri, itg_perenso_product_fields as ippf
  where
    ipp.prod_key = ippri.prod_key(+) and ippri.field_key = ippf.field_key(+)
)
)
select * from transformed