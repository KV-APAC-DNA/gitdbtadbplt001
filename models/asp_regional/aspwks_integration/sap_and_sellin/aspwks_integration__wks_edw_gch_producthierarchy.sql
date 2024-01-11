{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}


--Import CTE
with sdl_gcph_brand as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_gcph_brand') }}
),
sdl_gcph_category as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_gcph_category') }}
),
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),



--Logical CTE

--Final CTE
final as (
    select
  prod.material_num as materialnumber,
  prod.brnd_tamr_id,
  prod.ctgy_tamr_id,
  prod.brnd_origin_source_name,
  prod.ctgy_origin_source_name,
  prod.brnd_origin_entity_id,
  prod.ctgy_origin_entity_id,
  prod.brnd_unique_id,
  prod.ctgy_unique_id,
  prod.brnd_manualclassificationid,
  prod.ctgy_manualclassificationid,
  prod.brnd_manualclassificationpath,
  prod.ctgy_manualclassificationpath,
  prod.brnd_suggestedclassificationid,
  prod.ctgy_suggestedclassificationid,
  prod.brnd_suggestedclassificationpath,
  prod.ctgy_suggestedclassificationpath,
  prod.brnd_suggestedclassificationscore,
  prod.ctgy_suggestedclassificationscore,
  prod.brnd_finalclassificationpath,
  prod.ctgy_finalclassificationpath,
  prod.brnd_material_number,
  prod.ctgy_material_number,
  prod.region,
  prod.gcph_gfo as gcph_franchise,
  prod.gcph_brand,
  prod.gcph_subbrand,
  prod.gcph_variant,
  prod.gcph_needstate,
  prod.gcph_category,
  prod.gcph_subcategory,
  prod.gcph_segment,
  prod.gcph_subsegment,
  prod.ean_upc,
  prod.emea_gbpbgc,
  prod.emea_gbpmgrc,
  prod.emea_prodh3,
  prod.apac_variant,
  prod.industry_sector,
  prod.market,
  prod.data_type,
  prod.family,
  prod.product,
  prod.product_hierarchy,
  prod.description,
  prod.division,
  prod.base_unit,
  prod.regional_brand,
  prod.regional_subbrand,
  prod.regional_megabrand,
  prod.regional_franchise,
  prod.regional_franchise_group,
  prod.material_group,
  prod.material_type,
  prod.unit,
  prod.order_unit,
  prod.size_dimension,
  prod.height,
  prod.length,
  prod.width,
  prod.volume,
  prod.volume_unit,
  prod.gross_weight,
  prod.net_weight,
  prod.weight_unit,
  c.put_up as put_up_code,
  c.put_up_desc,
  split_part(trim(c.put_up_desc), ' ', 1) as size,
  (
    split_part(trim(c.put_up_desc), ' ', 2) || ' ' || split_part(trim(c.put_up_desc), ' ', 3)
  ) as unit_of_measure,
  prod.brnd_dateofextract,
  prod.ctgy_dateofextract,
  prod.brnd_cdl_datetime,
  prod.ctgy_cdl_datetime,
  prod.brnd_cdl_source_file,
  prod.ctgy_cdl_source_file,
  prod.brnd_load_key,
  prod.ctgy_load_key,
  --tgt.crt_dttm as tgt_crt_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
  --case when tgt.crt_dttm is null then 'i' else 'u' end as chng_flg
from (
  select
    coalesce(gcph.brnd_material_number, gcph.ctgy_material_number) as material_num,
    *
  from (
    select
      a.tamr_id as brnd_tamr_id,
      b.tamr_id as ctgy_tamr_id,
      a.origin_source_name as brnd_origin_source_name,
      b.origin_source_name as ctgy_origin_source_name,
      a.origin_entity_id as brnd_origin_entity_id,
      b.origin_entity_id as ctgy_origin_entity_id,
      a.unique_id as brnd_unique_id,
      b.unique_id as ctgy_unique_id,
      a.manualclassificationid as brnd_manualclassificationid,
      b.manualclassificationid as ctgy_manualclassificationid,
      a.manualclassificationpath as brnd_manualclassificationpath,
      b.manualclassificationpath as ctgy_manualclassificationpath,
      a.suggestedclassificationid as brnd_suggestedclassificationid,
      b.suggestedclassificationid as ctgy_suggestedclassificationid,
      a.suggestedclassificationpath as brnd_suggestedclassificationpath,
      b.suggestedclassificationpath as ctgy_suggestedclassificationpath,
      a.suggestedclassificationscore as brnd_suggestedclassificationscore,
      b.suggestedclassificationscore as ctgy_suggestedclassificationscore,
      a.finalclassificationpath as brnd_finalclassificationpath,
      b.finalclassificationpath as ctgy_finalclassificationpath,
      a.material_number as brnd_material_number,
      b.material_number as ctgy_material_number,
      coalesce(a.region, b.region) as region,
      a.gcph_gfo as gcph_gfo,
      a.gcph_brand as gcph_brand,
      a.gcph_subbrand as gcph_subbrand,
      a.gcph_variant as gcph_variant,
      b.gcph_needstate as gcph_needstate,
      b.gcph_category as gcph_category,
      b.gcph_subcategory as gcph_subcategory,
      b.gcph_segment as gcph_segment,
      b.gcph_subsegment as gcph_subsegment,
      coalesce(a.ean_upc, b.ean_upc) as ean_upc,
      coalesce(a.emea_gbpbgc, b.emea_gbpbgc) as emea_gbpbgc,
      coalesce(a.emea_gbpmgrc, b.emea_gbpmgrc) as emea_gbpmgrc,
      coalesce(a.emea_prodh3, b.emea_prodh3) as emea_prodh3,
      coalesce(a.apac_variant, b.apac_variant) as apac_variant,
      coalesce(a.industry_sector, b.industry_sector) as industry_sector,
      coalesce(a.market, b.market) as market,
      coalesce(a.data_type, b.data_type) as data_type,
      coalesce(a.family, b.family) as family,
      coalesce(a.product, b.product) as product,
      coalesce(a.product_hierarchy, b.product_hierarchy) as product_hierarchy,
      coalesce(a.description, b.description) as description,
      coalesce(a.division, b.division) as division,
      coalesce(a.base_unit, b.base_unit) as base_unit,
      coalesce(a.regional_brand, b.regional_brand) as regional_brand,
      coalesce(a.regional_subbrand, b.regional_subbrand) as regional_subbrand,
      coalesce(a.regional_megabrand, b.regional_megabrand) as regional_megabrand,
      coalesce(a.regional_franchise, b.regional_franchise) as regional_franchise,
      coalesce(a.regional_franchise_group, b.regional_franchise_group) as regional_franchise_group,
      coalesce(a.material_group, b.material_group) as material_group,
      coalesce(a.material_type, b.material_type) as material_type,
      coalesce(a.unit, b.unit) as unit,
      coalesce(a.order_unit, b.order_unit) as order_unit,
      coalesce(a.size_dimension, b.size_dimension) as size_dimension,
      coalesce(a.height, b.height) as height,
      coalesce(a.length, b.length) as length,
      coalesce(a.width, b.width) as width,
      coalesce(a.volume, b.volume) as volume,
      coalesce(a.volume_unit, b.volume_unit) as volume_unit,
      coalesce(a.gross_weight, b.gross_weight) as gross_weight,
      coalesce(a.net_weight, b.net_weight) as net_weight,
      coalesce(a.weight_unit, b.weight_unit) as weight_unit,
      a.dateofextract as brnd_dateofextract,
      b.dateofextract as ctgy_dateofextract,
      a.cdl_datetime as brnd_cdl_datetime,
      b.cdl_datetime as ctgy_cdl_datetime,
      a.cdl_source_file as brnd_cdl_source_file,
      b.cdl_source_file as ctgy_cdl_source_file,
      a.load_key as brnd_load_key,
      b.load_key as ctgy_load_key
    from (
      select
        *
      from sdl_gcph_brand
      where
        not material_number is null
    ) as a
    full outer join (
      select
        *
      from sdl_gcph_category
      where
        not material_number is null
    ) as b
      on a.material_number = b.material_number and a.region = b.region
  ) as gcph
) as prod
left join (
  select distinct
    matl_num,
    put_up,
    put_up_desc
  from edw_material_dim
) as c
  on ltrim(prod.material_num, 0) = ltrim(c.matl_num, 0) 
)

--final select
select * from final