{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "table",
        transient= false
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_edw_gch_producthierarchy') }}
),

--Logical CTE

--Final CTE
final as (
    select
brnd_tamr_id,
ctgy_tamr_id,
brnd_origin_source_name,
ctgy_origin_source_name,
brnd_origin_entity_id,
ctgy_origin_entity_id,
brnd_unique_id,
ctgy_unique_id,
brnd_manualclassificationid,
ctgy_manualclassificationid,
brnd_manualclassificationpath,
ctgy_manualclassificationpath,
brnd_suggestedclassificationid,
ctgy_suggestedclassificationid,
brnd_suggestedclassificationpath,
ctgy_suggestedclassificationpath,
brnd_suggestedclassificationscore,
ctgy_suggestedclassificationscore,
brnd_finalclassificationpath,
ctgy_finalclassificationpath,
materialnumber,
region,
gcph_franchise,
gcph_brand ,
gcph_subbrand,
gcph_variant,
gcph_needstate,
gcph_category,
gcph_subcategory,
gcph_segment,
gcph_subsegment,
ean_upc,
emea_gbpbgc,
emea_gbpmgrc,
emea_prodh3,
apac_variant,
industry_sector,
market,
data_type,
family,
product,
product_hierarchy,
description,
division,
base_unit,
regional_brand,
regional_subbrand,
regional_megabrand,
regional_franchise,
regional_franchise_group,
material_group,
material_type,
unit,
order_unit,
size_dimension,
height,
length,
width,
volume,
volume_unit,
gross_weight,
net_weight,
weight_unit,
put_up_code ,
put_up_desc as put_up_description,
size ,
unit_of_measure ,
brnd_dateofextract,
ctgy_dateofextract,
brnd_cdl_datetime,
ctgy_cdl_datetime,
brnd_cdl_source_file,
ctgy_cdl_source_file,
brnd_load_key,
ctgy_load_key,
current_timestamp()::timestamp_ntz(9) as crt_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

--Final select
select * from final 