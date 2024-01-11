{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
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
brnd_tamr_id::number(38,0) as brnd_tamr_id,
ctgy_tamr_id::number(38,0) as ctgy_tamr_id,
brnd_origin_source_name::varchar(256) as brnd_origin_source_name,
ctgy_origin_source_name::varchar(256) as ctgy_origin_source_name,
brnd_origin_entity_id::varchar(256) as brnd_origin_entity_id,
ctgy_origin_entity_id::varchar(256) as ctgy_origin_entity_id,
brnd_unique_id::varchar(256) as brnd_unique_id,
ctgy_unique_id::varchar(256) as ctgy_unique_id,
brnd_manualclassificationid::number(18,0) as brnd_manualclassificationid,
ctgy_manualclassificationid::number(18,0) as ctgy_manualclassificationid,
brnd_manualclassificationpath::varchar(256) as brnd_manualclassificationpath,
ctgy_manualclassificationpath::varchar(256) as ctgy_manualclassificationpath,
brnd_suggestedclassificationid::varchar(256) as brnd_suggestedclassificationid,
ctgy_suggestedclassificationid::varchar(256) as ctgy_suggestedclassificationid,
brnd_suggestedclassificationpath::varchar(256) as brnd_suggestedclassificationpath,
ctgy_suggestedclassificationpath::varchar(256) as ctgy_suggestedclassificationpath,
brnd_suggestedclassificationscore::number(15,10) as brnd_suggestedclassificationscore,
ctgy_suggestedclassificationscore::number(15,10) as ctgy_suggestedclassificationscore,
brnd_finalclassificationpath::varchar(256) as brnd_finalclassificationpath,
ctgy_finalclassificationpath::varchar(256) as ctgy_finalclassificationpath,
materialnumber::varchar(128) as materialnumber,
region::varchar(50) as region,
gcph_franchise::varchar(30) as gcph_franchise,
gcph_brand::varchar(80) as gcph_brand,
gcph_subbrand::varchar(100) as gcph_subbrand,
gcph_variant::varchar(100) as gcph_variant,
gcph_needstate::varchar(50) as gcph_needstate,
gcph_category::varchar(50) as gcph_category,
gcph_subcategory::varchar(50) as gcph_subcategory,
gcph_segment::varchar(50) as gcph_segment,
gcph_subsegment::varchar(100) as gcph_subsegment,
ean_upc::varchar(30) as ean_upc,
emea_gbpbgc::varchar(50) as emea_gbpbgc,
emea_gbpmgrc::varchar(100) as emea_gbpmgrc,
emea_prodh3::varchar(50) as emea_prodh3,
apac_variant::varchar(256) as apac_variant,
industry_sector::varchar(50) as industry_sector,
market::varchar(30) as market,
data_type::varchar(30) as data_type,
family::varchar(50) as family,
product::varchar(50) as product,
product_hierarchy::varchar(100) as product_hierarchy,
description::varchar(2000) as description,
division::varchar(30) as division,
base_unit::varchar(30) as base_unit,
regional_brand::varchar(100) as regional_brand,
regional_subbrand::varchar(256) as regional_subbrand,
regional_megabrand::varchar(100) as regional_megabrand,
regional_franchise::varchar(100) as regional_franchise,
regional_franchise_group::varchar(50) as regional_franchise_group,
material_group::varchar(100) as material_group,
material_type::varchar(30) as material_type,
unit::varchar(30) as unit,
order_unit::varchar(30) as order_unit,
size_dimension::varchar(30) as size_dimension,
height::number(10,3) as height,
length::number(10,3) as length,
width::number(10,3) as width,
volume::number(10,3) as volume,
volume_unit::varchar(30) as volume_unit,
gross_weight::number(10,3) as gross_weight,
net_weight::number(10,3) as net_weight,
weight_unit::varchar(30) as weight_unit,
put_up_code::varchar(10) as put_up_code,
put_up_desc::varchar(100) as put_up_description, 
size::varchar(200) as size, 
unit_of_measure::varchar(20) as unit_of_measure,
brnd_dateofextract::varchar(30) as brnd_dateofextract,
ctgy_dateofextract::varchar(30) as ctgy_dateofextract,
brnd_cdl_datetime::varchar(30) as brnd_cdl_datetime,
ctgy_cdl_datetime::varchar(30) as ctgy_cdl_datetime,
brnd_cdl_source_file::varchar(50) as brnd_cdl_source_file,
ctgy_cdl_source_file::varchar(50) as ctgy_cdl_source_file,
brnd_load_key::varchar(100) as brnd_load_key,
ctgy_load_key::varchar(100) as ctgy_load_key,
current_timestamp()::timestamp_ntz(9) as crt_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

--Final select
select * from final 