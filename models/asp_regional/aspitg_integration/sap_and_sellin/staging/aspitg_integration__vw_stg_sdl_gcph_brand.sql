--Import CTE
with source as (
    select * from {{ source('mds_access', 'mds_gcph_brand') }}
),

--Logical CTE

--Final CTE
final as (
    select 
    coalesce(tamr_id::number(38,0),0) as tamr_id ,
    coalesce(origin_source_name::varchar(256),'') as origin_source_name ,
    coalesce(origin_entity_id::varchar(256),'') as origin_entity_id ,
    coalesce(manualclassificationid::number(18,0),0) as manualclassificationid ,
    coalesce(manualclassificationpath::varchar(256),'') as manualclassificationpath ,
    coalesce(suggestedclassificationid::varchar(256),'') as suggestedclassificationid ,
    coalesce(suggestedclassificationpath::varchar(256),'') as suggestedclassificationpath ,
    coalesce(suggestedclassificationscore::number(15,10),'') as suggestedclassificationscore ,
    coalesce(gcph_gfo_name::varchar(30),'') as gcph_gfo ,
    coalesce(regional_franchise_group::varchar(50),'') as regional_franchise_group ,
    null as product_hierarchy ,
    coalesce(regional_brand::varchar(100),'') as regional_brand ,
    coalesce(unique_id::varchar(256),'') as unique_id ,
    coalesce(material_type::varchar(30),'') as material_type ,
    coalesce(base_unit::varchar(30),'') as base_unit ,
    coalesce(weight_unit::varchar(30),'') as weight_unit ,
    null as product ,
    coalesce(regional_franchise::varchar(100),'') as regional_franchise ,
    coalesce(emea_gbpbgc::varchar(50),'') as emea_gbpbgc ,
    coalesce(ean_upc::varchar(30),'') as ean_upc ,
    null as emea_prodh3 ,
    null as industry_sector ,
    coalesce(volume_unit::varchar(30),'') as volume_unit ,
    coalesce(width::number(10,3),'') as width ,
    coalesce(description::varchar(256),'') as description ,
    coalesce(data_type::varchar(30),'') as data_type ,
    null as family ,
    coalesce(gross_weight::number(10,3),0) as gross_weight ,
    coalesce(unit::varchar(30),'') as unit ,
    coalesce(emea_gbpmgrc::varchar(100),'') as emea_gbpmgrc ,
    coalesce(region::varchar(50),'') as region ,
    coalesce(order_unit::varchar(30),'') as order_unit ,
    coalesce(material_group::varchar(100),'') as material_group ,
    coalesce(apac_variant::varchar(256),'') as apac_variant ,
    coalesce(length::number(10,3),0) as length ,
    coalesce(height::number(10,3),0) as height ,
    coalesce(regional_subbrand::varchar(256),'') as regional_subbrand ,
    null as size_dimension ,
    coalesce(gcph_brand_name::varchar(100),'') as gcph_brand ,
    coalesce(volume::number(10,3),0) as volume ,
    coalesce(net_weight::number(10,3),0) as net_weight ,
    null as market ,
    null as division ,
    coalesce(regional_megabrand::varchar(100),'') as regional_megabrand ,
    coalesce(material_number::varchar(128),'') as material_number ,
    coalesce(finalclassificationpath::varchar(256),'') as finalclassificationpath ,
    coalesce(gcph_subbrand_name::varchar(100),'') as gcph_subbrand ,
    coalesce(gcph_variant_name::varchar(100),'') as gcph_variant ,
    coalesce(dateofextract::varchar(30),'') as dateofextract ,
    null as cdl_datetime ,
    null as cdl_source_file ,
    null as load_key
    from source
)

--Final select
select * from final