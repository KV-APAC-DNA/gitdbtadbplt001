--Import CTE
with source as (
    select * from {{ source('mds_access', 'mds_gcph_category') }}
),

--Logical CTE

--Final CTE
final as (
    select 
    tamr_id::number(38,0) as tamr_id ,
    origin_source_name::varchar(256) as origin_source_name ,
    origin_entity_id::varchar(256) as origin_entity_id ,
    manualclassificationid::number(18,0) as manualclassificationid ,
    manualclassificationpath::varchar(256) as manualclassificationpath ,
    suggestedclassificationid::varchar(256) as suggestedclassificationid ,
    suggestedclassificationpath::varchar(256) as suggestedclassificationpath ,
    suggestedclassificationscore::number(15,10) as suggestedclassificationscore ,
    unique_id::varchar(256) as unique_id ,
    volume_unit::varchar(30) as volume_unit ,
    regional_franchise_group::varchar(50) as regional_franchise_group ,
    ean_upc::varchar(30) as ean_upc ,
    description::varchar(256) as description ,
    regional_franchise::varchar(100) as regional_franchise ,
    weight_unit::varchar(30) as weight_unit ,
    regional_product_hierarchy::varchar(100) as product_hierarchy ,
    -- emea_prodh3::varchar(50) as emea_prodh3 ,
    -- product::varchar(50) as product ,
    data_type::varchar(30) as data_type ,
    base_unit::varchar(30) as base_unit ,
    -- division::varchar(30) as division ,
    length::number(10,3) as length ,
    net_weight::number(10,3) as net_weight ,
    apac_variant::varchar(256) as apac_variant ,
    unit::varchar(30) as unit ,
    volume::number(10,3) as volume ,
    -- family::varchar(50) as family ,
    emea_gbpmgrc::varchar(100) as emea_gbpmgrc ,
    material_type::varchar(30) as material_type ,
    -- size_dimension::varchar(30) as size_dimension ,
    region::varchar(50) as region ,
    regional_subbrand::varchar(256) as regional_subbrand ,
    material_group::varchar(100) as material_group ,
    regional_megabrand::varchar(100) as regional_megabrand ,
    order_unit::varchar(30) as order_unit ,
    gross_weight::number(10,3) as gross_weight ,
    height::number(10,3) as height ,
    -- market::varchar(30) as market ,
    emea_gbpbgc::varchar(50) as emea_gbpbgc ,
    width::number(10,3) as width ,
    regional_brand::varchar(100) as regional_brand ,
    -- industry_sector::varchar(50) as industry_sector ,
    gcph_needstate_name::varchar(50) as gcph_needstate ,
    material_number::varchar(128) as material_number ,
    finalclassificationpath::varchar(256) as finalclassificationpath ,
    gcph_category_name::varchar(50) as gcph_category ,
    gcph_subcategory_name::varchar(50) as gcph_subcategory ,
    gcph_segment_name::varchar(50) as gcph_segment ,
    gcph_subsegment_name::varchar(100) as gcph_subsegment ,
    dateofextract::varchar(30) as dateofextract ,
    -- cdl_datetime::varchar(30) as cdl_datetime ,
    cdl_source_file::varchar(50) as cdl_source_file ,
    load_key::varchar(100) as load_key 
    from source
)

--Final select
select * from final