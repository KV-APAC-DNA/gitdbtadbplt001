    --Import CTE
    with source as (
    select * from {{ source('mds_access', 'mds_gcph_category') }}
    ),

    --Logical CTE

    --Final CTE
    final as (
    select 
    coalesce(try_to_number(tamr_id),0) as tamr_id ,
    coalesce(origin_source_name::varchar(256),'') as origin_source_name ,
    coalesce(origin_entity_id::varchar(256),'') as origin_entity_id ,
    coalesce(try_to_number(manualclassificationid),0) as manualclassificationid ,
    coalesce(manualclassificationpath::varchar(256),'') as manualclassificationpath ,
    coalesce(suggestedclassificationid::varchar(256),'') as suggestedclassificationid ,
    coalesce(suggestedclassificationpath::varchar(256),'') as suggestedclassificationpath ,
    coalesce(try_to_number(suggestedclassificationscore),0) as suggestedclassificationscore ,
    coalesce(unique_id::varchar(256),'') as unique_id ,
    coalesce(volume_unit::varchar(30),'') as volume_unit ,
    coalesce(regional_franchise_group::varchar(50),'') as regional_franchise_group ,
    coalesce(ean_upc::varchar(30),'') as ean_upc ,
    coalesce(description::varchar(256),'') as description ,
    coalesce(regional_franchise::varchar(100),'') as regional_franchise ,
    coalesce(weight_unit::varchar(30),'') as weight_unit ,
    coalesce(regional_product_hierarchy::varchar(100),'') as product_hierarchy ,
    null as emea_prodh3 ,
    null as product ,
    coalesce(data_type::varchar(30),'') as data_type ,
    coalesce(base_unit::varchar(30),'') as base_unit ,
    null as division ,
    coalesce(try_to_number(length),0) as length ,
    coalesce(try_to_number(net_weight),0) as net_weight ,
    coalesce(apac_variant::varchar(256),'') as apac_variant ,
    coalesce(unit::varchar(30),'') as unit ,
    coalesce(try_to_number(volume),0) as volume ,
    null as family ,
    coalesce(emea_gbpmgrc::varchar(100),'') as emea_gbpmgrc ,
    coalesce(material_type::varchar(30),'') as material_type ,
    null as size_dimension ,
    coalesce(region::varchar(50),'') as region ,
    coalesce(regional_subbrand::varchar(256),'') as regional_subbrand ,
    coalesce(material_group::varchar(100),'') as material_group ,
    coalesce(regional_megabrand::varchar(100),'') as regional_megabrand ,
    coalesce(order_unit::varchar(30),'') as order_unit ,
    coalesce(try_to_number(gross_weight),0) as gross_weight ,
    coalesce(try_to_number(height),0) as height ,
    null as market ,
    coalesce(emea_gbpbgc::varchar(50),'') as emea_gbpbgc ,
    coalesce(try_to_number(width),0) as width ,
    coalesce(regional_brand::varchar(100),'') as regional_brand ,
    null as industry_sector ,
    coalesce(gcph_needstate_name::varchar(500),'') as gcph_needstate ,
    coalesce(material_number::varchar(5000),'') as material_number ,
    coalesce(finalclassificationpath::varchar(256),'') as finalclassificationpath ,
    coalesce(gcph_category_name::varchar(50),'') as gcph_category ,
    coalesce(gcph_subcategory_name::varchar(50),'') as gcph_subcategory ,
    coalesce(gcph_segment_name::varchar(50),'') as gcph_segment ,
    coalesce(gcph_subsegment_name::varchar(100),'') as gcph_subsegment ,
    coalesce(dateofextract::varchar(30),'') as dateofextract ,
    null as cdl_datetime ,
    null as cdl_source_file ,
    null as load_key 
    from source where region ilike 'apac'
    and _deleted_='F'
    )

    --Final select
    select * from final