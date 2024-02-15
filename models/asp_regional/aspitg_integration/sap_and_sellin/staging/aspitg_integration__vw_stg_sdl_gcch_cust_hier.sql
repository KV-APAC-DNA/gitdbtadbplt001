--Import CTE
with source as (
    select * from {{ source('mds_access', 'mds_gcch_gcgh_hierarchy') }}
),

--Logical CTE

--Final CTE
final as (
    select 
    tamr_id::number(18,0) as tamr_id ,
    origin_source_name::varchar(255) as origin_source_name ,
    origin_entity_id::varchar(255) as origin_entity_id ,
    manualclassificationid::number(18,0) as manualclassificationid ,
    manualclassificationpath::varchar(255) as manualclassificationpath ,
    suggestedclassificationid::varchar(255) as suggestedclassificationid ,
    suggestedclassificationpath::varchar(255) as suggestedclassificationpath ,
    suggestedclassificationscore::number(15,10) as suggestedclassificationscore ,
    finalclassificationpath::varchar(255) as finalclassificationpath ,
    unique_id::varchar(255) as unique_id ,
    region::varchar(50) as region ,
    customer::varchar(100) as customer ,
    country::varchar(30) as country ,
    name::varchar(512) as name ,
    city::varchar(256) as city ,
    postal_code::varchar(30) as postal_code ,
    state::varchar(30) as state ,
    district::varchar(100) as district ,
    search_term::varchar(128) as search_term ,
    regional_customer::varchar(128) as regional_customer ,
    regional_customer_code::varchar(50) as regional_customer_code ,
    regional_banner::varchar(50) as regional_banner ,
    regional_banner_code::varchar(50) as regional_banner_code ,
    regional_channel::varchar(50) as regional_channel ,
    regional_segmentation::varchar(50) as regional_segmentation ,
    regional_class_of_trade::varchar(50) as regional_class_of_trade ,
    emea_top_20_key_account::varchar(50) as emea_top_20_key_account ,
    apac_global_customer::varchar(50) as apac_global_customer ,
    apac_go_to_model::varchar(50) as apac_go_to_model ,
    apac_sub_channel::varchar(50) as apac_sub_channel ,
    apac_banner_in_format::varchar(50) as apac_banner_in_format ,
    latam_regional_customer_group::varchar(50) as latam_regional_customer_group ,
    gcgh_region_name::varchar(50) as gcgh_region ,
    gcgh_cluster_name::varchar(50) as gcgh_cluster ,
    gcgh_subcluster_name::varchar(50) as gcgh_subcluster ,
    gcgh_market_name::varchar(50) as gcgh_market ,
    gcch_customer_name::varchar(50) as gcch_customer ,
    gcch_retail_banner_name::varchar(50) as gcch_retail_banner ,
    dateofextract::varchar(30) as dateofextract ,
    -- cdl_datetime::varchar(30) as cdl_datetime ,
    -- cdl_source_file::varchar(50) as cdl_source_file ,
    -- load_key::varchar(128) as load_key ,
    gcch_primary_format_name::varchar(50) as primary_format 
    -- distributor_attribute::varchar(50) as distributor_attribute 
    from source
)

--Final select
select * from final