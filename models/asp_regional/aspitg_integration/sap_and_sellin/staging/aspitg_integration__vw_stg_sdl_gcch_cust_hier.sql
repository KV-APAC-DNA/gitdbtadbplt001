--Import CTE
with source as (
    select * from {{ source('mds_access', 'mds_gcch_gcgh_hierarchy') }}
),

--Logical CTE

--Final CTE
final as (
       select 
    coalesce(to_number(tamr_id),0)as tamr_id ,
    coalesce(origin_source_name::varchar(255),'') as origin_source_name ,
    coalesce(origin_entity_id::varchar(255),'') as origin_entity_id ,
    coalesce(to_number(manualclassificationid) ,0) as manualclassificationid ,
    coalesce(manualclassificationpath::varchar(255)  ,'') as manualclassificationpath ,
    coalesce(suggestedclassificationid::varchar(255)  ,'') as suggestedclassificationid ,
    coalesce(suggestedclassificationpath::varchar(255)  ,'')as suggestedclassificationpath ,
    coalesce(to_number(suggestedclassificationscore) ,0) as suggestedclassificationscore ,
    coalesce(finalclassificationpath::varchar(255)  ,'') as finalclassificationpath ,
    coalesce(unique_id::varchar(255)  ,'')as unique_id ,
    coalesce(region::varchar(50)  ,'')as region ,
    coalesce(customer::varchar(100)  ,'')as customer ,
    coalesce(country::varchar(30)  ,'')as country ,
    coalesce(name::varchar(512)  ,'')as name ,
    coalesce(city::varchar(256)  ,'')as city ,
    coalesce(postal_code::varchar(30)  ,'')as postal_code ,
    coalesce(state::varchar(30)  ,'')as state ,
    coalesce(district::varchar(100)  ,'')as district ,
    coalesce(search_term::varchar(128)  ,'')as search_term ,
    coalesce(regional_customer::varchar(128)  ,'')as regional_customer ,
    coalesce(regional_customer_code::varchar(50)  ,'')as regional_customer_code ,
    coalesce(regional_banner::varchar(50)  ,'')as regional_banner ,
    coalesce(regional_banner_code::varchar(50)  ,'')as regional_banner_code ,
    coalesce(regional_channel::varchar(50)  ,'')as regional_channel ,
    coalesce(regional_segmentation::varchar(50)  ,'')as regional_segmentation ,
    coalesce(regional_class_of_trade::varchar(50)  ,'')as regional_class_of_trade ,
    coalesce(emea_top_20_key_account::varchar(50)  ,'')as emea_top_20_key_account ,
    coalesce(apac_global_customer::varchar(50)  ,'')as apac_global_customer ,
    coalesce(apac_go_to_model::varchar(50)  ,'')as apac_go_to_model ,
    coalesce(apac_sub_channel::varchar(50)  ,'')as apac_sub_channel ,
    coalesce(apac_banner_in_format::varchar(50)  ,'')as apac_banner_in_format ,
    coalesce(latam_regional_customer_group::varchar(50)  ,'')as latam_regional_customer_group,
    coalesce(gcgh_region_name::varchar(50)  ,'')as gcgh_region ,
    coalesce(gcgh_cluster_name::varchar(50)  ,'')as gcgh_cluster ,
    coalesce(gcgh_subcluster_name::varchar(50)  ,'')as gcgh_subcluster ,
    coalesce(gcgh_market_name::varchar(50)  ,'')as gcgh_market ,
    coalesce(gcch_customer_name::varchar(500)  ,'')as gcch_customer ,
    coalesce(gcch_retail_banner_name::varchar(50)  ,'')as gcch_retail_banner ,
    coalesce(dateofextract::varchar(30)  ,'')as dateofextract ,
    null as cdl_datetime ,
    null as cdl_source_file ,
    null as load_key ,
    coalesce(gcch_primary_format_name::varchar(50)  ,'')as primary_format ,
    null as distributor_attribute 
 from source
  where region ilike 'apac'
  and _deleted_='F'
)

--Final select
select * from final