{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} WHERE country = 'IN' AND source_system = 'SALES_CUBE' AND activity_type = 'SALES_ANALYSIS_NOCB' ;
        {% endif %}"
    )
}}
with 
itg_mds_in_hcp_sales_hierarchy_mapping as 
(
    select * from snapinditg_integration.itg_mds_in_hcp_sales_hierarchy_mapping
),
wks_edw_hcp360_sales_cube_details as 
(
    select * from snapindwks_integration.wks_edw_hcp360_sales_cube_details
),

tempb as 
(
    SELECT 'IN' AS country
       ,'SALES_CUBE' AS source_system,
       null as channel
       ,'SALES_ANALYSIS_NOCB' AS activity_type
    ,current_timestamp() as crt_dttm
    ,current_timestamp() as updt_dttm
       ,sales.mth_mm as year_month
       ,CASE
         WHEN sales.brand_name = 'ORSL' THEN 'ORSL'
         WHEN sales.brand_name = 'Johnson''s Baby' THEN 'JBABY'
         WHEN sales.brand_name = 'AVEENO Baby' THEN 'DERMA'
         WHEN sales.brand_name = 'AVEENO BODY' THEN 'DERMA'
       END AS Brand
       ,sales.customer_code 
       ,sales.customer_name     
       ,sales.retailer_code
       ,sales.retailer_name
       ,sales.retailer_category_cd
       ,sales.retailer_category_name	
	   ,(sales.customer_code || '-' || sales.retailer_code) AS  num_buying_retailer
	   ,nvl (map.region_code ,'Non-Covered Area') AS region
       ,nvl (map.zone_code ,'Non-Covered Area') AS zone
      , nvl (map.sales_area_code ,'Non-Covered Area') AS sales_area
      , SUM(achievement_nr) AS sales_value	   

        from           
        wks_edw_hcp360_sales_cube_details sales
        left outer join
        itg_mds_in_hcp_sales_hierarchy_mapping map
        on sales.customer_code = map.rds_code :: varchar
        and (CASE
                WHEN sales.brand_name = 'ORSL' THEN 'ORSL'
                WHEN sales.brand_name = 'Johnson''s Baby' THEN 'JBABY'
                WHEN sales.brand_name = 'AVEENO Baby' THEN 'DERMA'
                WHEN sales.brand_name = 'AVEENO BODY' THEN 'DERMA'
            END    ) = map.brand_name_code
        group by  mth_mm	
        ,brand	
        ,customer_code	
        ,customer_name	
        ,retailer_code	
        ,retailer_name	
        ,retailer_category_cd	
        ,retailer_category_name
        ,(sales.customer_code || '-' || sales.retailer_code)	
        ,region_code	
        ,zone_code	
        ,sales_area_code
),
final as 
(
    select 
     country
    ,source_system
    ,activity_type
    ,year_month
    ,brand
    ,customer_code
    ,customer_name
    ,retailer_code
    ,retailer_name
    ,retailer_category_cd
    ,retailer_category_name
    ,num_buying_retailer
    ,region
    ,zone
    ,sales_area
    ,sales_value
    ,crt_dttm
    ,updt_dttm from 
tempb
)
select * from final