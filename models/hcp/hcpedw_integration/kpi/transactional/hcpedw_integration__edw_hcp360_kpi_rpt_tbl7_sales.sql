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
    select * from {{ ref('hcpitg_integration__itg_mds_in_hcp_sales_hierarchy_mapping') }}
),
wks_edw_hcp360_sales_cube_details as 
(
    select * from {{ ref('hcpwks_integration__wks_edw_hcp360_sales_cube_details') }}
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
         WHEN rtrim(sales.brand_name)  = 'ORSL' THEN 'ORSL'
         WHEN rtrim(sales.brand_name)  = 'Johnson''s Baby' THEN 'JBABY'
         WHEN rtrim(sales.brand_name)  = 'AVEENO Baby' THEN 'DERMA'
         WHEN rtrim(sales.brand_name ) = 'AVEENO BODY' THEN 'DERMA'
       END AS Brand
       ,rtrim(sales.customer_code) as customer_code
       ,rtrim(sales.customer_name)  as customer_name
       ,rtrim(sales.retailer_code) as retailer_code
       ,rtrim(sales.retailer_name) as retailer_name
       ,rtrim(sales.retailer_category_cd) as retailer_category_cd
       ,rtrim(sales.retailer_category_name) as retailer_category_name
	   ,(sales.customer_code || '-' || sales.retailer_code) AS  num_buying_retailer
	   ,nvl (map.region_code ,'Non-Covered Area') AS region
       ,nvl (map.zone_code ,'Non-Covered Area') AS zone
      , nvl (map.sales_area_code ,'Non-Covered Area') AS sales_area
      , SUM(achievement_nr) AS sales_value	   

        from           
        wks_edw_hcp360_sales_cube_details sales
        left outer join
        itg_mds_in_hcp_sales_hierarchy_mapping map
        on rtrim(sales.customer_code) = rtrim(map.rds_code) :: varchar
        and (CASE
                WHEN rtrim(sales.brand_name) = 'ORSL' THEN 'ORSL'
                WHEN rtrim(sales.brand_name) = 'Johnson''s Baby' THEN 'JBABY'
                WHEN rtrim(sales.brand_name) = 'AVEENO Baby' THEN 'DERMA'
                WHEN rtrim(sales.brand_name) = 'AVEENO BODY' THEN 'DERMA'
            END    ) = rtrim(map.brand_name_code)
        group by  
        mth_mm	
        ,brand
        ,rtrim(customer_code)
        ,rtrim(customer_name)
        ,rtrim(retailer_code)
        ,rtrim(retailer_name)
        ,rtrim(retailer_category_cd)
        ,rtrim(retailer_category_name)
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
        ,updt_dttm 
    from tempb
)
select * from final