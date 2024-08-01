with wks_edw_hcp360_sales_cube_details as(
    select * from {{ ref('hcpwks_integration__wks_edw_hcp360_sales_cube_details') }}
),
itg_mds_in_hcp_sales_hierarchy_mapping as(
    select * from {{ ref('hcpitg_integration__itg_mds_in_hcp_sales_hierarchy_mapping') }}
),
final as(
    SELECT s.day,
       s.mth_mm,
       CASE
         WHEN s.brand_name = 'ORSL' THEN 'ORSL'
         WHEN s.brand_name = 'Johnson''s Baby' THEN 'JBABY'
         WHEN s.brand_name = 'AVEENO Baby' THEN 'DERMA'
         WHEN s.brand_name = 'AVEENO BODY' THEN 'DERMA'
		 WHEN s.brand_name in ('Johnson\`s Cottontouch','Johnson''s Cottontouch') THEN 'JBABY'
       END AS Brand,
	  s.variant_name, 
      s.customer_code ,
      s.customer_name ,
      
      case when s.channel_name = 'Wholesale' and s.retailer_category_cd = 'Pharma' then 'Wholesale-Pharma'
           when s.channel_name = 'Wholesale' and s.retailer_category_cd != 'Pharma' then 'Wholesale-Non-Pharma'
           when s.channel_name = 'GT' and s.retailer_category_cd = 'CH' then 'GT-Chemist'     
           when s.channel_name = 'GT' and s.retailer_category_cd != 'CH' then 'GT-Others'     
           else s.channel_name end as channel_name  ,
          m.region_code ,
          m.zone_code ,
          m.sales_area_code ,
          s.UDC_AvBabyBodyDocQ42019,
	      s.udc_babyprofesionalcac2019, 
          sum (s.achievement_nr ) achievement_nr
from           
wks_edw_hcp360_sales_cube_details s
left outer join
itg_mds_in_hcp_sales_hierarchy_mapping m
on s.customer_code = m.rds_code :: varchar
and (CASE
         WHEN s.brand_name = 'ORSL' THEN 'ORSL'
         WHEN s.brand_name = 'Johnson''s Baby' THEN 'JBABY'
         WHEN s.brand_name = 'AVEENO Baby' THEN 'DERMA'
         WHEN s.brand_name = 'AVEENO BODY' THEN 'DERMA'
		 WHEN s.brand_name in ('Johnson\`s Cottontouch','Johnson''s Cottontouch') THEN 'JBABY'
       END    ) = m.brand_name_code
 group by 
day,mth_mm,Brand,variant_name,customer_code,customer_name,7,region_code,zone_code,sales_area_code,UDC_AvBabyBodyDocQ42019,udc_babyprofesionalcac2019
)
select * from final



