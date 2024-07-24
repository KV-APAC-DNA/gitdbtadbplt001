with edw_rpt_sales_details as(
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}
),
final as(
    select brand_name,
       day,
       mth_mm,
       cal_yr,
       fisc_yr,
       achievement_nr,
       customer_code,
       customer_name,    
       product_code,
       product_name,
       product_desc,
       variant_name ,
       franchise_name,
       product_category_name,
       channel_name,
       business_channel,
       retailer_code,     
       retailer_name,
       retailer_category_cd,
       retailer_category_name,
	   udc_avbabybodydocq42019,  
	   udc_babyprofesionalcac2019 
from edw_rpt_sales_details
where brand_name in ('AVEENO Baby' ,'AVEENO BODY' , 'Johnson''s Baby' , 'ORSL' ,'Johnson\`s Cottontouch','Johnson''s Cottontouch')
)
select * from final