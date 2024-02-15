with source as
(
    select  * from {{ source('myssdl_raw','sdl_so_inv_133985') }}
),

final as (
select 
distributor_id,
date, 			
distributor_wh_id  ,         
sap_material_id   ,          
product_code,                
product_ean_code ,           
product_description ,   
replace (quantity_available,',','') as quantity_available,     
uom_available ,    
replace (quantity_on_order,',','') as quantity_on_order,                    
uom_on_order ,        
replace (quantity_committed,',','') as quantity_committed,            
uom_committed ,         
replace (quantity_available_in_pieces,',','') as quantity_available_in_pieces,     
replace (quantity_on_order_in_pieces,',','')  as quantity_on_order_in_pieces,     
replace (quantity_committed_in_pieces,',','') as quantity_committed_in_pieces,    
replace (unit_price,',','') as unit_price,     
replace (total_value_available,',','') as total_value_available,     
custom_field_1 ,             
custom_field_2 ,
file_name
from source
)

select * from final