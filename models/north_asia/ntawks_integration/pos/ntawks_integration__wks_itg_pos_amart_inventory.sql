with sdl_tw_pos_amart_inventory as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_amart_inventory') }}
    
),

transformed AS 
(
    SELECT * FROM sdl_tw_pos_amart_inventory
),
final as 
(
    select * from transformed
    

)   
select * from final