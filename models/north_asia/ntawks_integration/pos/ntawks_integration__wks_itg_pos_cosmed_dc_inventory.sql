{{
    config(
        materialized='incremental',
        incincremental_strategy='append'
    )
}}

with sdl_tw_pos_cosmed_dc_inventory as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_cosmed_dc_inventory') }}
    
),

transformed AS 
(
    SELECT * FROM sdl_tw_pos_cosmed_dc_inventory
),
final as 
(
    select * from transformed
    

)   
select * from final