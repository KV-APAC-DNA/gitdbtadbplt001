{{
    config(
        materialized='incremental',
        incincremental_strategy='append'
    )
}}

with sdl_tw_pos_amart_purchase as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_amart_purchase') }}
    
),

transformed AS 
(
    SELECT * FROM sdl_tw_pos_amart_purchase
),
final as 
(
    select * from transformed
    

)   
select * from final