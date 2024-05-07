with source as 
(select * from {{ source('phlsdl_raw', 'sdl_ph_pos_robinsons') }}
),
final as (
    select * from source
)
select * from final