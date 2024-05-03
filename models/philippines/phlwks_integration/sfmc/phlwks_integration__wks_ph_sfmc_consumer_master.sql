with 
source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_sfmc_consumer_master') }}
),

final as
(
    select * from source
)

select * from final