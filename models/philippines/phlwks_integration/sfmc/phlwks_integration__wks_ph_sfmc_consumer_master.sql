with 
source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_sfmc_consumer_master') }}
),
select * from source