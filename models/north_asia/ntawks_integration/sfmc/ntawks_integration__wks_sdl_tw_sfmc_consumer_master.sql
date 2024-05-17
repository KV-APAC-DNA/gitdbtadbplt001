with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_tw_sfmc_consumer_master') }}
)
select * from source