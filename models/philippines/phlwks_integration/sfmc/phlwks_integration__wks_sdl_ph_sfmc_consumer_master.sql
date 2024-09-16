with 
source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_sfmc_consumer_master') }}
    where file_name not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_sfmc_consumer_master__test_null__ff')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_sfmc_consumer_master__test_duplicate__ff')}}
    )
)
select * from source