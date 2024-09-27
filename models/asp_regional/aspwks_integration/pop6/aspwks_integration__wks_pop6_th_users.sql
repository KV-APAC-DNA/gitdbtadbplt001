with sdl_pop6_th_users as 
(
<<<<<<< HEAD
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_users_test') }}
=======
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_users') }}
    where file_name not in (
        select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_users__null_test') }}
        union all
        select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_users__duplicate_test') }}
    )
>>>>>>> e00bf79505862c0d02306f4fc64454e4a04f93bb
),
final as (
    select * from sdl_pop6_th_users
)
select * from final