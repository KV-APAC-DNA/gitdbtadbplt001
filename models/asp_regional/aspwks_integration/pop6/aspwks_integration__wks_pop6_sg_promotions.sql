with sdl_pop6_sg_promotions as 
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_promotions') }}
    where file_name not in (
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_promotions__duplicate_test') }}
        union all
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_promotions__null_test') }}
    )
),
final as (
    select * from sdl_pop6_sg_promotions
)
select * from final