with 
source as
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_general_audits') }}
    where file_name not in (
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_general_audits__null_test') }}
        union all
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_general_audits__duplicate_test') }}
    )
),
final as
(
    select * from source
)
select * from final