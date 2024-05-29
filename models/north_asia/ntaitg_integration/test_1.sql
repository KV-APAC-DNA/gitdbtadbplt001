with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_target') }}
),
final as
(
    select * from source
)
select * from final