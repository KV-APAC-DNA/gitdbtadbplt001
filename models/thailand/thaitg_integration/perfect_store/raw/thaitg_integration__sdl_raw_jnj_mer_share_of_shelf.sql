with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_mer_share_of_shelf') }}
),
final as(
    select * from source
)
select * from final