with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_mer_cop') }}
),
final as(
    select * from source
)
select * from final