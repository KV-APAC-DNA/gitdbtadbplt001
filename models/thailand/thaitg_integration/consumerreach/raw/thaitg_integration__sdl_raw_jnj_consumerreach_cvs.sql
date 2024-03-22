with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_consumerreach_cvs') }}
),
final as(
    select * from source
)
select * from final