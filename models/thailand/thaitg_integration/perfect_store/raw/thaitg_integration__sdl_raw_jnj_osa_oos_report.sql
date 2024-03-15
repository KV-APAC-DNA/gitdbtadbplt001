with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_osa_oos_report') }}
),
final as(
    select * from source
)
select * from final