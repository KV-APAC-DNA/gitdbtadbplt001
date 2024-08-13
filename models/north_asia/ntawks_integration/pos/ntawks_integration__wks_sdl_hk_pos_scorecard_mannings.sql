with source as (
    select * from {{ source('ntasdl_raw', 'sdl_hk_pos_scorecard_mannings') }}
),
final as (
    select * from source
)
select * from final