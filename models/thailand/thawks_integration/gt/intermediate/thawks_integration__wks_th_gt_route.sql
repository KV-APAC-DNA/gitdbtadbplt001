with source as (
    select * from {{ source('thasdl_raw', 'sdl_th_gt_route') }}
)

select * from source