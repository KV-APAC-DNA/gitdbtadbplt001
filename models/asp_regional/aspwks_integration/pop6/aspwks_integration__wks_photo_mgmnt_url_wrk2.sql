with sdl_photo_mgmnt_url_cnt as (
    select * from {{ ref('aspwks_integration__wks_photo_mgmnt_url_cnt') }}
),
itg_photo_mgmnt_url as (
    select * from {{ source('aspitg_integration', 'itg_photo_mgmnt_url_temp')}}
),
final as (
    select sdl.photo_key,
        sdl.response,
        sdl.url_cnt,
        sdl.run_id,
        sdl.change_flag
    from sdl_photo_mgmnt_url_cnt sdl,
        itg_photo_mgmnt_url itg
    where change_flag = 'U'
        and sdl.photo_key = itg.original_photo_key
        and sdl.response != itg.original_response
    union
    select sdl.photo_key,
        sdl.response,
        sdl.url_cnt,
        sdl.run_id,
        sdl.change_flag
    from sdl_photo_mgmnt_url_cnt sdl
    where change_flag = 'I'
)
select * from final