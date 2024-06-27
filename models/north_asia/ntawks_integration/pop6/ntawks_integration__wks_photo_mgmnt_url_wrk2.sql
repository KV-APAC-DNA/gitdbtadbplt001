with sdl_photo_mgmnt_url_cnt as (
    -- select * from {{ ref('ntawks_integration__wks_photo_mgmnt_url_cnt') }}
    select * from dev_dna_load.snapntasdl_raw.sdl_photo_mgmnt_url_cnt
),
itg_photo_mgmnt_url as (
    select * from ntaitg_integration.itg_photo_mgmnt_url
    -- select * from {{ source('ntaitg_integration', 'itg_photo_mgmnt_url_temp')}}
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