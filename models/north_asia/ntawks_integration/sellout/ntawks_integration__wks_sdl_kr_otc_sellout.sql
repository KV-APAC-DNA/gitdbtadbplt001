with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_otc_sellout') }}
),
itg_kr_gt_sellout as (
    select * from {{ source('ntaitg_integration', 'itg_kr_gt_sellout') }}
),
final as (
    select * from source
    where to_timestamp(SPLIT_PART(SPLIT_PART(source.FILE_NAME,'_',4),'.',1),'YYYYMMDDHH24MISS')>(select max(updt_dttm) from itg_kr_gt_sellout where dstr_cd='OTC')
)
select * from final