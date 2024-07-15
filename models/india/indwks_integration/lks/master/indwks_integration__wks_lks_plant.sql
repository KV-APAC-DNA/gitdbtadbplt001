with sdl_lks_plant as
(
    select * from {{ ref('indwks_integration__wks_sdl_lks_plant') }}
),
itg_plant as 
(
    select * from {{ source('inditg_integration', 'itg_plant') }}
),
final as 
(
    SELECT

    COALESCE(src.plantcode,tgt.plantcode) as plantcode,
    COALESCE(src.plantid,tgt.plantid) as plantid,
    COALESCE(src.plantname,tgt.plantname) as plantname,
    COALESCE(src.shortname,tgt.shortname) as shortname,
    COALESCE(src.name2,tgt.name2) as name2,
    COALESCE(src.statecode,tgt.statecode) as statecode,
    COALESCE(src.active,tgt.active) as active,
    COALESCE(src.createdby,tgt.createdby) as createdby,
    COALESCE(src.suppliercode,tgt.suppliercode) as suppliercode,
    CASE
    WHEN tgt.crt_dttm IS NULL THEN src.crt_dttm
    ELSE tgt.crt_dttm
    END AS crt_dttm ,

    CASE
    WHEN tgt.crt_dttm IS NULL THEN NULL
    ELSE convert_timezone('UTC',current_timestamp())::timestamp_ntz
    END AS updt_dttm,

    CASE
    WHEN tgt.crt_dttm IS NULL THEN 'I'
    ELSE 'U'
    END AS chng_flg
    FROM sdl_lks_plant src
    LEFT JOIN itg_plant tgt ON src.plantcode = tgt.plantcode and src.plantid = tgt.plantid
)

select * from final