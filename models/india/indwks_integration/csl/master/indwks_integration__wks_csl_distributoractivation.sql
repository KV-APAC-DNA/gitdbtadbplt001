{{
    config
    (
        pre_hook= '{{build_itg_distributoractivation_temp()}}'
    )
}}

with sdl_csl_distributoractivation as 
(
    select * from {{source('indsdl_raw', 'sdl_csl_distributoractivation')}}
),
itg_distributoractivation as
(
    select * from {{ source('inditg_integration', 'itg_distributoractivation_temp') }}
),
transformed as
(
    SELECT src.distcode,
    activefromdate,
    activatedby,
    activatedon,
    inactivefromdate,
    inactivatedby,
    inactivatedon,
    activestatus,
    createddate,
    CASE 
      WHEN tgt.crt_dttm IS NULL
        THEN src.crt_dttm
      ELSE tgt.crt_dttm
      END AS crt_dttm,
    CASE 
      WHEN tgt.crt_dttm IS NULL
        THEN NULL
      ELSE getdate()
      END AS updt_dttm,
    CASE 
      WHEN tgt.crt_dttm IS NULL
        THEN 'I'
      ELSE 'U'
      END AS chng_flg FROM (
    SELECT DISTINCT *
    FROM SDL_CSL_DISTRIBUTORACTIVATION
    ) src LEFT OUTER JOIN (
    SELECT distcode,
      crt_dttm
    FROM ITG_DISTRIBUTORACTIVATION
    ) tgt ON src.distcode = tgt.distcode
),
final as 
(
    select
        distcode::varchar(400) as distcode,
        activefromdate::timestamp_ntz(9) as activefromdate,
        activatedby::number(18,0) as activatedby,
        activatedon::timestamp_ntz(9) as activatedon,
        inactivefromdate::timestamp_ntz(9) as inactivefromdate,
        inactivatedby::number(18,0) as inactivatedby,
        inactivatedon::timestamp_ntz(9) as inactivatedon,
        activestatus::number(18,0) as activestatus,
        createddate::timestamp_ntz(9) as createddate,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        chng_flg::varchar(1) as chng_flg
    from transformed
)
select * from final
