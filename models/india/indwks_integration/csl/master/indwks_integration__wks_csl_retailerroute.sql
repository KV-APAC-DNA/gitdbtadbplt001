{{
    config
    (
        pre_hook= '{{build_itg_retailerroute_temp()}}'
    )
}}

with sdl_csl_retailerroute as 
(
    select * from {{source('indsdl_raw', 'sdl_csl_retailerroute')}}
),
itg_retailerroute as 
(
    select * from {{ source('inditg_integration', 'itg_retailerroute') }}
),
transformed as
(
    SELECT src.distcode,
    src.rtrcode,
    src.rtrname,
    src.rmcode,
    src.rmname,
    routetype,
    uploadflag,
    createddate,
    syncid,
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
    SELECT distcode,
      rtrcode,
      rtrname,
      rmcode,
      rmname,
      routetype,
      uploadflag,
      createddate,
      syncid,
      crt_dttm
    FROM (
      SELECT distcode,
        rtrcode,
        rtrname,
        rmcode,
        rmname,
        routetype,
        uploadflag,
        createddate,
        syncid,
        crt_dttm,
        ROW_NUMBER() OVER (
          PARTITION BY distcode,
          rtrcode,
          rtrname,
          rmcode,
          rmname ORDER BY createddate DESC
          ) AS rn
      FROM SDL_CSL_RETAILERROUTE a
      WHERE UPPER(routetype) = 'S'
      ) b
    WHERE b.rn = 1
    ) src LEFT OUTER JOIN (
    SELECT distcode,
      rtrcode,
      rtrname,
      rmcode,
      rmname,
      crt_dttm
    FROM itg_retailerroute
    ) tgt ON src.distcode = tgt.distcode
    AND src.rtrcode = tgt.rtrcode
    AND COALESCE(src.rtrname, 'NA') = COALESCE(tgt.rtrname, 'NA')
    AND src.rmcode = tgt.rmcode
    AND COALESCE(src.rmname, 'NA') = COALESCE(tgt.rmname, 'NA')
),
final as 
(
    select
        distcode::varchar(100) as distcode,
        rtrcode::varchar(100) as rtrcode,
        rtrname::varchar(100) as rtrname,
        rmcode::varchar(100) as rmcode,
        rmname::varchar(100) as rmname,
        routetype::varchar(100) as routetype,
        uploadflag::varchar(10) as uploadflag,
        createddate::timestamp_ntz(9) as createddate,
        syncid::number(38,0) as syncid,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        chng_flg::varchar(1) as chng_flg
    from transformed
)
select * from final
