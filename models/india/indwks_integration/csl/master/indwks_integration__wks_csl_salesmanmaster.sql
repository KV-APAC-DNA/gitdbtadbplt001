{{
    config
    (
        pre_hook= '{{build_itg_salesmanmaster_temp()}}'
    )
}}

with sdl_csl_salesmanmaster as 
(
    select * from {{source('indsdl_raw', 'sdl_csl_salesmanmaster')}}
),
itg_salesmanmaster_temp as
(
    select * from {{ source('inditg_integration', 'itg_salesmanmaster_temp') }}
),
transformed as 
(
    SELECT
    src.distcode,
    smid,
    src.smcode,
    src.smname,
    smphoneno,
    smemail,
    smotherdetails,
    smdailyallowance,
    smmonthlysalary,
    smmktcredit,
    smcreditdays,
    STATUS,
    rmid,
    src.rmcode,
    src.rmname,
    uploadflag,
    createddate,
    syncid,
    rdssmtype,
    src.uniquesalescode,
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
      smid,
      smcode,
      smname,
      smphoneno,
      smemail,
      smotherdetails,
      smdailyallowance,
      smmonthlysalary,
      smmktcredit,
      smcreditdays,
      STATUS,
      rmid,
      rmcode,
      rmname,
      uploadflag,
      createddate,
      syncid,
      rdssmtype,
      uniquesalescode,
      crt_dttm
    FROM (
      SELECT distcode,
        smid,
        smcode,
        smname,
        smphoneno,
        smemail,
        smotherdetails,
        smdailyallowance,
        smmonthlysalary,
        smmktcredit,
        smcreditdays,
        STATUS,
        rmid,
        rmcode,
        rmname,
        uploadflag,
        createddate,
        syncid,
        rdssmtype,
        uniquesalescode,
        crt_dttm,
        ROW_NUMBER() OVER (
          PARTITION BY distcode,
          smcode,
          smname,
          rmname,
          rmcode ORDER BY createddate DESC
          ) AS rn
      FROM SDL_CSL_SALESMANMASTER a
      ) b
    WHERE b.rn = 1
    ) src LEFT OUTER JOIN (
    SELECT distcode,
      smcode,
      smname,
      rmname,
      rmcode,
      uniquesalescode,
      crt_dttm
    FROM ITG_SALESMANMASTER_TEMP
    ) tgt ON src.distcode = tgt.distcode
    AND src.smcode = tgt.smcode
    AND src.smname = tgt.smname
    AND COALESCE(src.rmname, 'NA') = COALESCE(tgt.rmname, 'NA')
    AND COALESCE(src.rmcode, 'NA') = COALESCE(tgt.rmcode, 'NA')
),
final as 
(
    select
        distcode::varchar(100) as distcode,
        smid::number(18,0) as smid,
        smcode::varchar(100) as smcode,
        smname::varchar(100) as smname,
        smphoneno::varchar(100) as smphoneno,
        smemail::varchar(100) as smemail,
        smotherdetails::varchar(500) as smotherdetails,
        smdailyallowance::number(38,6) as smdailyallowance,
        smmonthlysalary::number(38,6) as smmonthlysalary,
        smmktcredit::number(38,6) as smmktcredit,
        smcreditdays::number(18,0) as smcreditdays,
        status::varchar(20) as status,
        rmid::number(18,0) as rmid,
        rmcode::varchar(100) as rmcode,
        rmname::varchar(100) as rmname,
        uploadflag::varchar(1) as uploadflag,
        createddate::timestamp_ntz(9) as createddate,
        syncid::number(38,0) as syncid,
        rdssmtype::varchar(100) as rdssmtype,
        uniquesalescode::varchar(15) as uniquesalescode,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        chng_flg::varchar(1) as chng_flg
    from transformed
)
select * from final
