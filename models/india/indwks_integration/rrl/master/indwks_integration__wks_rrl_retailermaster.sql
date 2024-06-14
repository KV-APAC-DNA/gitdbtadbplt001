{{
    config(
        pre_hook="{{built_itg_rrl_retailermaster()}}"  
    )
}}
with itg_rrl_retailermaster as
(
    select * from  {{ source('inditg_integration', 'itg_rrl_retailermaster') }}
),

wks_tmp_date as
(
    select * from  {{ source('indwks_integration', 'wks_tmp_date') }}
),
sdl_rrl_retailermaster as
(
    select * from  {{ source('indsdl_raw', 'sdl_rrl_retailermaster') }}
),

transformed as(
    select
    src.retailercode,
    src.retailername src_retailername,
    tgt.retailername tgt_retailername,
    routecode,
    retailerclasscode,
    src.villagecode,
    src.rsdcode,
    src.distributorcode,
    foodlicenseno,
    druglicenseno,
    address,
    phone,
    mobile,
    prcontact,
    seccontact,
    creditlimit,
    creditperiod,
    invoicelimit,
    isapproved,
    isactive,
    src.rsrcode,
    drugvaliditydate,
    fssaivaliditydate,
    src.displaystatus src_displaystatus,
    tgt.displaystatus tgt_displaystatus,
    createddate,
    ownername,
    druglicenseno2,
    r_statecode,
    r_districtcode,
    r_tahsilcode,
    address1,
    address2,
    retailerchannelcode,
    retailerclassid,
    filename,
    CASE 
      WHEN tgt.crt_dttm IS NULL
        THEN tmpdt.currtime
      WHEN tgt.crt_dttm IS NOT NULL
        AND (
          src.retailername <> tgt.retailername
          OR src.displaystatus <> tgt.displaystatus
          )
        THEN tgt.crt_dttm
      ELSE tgt.crt_dttm
      END AS crt_dttm,
    CASE 
      WHEN tgt.crt_dttm IS NULL
        THEN NULL
      WHEN tgt.crt_dttm IS NOT NULL
        AND (
          src.retailername <> tgt.retailername
          OR src.displaystatus <> tgt.displaystatus
          )
        THEN tmpdt.currtime
      ELSE tmpdt.currtime
      END AS updt_dttm,
    CASE 
      WHEN tgt.crt_dttm IS NULL
        THEN 'I'
      WHEN tgt.crt_dttm IS NOT NULL
        AND (
          src.retailername <> tgt.retailername
          OR src.displaystatus <> tgt.displaystatus
          )
        THEN 'U2'
      ELSE 'U'
      END AS chng_flg,
    CASE 
      WHEN tgt.crt_dttm IS NULL
        THEN 'Y'
      WHEN tgt.crt_dttm IS NOT NULL
        AND (
          src.retailername <> tgt.retailername
          OR src.displaystatus <> tgt.displaystatus
          )
        THEN 'N'
      ELSE 'Y'
      END AS actv_flg FROM (
    SELECT retailercode,
      retailername,
      routecode,
      retailerclasscode,
      villagecode,
      rsdcode,
      distributorcode,
      foodlicenseno,
      druglicenseno,
      address,
      phone,
      mobile,
      prcontact,
      seccontact,
      creditlimit,
      creditperiod,
      invoicelimit,
      isapproved,
      isactive,
      rsrcode,
      drugvaliditydate,
      fssaivaliditydate,
      displaystatus,
      createddate,
      ownername,
      druglicenseno2,
      r_statecode,
      r_districtcode,
      r_tahsilcode,
      address1,
      address2,
      retailerchannelcode,
      retailerclassid,
      filename
    FROM (
      SELECT retailercode,
        retailername,
        routecode,
        retailerclasscode,
        villagecode,
        rsdcode,
        distributorcode,
        foodlicenseno,
        druglicenseno,
        address,
        phone,
        mobile,
        prcontact,
        seccontact,
        creditlimit,
        creditperiod,
        invoicelimit,
        isapproved,
        isactive,
        rsrcode,
        drugvaliditydate,
        fssaivaliditydate,
        displaystatus,
        createddate,
        ownername,
        druglicenseno2,
        r_statecode,
        r_districtcode,
        r_tahsilcode,
        address1,
        address2,
        retailerchannelcode,
        retailerclassid,
        filename,
        crt_dttm,
        row_number() OVER (
          PARTITION BY upper(distributorcode),
          upper(retailercode),
          upper(rsdcode),
          upper(villagecode),
          upper(rsrcode) ORDER BY createddate DESC,
            displaystatus,
            retailername
          ) AS rn
      FROM sdl_rrl_retailermaster a
      WHERE isactive = true
    
      ) b
    WHERE b.rn = 1
    ) src LEFT OUTER JOIN (
    SELECT distributorcode,
      retailercode,
      rsdcode,
      retailername,
      villagecode,
      rsrcode,
      displaystatus,
      actv_flg,
      crt_dttm
    FROM itg_rrl_retailermaster
    ) tgt ON upper(src.distributorcode) = upper(tgt.distributorcode)
    AND upper(src.retailercode) = upper(tgt.retailercode)
    AND upper(src.rsdcode) = upper(tgt.rsdcode)
    AND upper(src.villagecode) = upper(tgt.villagecode)
    AND upper(src.rsrcode) = upper(tgt.rsrcode)
    AND tgt.actv_flg = 'Y'
    CROSS JOIN wks_tmp_date tmpdt

),

final as
(
    select
    retailercode::varchar(50) as retailercode,
    src_retailername::varchar(100) as src_retailername,
    tgt_retailername::varchar(100) as tgt_retailername,
    routecode::varchar(25) as routecode,
    retailerclasscode::varchar(50) as retailerclasscode,
    villagecode::varchar(50) as villagecode,
    rsdcode::varchar(50) as rsdcode,
    distributorcode::varchar(50) as distributorcode,
    foodlicenseno::varchar(50) as foodlicenseno,
    druglicenseno::varchar(50) as druglicenseno,
    address::varchar(100) as address,
    phone::varchar(15) as phone,
    mobile::varchar(15) as mobile,
    prcontact::varchar(50) as prcontact,
    seccontact::varchar(50) as seccontact,
    creditlimit::number(18,0) as creditlimit,
    creditperiod::number(18,0) as creditperiod,
    invoicelimit::varchar(30) as invoicelimit,
    isapproved::varchar(1) as isapproved,
    isactive::boolean as isactive,
    rsrcode::varchar(100) as rsrcode,
    drugvaliditydate::timestamp_ntz(9) as drugvaliditydate,
    fssaivaliditydate::timestamp_ntz(9) as fssaivaliditydate,
    src_displaystatus::varchar(20) as src_displaystatus,
    tgt_displaystatus::varchar(20) as tgt_displaystatus,
    createddate::timestamp_ntz(9) as createddate,
    ownername::varchar(100) as ownername,
    druglicenseno2::varchar(50) as druglicenseno2,
    r_statecode::number(18,0) as r_statecode,
    r_districtcode::number(18,0) as r_districtcode,
    r_tahsilcode::number(18,0) as r_tahsilcode,
    address1::varchar(100) as address1,
    address2::varchar(100) as address2,
    retailerchannelcode::varchar(40) as retailerchannelcode,
    retailerclassid::number(18,0) as retailerclassid,
    filename::varchar(100) as filename,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    chng_flg::varchar(2) as chng_flg,
    actv_flg::varchar(1) as actv_flg

  from transformed
)
select * from final
