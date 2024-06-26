with 
itg_udcdetails as 
(
    select * from {{ ref('inditg_integration__itg_udcdetails') }}
),
itg_udcmaster as 
(
    select * from {{ ref('inditg_integration__itg_udcmaster') }}
),
edw_retailer_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
final as 
(
    SELECT udc_1.distcode,
       udc_1.mastervaluecode AS retailer_code,
       udc_1.mastervaluename,
       udc_1.columnname,
       udc_1.columnvalue AS gtm_flag,
       udc_1.rn,
       udc_1.rtruniquecode AS urc
FROM (SELECT itg_udcdetails.distcode,
             itg_udcdetails.mastervaluecode,
             UPPER(itg_udcdetails.mastervaluename::TEXT) AS mastervaluename,
             itg_udcdetails.columnname,
             CASE
               WHEN itg_udcdetails.columnvalue IS NULL OR BTRIM(itg_udcdetails.columnvalue::TEXT) = ''::CHARACTER VARYING::TEXT THEN NULL::CHARACTER VARYING::TEXT
               ELSE UPPER(itg_udcdetails.columnvalue::TEXT)
             END AS columnvalue,
             pg_catalog.row_number() OVER (PARTITION BY itg_udcdetails.distcode,itg_udcdetails.mastervaluecode,itg_udcdetails.columnname ORDER BY itg_udcdetails.createddate DESC,itg_udcdetails.columnvalue DESC NULLS LAST) AS rn,
             ret.rtruniquecode
      FROM itg_udcdetails itg_udcdetails
        LEFT JOIN itg_udcmaster udcmaster ON itg_udcdetails.columnname::TEXT = udcmaster.columnname::TEXT
        LEFT JOIN edw_retailer_dim ret
               ON itg_udcdetails.mastervaluecode = ret.retailer_code
              AND itg_udcdetails.distcode = ret.customer_code
              AND ret.actv_flg = 'Y'
      WHERE itg_udcdetails.mastername::TEXT = 'Retailer Master'::CHARACTER VARYING::TEXT
      AND   udcmaster.udcstatus = 1
      AND   itg_udcdetails.columnname::TEXT = 'New GTM') udc_1
WHERE udc_1.rn = 1
)
select * from final