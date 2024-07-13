with itg_udcdetails as(
    select * from {{ ref('inditg_integration__itg_udcdetails') }}
),
itg_udcmaster as(
    select * from {{ ref('inditg_integration__itg_udcmaster') }}
),
wks_skurecom_mi_actuals_tmp1 as(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_actuals_tmp1') }}
),
final as(
    SELECT udc_1.distcode,
       udc_1.mastervaluecode,
       udc_1.mastervaluename,
       udc_1.columnname,
       udc_1.columnvalue,
       udc_1.rn
FROM (SELECT itg_udcdetails.distcode,
             itg_udcdetails.mastervaluecode,
             UPPER(itg_udcdetails.mastervaluename::TEXT) AS mastervaluename,
             itg_udcdetails.columnname,
             CASE
               WHEN itg_udcdetails.columnvalue IS NULL OR TRIM(itg_udcdetails.columnvalue::TEXT) = ''::CHARACTER VARYING::TEXT THEN NULL::CHARACTER VARYING::TEXT
               ELSE UPPER(itg_udcdetails.columnvalue::TEXT)
             END AS columnvalue,
             row_number() OVER (PARTITION BY itg_udcdetails.distcode,itg_udcdetails.mastervaluecode,itg_udcdetails.columnname ORDER BY itg_udcdetails.createddate DESC,itg_udcdetails.columnvalue DESC NULLS LAST) AS rn
      FROM itg_udcdetails itg_udcdetails
        LEFT JOIN itg_udcmaster udcmaster ON itg_udcdetails.columnname::TEXT = udcmaster.columnname::TEXT
      WHERE itg_udcdetails.mastername::TEXT = 'Retailer Master'::CHARACTER VARYING::TEXT
      AND   udcmaster.udcstatus = 1
      AND   itg_udcdetails.columnname::TEXT = 'New GTM'::CHARACTER VARYING::TEXT) udc_1
WHERE udc_1.rn = 1
AND (udc_1.distcode,udc_1.mastervaluecode) IN (SELECT cust_cd, retailer_cd
                                               FROM wks_skurecom_mi_actuals_tmp1
                                               GROUP BY 1,2)
)
select * from final