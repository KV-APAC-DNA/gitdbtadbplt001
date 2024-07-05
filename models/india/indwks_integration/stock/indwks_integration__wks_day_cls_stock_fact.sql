with sdl_csl_productwisestock as
(
    select * from {{ source('indsdl_raw', 'sdl_csl_productwisestock') }}
),
edw_customer_dim as
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
itg_day_cls_stock_fact as
(
    select * from {{ source('inditg_integration', 'itg_day_cls_stock_fact_temp') }}
),
transformed as
(
SELECT 
  distcode as distcode,
  to_date(transdate) as transdate,
  MAX(lcnid) as lcnid,
  MAX(lcncode) as lcncode,
  MAX(prdid) as prdid,
  prdcode as prdcode,
  SUM(salopenstock) as salopenstock,
  SUM(unsalopenstock) as unsalopenstock,
  SUM(offeropenstock) as offeropenstock,
  SUM(salpurchase) as salpurchase,
  SUM(unsalpurchase) as unsalpurchase,
  SUM(offerpurchase) as offerpurchase,
  SUM(salpurreturn) as salpurreturn,
  SUM(unsalpurreturn) as unsalpurreturn,
  SUM(offerpurreturn) as offerpurreturn,
  SUM(salsales) as salsales,
  SUM(unsalsales) as unsalsales,
  SUM(offersales) as offersales,
  SUM(salstockin) as salstockin,
  SUM(unsalstockin) as unsalstockin,
  SUM(offerstockin) as offerstockin,
  SUM(salstockin) as salstockout,
  SUM(unsalstockin) as unsalstockout,
  SUM(offerstockin) as offerstockout,
  SUM(salstockout) as damagein,
  SUM(unsalstockout) as damageout,
  SUM(offerstockout) as salsalesreturn,
  SUM(damagein) as unsalsalesreturn,
  SUM(damageout) as offersalesreturn,
  SUM(salsalesreturn) as salstkjurin,
  SUM(unsalsalesreturn) as unsalstkjurin,
  SUM(offersalesreturn) as offerstkjurin,
  SUM(salstkjurin) as salstkjurout,
  SUM(unsalstkjurin) as unsalstkjurout,
  SUM(offerstkjurin) as offerstkjurout,
  SUM(salbattfrin) as salbattfrin,
  SUM(unsalbattfrin) as unsalbattfrin,
  SUM(offerbattfrin) as offerbattfrin,
  SUM(salbattfrout) as salbattfrout,
  SUM(unsalbattfrout) as unsalbattfrout,
  SUM(offerbattfrout) as offerbattfrout,
  SUM(sallcntfrin) as sallcntfrin,
  SUM(unsallcntfrin) as unsallcntfrin,
  SUM(offerlcntfrin) as offerlcntfrin,
  SUM(sallcntfrout) as sallcntfrout,
  SUM(unsallcntfrout) as unsallcntfrout,
  SUM(offerlcntfrout) as offerlcntfrout,
  SUM(salreplacement) as salreplacement,
  SUM(offerreplacement) as offerreplacement,
  SUM(salclsstock) as salclsstock,
  SUM(unsalclsstock) as unsalclsstock,
  SUM(offerclsstock) as offerclsstock,
  MAX(uploaddate) as uploaddate,
  MAX(uploadflag) as uploadflag,
  MAX(createddate) as createddate,
  MAX(syncid) as syncid,
  MAX(prod.crt_dttm) as crt_dttm,
FROM sdl_csl_productwisestock prod,
  edw_customer_dim cust
WHERE prod.distcode = cust.customer_code
  AND to_date(createddate) = (
    SELECT DISTINCT (to_date(createddate)) as createddate
    FROM sdl_csl_productwisestock
    WHERE to_date(createddate) > (
        SELECT to_date(MAX(createddate))
        FROM itg_DAY_CLS_STOCK_FACT
        )
    ORDER BY createddate ASC
    )
GROUP BY distcode,
  to_date(transdate),
  prdcode
),
final as 
(   
    select
        distcode::varchar(50) as distcode,
        transdate::timestamp_ntz(9) as transdate,
        lcnid::number(18,0) as lcnid,
        lcncode::varchar(100) as lcncode,
        prdid::number(18,0) as prdid,
        prdcode::varchar(100) as prdcode,
        salopenstock::number(18,0) as salopenstock,
        unsalopenstock::number(18,0) as unsalopenstock,
        offeropenstock::number(18,0) as offeropenstock,
        salpurchase::number(18,0) as salpurchase,
        unsalpurchase::number(18,0) as unsalpurchase,
        offerpurchase::number(18,0) as offerpurchase,
        salpurreturn::number(18,0) as salpurreturn,
        unsalpurreturn::number(18,0) as unsalpurreturn,
        offerpurreturn::number(18,0) as offerpurreturn,
        salsales::number(18,0) as salsales,
        unsalsales::number(18,0) as unsalsales,
        offersales::number(18,0) as offersales,
        salstockin::number(18,0) as salstockin,
        unsalstockin::number(18,0) as unsalstockin,
        offerstockin::number(18,0) as offerstockin,
        salstockout::number(18,0) as salstockout,
        unsalstockout::number(18,0) as unsalstockout,
        offerstockout::number(18,0) as offerstockout,
        damagein::number(18,0) as damagein,
        damageout::number(18,0) as damageout,
        salsalesreturn::number(18,0) as salsalesreturn,
        unsalsalesreturn::number(18,0) as unsalsalesreturn,
        offersalesreturn::number(18,0) as offersalesreturn,
        salstkjurin::number(18,0) as salstkjurin,
        unsalstkjurin::number(18,0) as unsalstkjurin,
        offerstkjurin::number(18,0) as offerstkjurin,
        salstkjurout::number(18,0) as salstkjurout,
        unsalstkjurout::number(18,0) as unsalstkjurout,
        offerstkjurout::number(18,0) as offerstkjurout,
        salbattfrin::number(18,0) as salbattfrin,
        unsalbattfrin::number(18,0) as unsalbattfrin,
        offerbattfrin::number(18,0) as offerbattfrin,
        salbattfrout::number(18,0) as salbattfrout,
        unsalbattfrout::number(18,0) as unsalbattfrout,
        offerbattfrout::number(18,0) as offerbattfrout,
        sallcntfrin::number(18,0) as sallcntfrin,
        unsallcntfrin::number(18,0) as unsallcntfrin,
        offerlcntfrin::number(18,0) as offerlcntfrin,
        sallcntfrout::number(18,0) as sallcntfrout,
        unsallcntfrout::number(18,0) as unsallcntfrout,
        offerlcntfrout::number(18,0) as offerlcntfrout,
        salreplacement::number(18,0) as salreplacement,
        offerreplacement::number(18,0) as offerreplacement,
        salclsstock::number(18,0) as salclsstock,
        unsalclsstock::number(18,0) as unsalclsstock,
        offerclsstock::number(18,0) as offerclsstock,
        uploaddate::timestamp_ntz(9) as uploaddate,
        uploadflag::varchar(10) as uploadflag,
        createddate::timestamp_ntz(9) as createddate,
        syncid::number(38,0) as syncid,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from transformed 
)
select * from final
