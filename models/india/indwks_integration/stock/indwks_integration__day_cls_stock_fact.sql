with itg_day_cls_stock_fact as
(
    select * from {{ ref('inditg_integration__itg_day_cls_stock_fact') }}
),
final as
(
SELECT 
    distcode::varchar(50) as distcode,
    to_date(transdate)::timestamp_ntz(9) as transdate,
    max(lcnid)::number(18,0) as lcnid,
    max(lcncode)::varchar(100) as lcncode,
    max(prdid)::number(18,0) as prdid,
    prdcode::varchar(100) as prdcode,
    sum(salopenstock)::number(18,0) as salopenstock,
    sum(unsalopenstock)::number(18,0) as unsalopenstock,
    sum(offeropenstock)::number(18,0) as offeropenstock,
    sum(salpurchase)::number(18,0) as salpurchase,
    sum(unsalpurchase)::number(18,0) as unsalpurchase,
    sum(offerpurchase)::number(18,0) as offerpurchase,
    sum(salpurreturn)::number(18,0) as salpurreturn,
    sum(unsalpurreturn)::number(18,0) as unsalpurreturn,
    sum(offerpurreturn)::number(18,0) as offerpurreturn,
    sum(salsales)::number(18,0) as salsales,
    sum(unsalsales)::number(18,0) as unsalsales,
    sum(offersales)::number(18,0) as offersales,
    sum(salstockin)::number(18,0) as salstockin,
    sum(unsalstockin)::number(18,0) as unsalstockin,
    sum(offerstockin)::number(18,0) as offerstockin,
    sum(salstockin)::number(18,0) as salstockout,
    sum(unsalstockin)::number(18,0) as unsalstockout,
    sum(offerstockin)::number(18,0) as offerstockout,
    sum(salstockout)::number(18,0) as damagein,
    sum(unsalstockout)::number(18,0) as damageout,
    sum(offerstockout)::number(18,0) as salsalesreturn,
    sum(damagein)::number(18,0) as unsalsalesreturn,
    sum(damageout)::number(18,0) as offersalesreturn,
    sum(salsalesreturn)::number(18,0) as salstkjurin,
    sum(unsalsalesreturn)::number(18,0) as unsalstkjurin,
    sum(offersalesreturn)::number(18,0) as offerstkjurin,
    sum(salstkjurin)::number(18,0) as salstkjurout,
    sum(unsalstkjurin)::number(18,0) as unsalstkjurout,
    sum(offerstkjurin)::number(18,0) as offerstkjurout,
    sum(salbattfrin)::number(18,0) as salbattfrin,
    sum(unsalbattfrin)::number(18,0) as unsalbattfrin,
    sum(offerbattfrin)::number(18,0) as offerbattfrin,
    sum(salbattfrout)::number(18,0) as salbattfrout,
    sum(unsalbattfrout)::number(18,0) as unsalbattfrout,
    sum(offerbattfrout)::number(18,0) as offerbattfrout,
    sum(sallcntfrin)::number(18,0) as sallcntfrin,
    sum(unsallcntfrin)::number(18,0) as unsallcntfrin,
    sum(offerlcntfrin)::number(18,0) as offerlcntfrin,
    sum(sallcntfrout)::number(18,0) as sallcntfrout,
    sum(unsallcntfrout)::number(18,0) as unsallcntfrout,
    sum(offerlcntfrout)::number(18,0) as offerlcntfrout,
    sum(salreplacement)::number(18,0) as salreplacement,
    sum(offerreplacement)::number(18,0) as offerreplacement,
    salclsstock::number(18,0) as salclsstock,
    sum(unsalclsstock)::number(18,0) as unsalclsstock,
    sum(offerclsstock)::number(18,0) as offerclsstock,
    max(uploaddate)::timestamp_ntz(9) as uploaddate,
    max(uploadflag)::varchar(10) as uploadflag,
    max(createddate)::timestamp_ntz(9) as createddate,
    max(syncid)::number(38,0) as syncid,
    max(crt_dttm)::timestamp_ntz(9) as crt_dttm,
    max(nr)::float as nr
FROM (
  SELECT rank() OVER (
      PARTITION BY a.distcode,
      to_date(a.transdate),
      a.prdcode ORDER BY a.createddate DESC
      ) AS RANK,
    a.*
  FROM itg_DAY_CLS_STOCK_FACT a
  INNER JOIN itg_DAY_CLS_STOCK_FACT b ON a.distcode = b.distcode
    AND a.transdate = b.transdate
    AND a.prdcode = b.prdcode
    AND to_date(b.createddate) = (
      SELECT to_date(MAX(createddate))
      FROM itg_DAY_CLS_STOCK_FACT
      )
  ) dist
GROUP BY distcode,
  transdate,
  prdcode,
  salclsstock,
  RANK
HAVING RANK = 1
)
select * from final
