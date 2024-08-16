{{ 
    config
    (
                    materialized = "incremental", 
                    incremental_strategy = "append", 
                    pre_hook ="{% if is_incremental() %} 
                                delete from {{this}} WHERE to_date(createddate) = 
                                (SELECT DISTINCT (to_date(createddate)) as createddate
                                FROM {{ source('indsdl_raw', 'sdl_csl_productwisestock') }}
                                WHERE to_date(createddate) > (SELECT to_date(MAX(createddate))
                                FROM {{this}}) ORDER BY createddate ASC);
                                {% endif %}"
    )
}}

with wks_day_cls_stock_fact as 
(
    select * from {{ ref('indwks_integration__wks_day_cls_stock_fact') }}
),
  edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
  itg_xdm_batchmaster as 
(
    select * from {{ ref('inditg_integration__itg_xdm_batchmaster') }}
),
cte as 
(
    SELECT DISTINCT *
    FROM (
      SELECT prod.*,
        pl.NetRate AS nr,
        cust.state_code,
        row_number() OVER (
          PARTITION BY distcode,
          transdate,
          lcnid,
          lcncode,
          prdid,
          prdcode,
          salopenstock,
          unsalopenstock,
          offeropenstock,
          salpurchase,
          unsalpurchase,
          offerpurchase,
          salpurreturn,
          unsalpurreturn,
          offerpurreturn,
          salsales,
          unsalsales,
          offersales,
          salstockin,
          unsalstockin,
          offerstockin,
          salstockout,
          unsalstockout,
          offerstockout,
          damagein,
          damageout,
          salsalesreturn,
          unsalsalesreturn,
          offersalesreturn,
          salstkjurin,
          unsalstkjurin,
          offerstkjurin,
          salstkjurout,
          unsalstkjurout,
          offerstkjurout,
          salbattfrin,
          unsalbattfrin,
          offerbattfrin,
          salbattfrout,
          unsalbattfrout,
          offerbattfrout,
          sallcntfrin,
          unsallcntfrin,
          offerlcntfrin,
          sallcntfrout,
          unsallcntfrout,
          offerlcntfrout,
          salreplacement,
          offerreplacement,
          salclsstock,
          unsalclsstock,
          offerclsstock,
          uploaddate,
          uploadflag,
          createddate,
          syncid ORDER BY prod.transdate DESC
          ) rn
      FROM WKS_DAY_CLS_STOCK_FACT prod
      LEFT JOIN edw_customer_dim cust ON cust.customer_code = prod.distcode
      LEFT JOIN itg_xdm_batchmaster pl ON prod.prdcode = pl.ProdCode
        AND cust.state_code = pl.StateCode
      )
    WHERE rn = 1
),
transformed as 
(
    SELECT distcode,
      transdate,
      lcnid,
      lcncode,
      prdid,
      prdcode,
      salopenstock,
      unsalopenstock,
      offeropenstock,
      salpurchase,
      unsalpurchase,
      offerpurchase,
      salpurreturn,
      unsalpurreturn,
      offerpurreturn,
      salsales,
      unsalsales,
      offersales,
      salstockin,
      unsalstockin,
      offerstockin,
      salstockout,
      unsalstockout,
      offerstockout,
      damagein,
      damageout,
      salsalesreturn,
      unsalsalesreturn,
      offersalesreturn,
      salstkjurin,
      unsalstkjurin,
      offerstkjurin,
      salstkjurout,
      unsalstkjurout,
      offerstkjurout,
      salbattfrin,
      unsalbattfrin,
      offerbattfrin,
      salbattfrout,
      unsalbattfrout,
      offerbattfrout,
      sallcntfrin,
      unsallcntfrin,
      offerlcntfrin,
      sallcntfrout,
      unsallcntfrout,
      offerlcntfrout,
      salreplacement,
      offerreplacement,
      salclsstock,
      unsalclsstock,
      offerclsstock,
      uploaddate,
      uploadflag,
      createddate,
      syncid,
      crt_dttm,
      CASE 
        WHEN NR IS NULL
          THEN 0
        ELSE NR
        END AS nr
    FROM cte
),
final as 
(
SELECT distcode::VARCHAR(50) AS distcode,
    transdate::timestamp_ntz(9) AS transdate,
    lcnid::number(18, 0) AS lcnid,
    lcncode::VARCHAR(100) AS lcncode,
    prdid::number(18, 0) AS prdid,
    prdcode::VARCHAR(100) AS prdcode,
    salopenstock::number(18, 0) AS salopenstock,
    unsalopenstock::number(18, 0) AS unsalopenstock,
    offeropenstock::number(18, 0) AS offeropenstock,
    salpurchase::number(18, 0) AS salpurchase,
    unsalpurchase::number(18, 0) AS unsalpurchase,
    offerpurchase::number(18, 0) AS offerpurchase,
    salpurreturn::number(18, 0) AS salpurreturn,
    unsalpurreturn::number(18, 0) AS unsalpurreturn,
    offerpurreturn::number(18, 0) AS offerpurreturn,
    salsales::number(18, 0) AS salsales,
    unsalsales::number(18, 0) AS unsalsales,
    offersales::number(18, 0) AS offersales,
    salstockin::number(18, 0) AS salstockin,
    unsalstockin::number(18, 0) AS unsalstockin,
    offerstockin::number(18, 0) AS offerstockin,
    salstockout::number(18, 0) AS salstockout,
    unsalstockout::number(18, 0) AS unsalstockout,
    offerstockout::number(18, 0) AS offerstockout,
    damagein::number(18, 0) AS damagein,
    damageout::number(18, 0) AS damageout,
    salsalesreturn::number(18, 0) AS salsalesreturn,
    unsalsalesreturn::number(18, 0) AS unsalsalesreturn,
    offersalesreturn::number(18, 0) AS offersalesreturn,
    salstkjurin::number(18, 0) AS salstkjurin,
    unsalstkjurin::number(18, 0) AS unsalstkjurin,
    offerstkjurin::number(18, 0) AS offerstkjurin,
    salstkjurout::number(18, 0) AS salstkjurout,
    unsalstkjurout::number(18, 0) AS unsalstkjurout,
    offerstkjurout::number(18, 0) AS offerstkjurout,
    salbattfrin::number(18, 0) AS salbattfrin,
    unsalbattfrin::number(18, 0) AS unsalbattfrin,
    offerbattfrin::number(18, 0) AS offerbattfrin,
    salbattfrout::number(18, 0) AS salbattfrout,
    unsalbattfrout::number(18, 0) AS unsalbattfrout,
    offerbattfrout::number(18, 0) AS offerbattfrout,
    sallcntfrin::number(18, 0) AS sallcntfrin,
    unsallcntfrin::number(18, 0) AS unsallcntfrin,
    offerlcntfrin::number(18, 0) AS offerlcntfrin,
    sallcntfrout::number(18, 0) AS sallcntfrout,
    unsallcntfrout::number(18, 0) AS unsallcntfrout,
    offerlcntfrout::number(18, 0) AS offerlcntfrout,
    salreplacement::number(18, 0) AS salreplacement,
    offerreplacement::number(18, 0) AS offerreplacement,
    salclsstock::number(18, 0) AS salclsstock,
    unsalclsstock::number(18, 0) AS unsalclsstock,
    offerclsstock::number(18, 0) AS offerclsstock,
    uploaddate::timestamp_ntz(9) AS uploaddate,
    uploadflag::VARCHAR(10) AS uploadflag,
    createddate::timestamp_ntz(9) AS createddate,
    syncid::number(38, 0) AS syncid,
    current_timestamp()::timestamp_ntz(9) AS crt_dttm,
    nr::FLOAT AS nr
FROM transformed
)
select * from final
