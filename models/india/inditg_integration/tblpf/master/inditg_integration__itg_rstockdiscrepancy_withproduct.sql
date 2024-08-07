{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE to_date(closingdate) >= (SELECT MIN(to_date(closingdate)) FROM {{ source('indsdl_raw', 'sdl_rstockdiscrepancy_withproduct') }});
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('indsdl_raw', 'sdl_rstockdiscrepancy_withproduct') }}
),
final as
(
    SELECT 
    distcode,
    prdcode,
    year,
    month,
    openingdate,
    closingdate,
    COALESCE(salopenstock,0) AS salopenstock,
    COALESCE(unsalopenstock,0) AS unsalopenstock,
    COALESCE(offeropenstock,0) AS offeropenstock,
    COALESCE(salpurchase,0) AS salpurchase,
    COALESCE(unsalpurchase,0) AS unsalpurchase,
    COALESCE(offerpurchase,0) AS offerpurchase,
    COALESCE(salpurreturn,0) AS salpurreturn,
    COALESCE(unsalpurreturn,0) AS unsalpurreturn,
    COALESCE(offerpurreturn,0) AS offerpurreturn,
    COALESCE(salsales,0) AS salsales,
    COALESCE(unsalsales,0) AS unsalsales,
    COALESCE(offersales,0) AS offersales,
    COALESCE(salstockin,0) AS salstockin,
    COALESCE(unsalstockin,0) AS unsalstockin,
    COALESCE(offerstockin,0) AS offerstockin,
    COALESCE(salstockout,0) AS salstockout,
    COALESCE(unsalstockout,0) AS unsalstockout,
    COALESCE(offerstockout,0) AS offerstockout,
    COALESCE(damagein,0) AS damagein,
    COALESCE(damageout,0) AS damageout,
    COALESCE(salsalesreturn,0) AS salsalesreturn,
    COALESCE(unsalsalesreturn,0) AS unsalsalesreturn,
    COALESCE(offersalesreturn,0) AS offersalesreturn,
    COALESCE(salstkjurin,0) AS salstkjurin,
    COALESCE(unsalstkjurin,0) AS unsalstkjurin,
    COALESCE(offerstkjurin,0) AS offerstkjurin,
    COALESCE(salstkjurout,0) AS salstkjurout,
    COALESCE(unsalstkjurout,0) AS unsalstkjurout,
    COALESCE(offerstkjurout,0) AS offerstkjurout,
    COALESCE(salbattfrin,0) AS salbattfrin,
    COALESCE(unsalbattfrin,0) AS unsalbattfrin,
    COALESCE(offerbattfrin,0) AS offerbattfrin,
    COALESCE(salbattfrout,0) AS salbattfrout,
    COALESCE(unsalbattfrout,0) AS unsalbattfrout,
    COALESCE(offerbattfrout,0) AS offerbattfrout,
    COALESCE(sallcntfrin,0) AS sallcntfrin,
    COALESCE(unsallcntfrin,0) AS unsallcntfrin,
    COALESCE(offerlcntfrin,0) AS offerlcntfrin,
    COALESCE(sallcntfrout,0) AS sallcntfrout,
    COALESCE(unsallcntfrout,0) AS unsallcntfrout,
    COALESCE(offerlcntfrout,0) AS offerlcntfrout,
    COALESCE(salreplacement,0) AS salreplacement,
    COALESCE(offerreplacement,0) AS offerreplacement,
    COALESCE(salclsstock,0) AS salclsstock,
    COALESCE(unsalclsstock,0) AS unsalclsstock,
    COALESCE(offerclsstock,0) AS offerclsstock,
    COALESCE(calsalclosing,0) AS calsalclosing,
    COALESCE(salclosingdiff,0) AS salclosingdiff,
    COALESCE(calunsalclosing,0) AS calunsalclosing,
    COALESCE(unsalclosingdiff,0) AS unsalclosingdiff,
    COALESCE(calofferclosing,0) AS calofferclosing,
    COALESCE(offerclosingdiff,0) AS offerclosingdiff,
    COALESCE(nr,0) AS nr,
    COALESCE(lp,0) AS lp,
    COALESCE(ptr,0) AS ptr,
    createddate,
    createddt,
    convert_timezone('Asia/Kolkata',current_timestamp()) AS updt_dttm
    FROM source
)
select distcode::varchar(50) as distcode,
    prdcode::varchar(50) as prdcode,
    year::number(18,0) as year,
    month::number(18,0) as month,
    openingdate::timestamp_ntz(9) as openingdate,
    closingdate::timestamp_ntz(9) as closingdate,
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
    calsalclosing::number(18,0) as calsalclosing,
    salclosingdiff::number(18,0) as salclosingdiff,
    calunsalclosing::number(18,0) as calunsalclosing,
    unsalclosingdiff::number(18,0) as unsalclosingdiff,
    calofferclosing::number(18,0) as calofferclosing,
    offerclosingdiff::number(18,0) as offerclosingdiff,
    nr::number(18,6) as nr,
    lp::number(18,6) as lp,
    ptr::number(18,6) as ptr,
    createddate::timestamp_ntz(9) as createddate,
    createddt::timestamp_ntz(9) as createddt,
    updt_dttm::timestamp_ntz(9) as updtdttm
from final