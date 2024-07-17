with wks_tp_salescube_base as
(
    select * from snapindwks_integration.wks_tp_salescube_base
),
final as
(
    SELECT base.BRAND
       ,base.YEAR_MONTH
       ,base.REGION
       ,base.ZONE
       ,base.SALES_AREA
       ,MAT.YEAR_MONTH AS MAT_YEAR_MONTH
FROM (SELECT BRAND
            ,YEAR_MONTH
            ,REGION
            ,ZONE
            ,SALES_AREA
      FROM  wks_tp_salescube_base
      GROUP BY 1,2,3,4,5) base 
LEFT JOIN (SELECT BRAND
            ,YEAR_MONTH
            ,REGION
            ,ZONE
            ,SALES_AREA
      FROM  wks_tp_salescube_base
      GROUP BY 1,2,3,4,5) MAT
            ON base.BRAND = MAT.BRAND
           AND base.REGION = MAT.REGION
           AND base.ZONE = MAT.ZONE
           AND base.SALES_AREA = MAT.SALES_AREA
           AND TO_DATE(mat.YEAR_MONTH,'YYYYMM') > ADD_MONTHS(TO_DATE(base.YEAR_MONTH,'YYYYMM'),-12)
           AND TO_DATE(mat.YEAR_MONTH,'YYYYMM') < ADD_MONTHS(TO_DATE (base.YEAR_MONTH,'YYYYMM'),1)
)
select * from final