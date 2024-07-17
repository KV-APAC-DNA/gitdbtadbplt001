with wks_tp_salescube_base as
(
    select * from snapindwks_integration.wks_tp_salescube_base
),
wks_tp_salescube_mat1 as
(
    select * from {{ ref('hcpwks_integration__wks_tp_salescube_mat1') }}
),
final as
(
    SELECT mat.BRAND
        ,mat.YEAR_MONTH
        ,mat.REGION
        ,mat.ZONE
        ,mat.SALES_AREA
        ,COUNT(DISTINCT base.num_buying_retailer) AS "ret_cnt_mat"
    FROM (SELECT BRAND
                ,YEAR_MONTH
                ,REGION
                ,ZONE
                ,SALES_AREA
                ,num_buying_retailer
        FROM  wks_tp_salescube_base
        GROUP BY 1,2,3,4,5,6) base 
    INNER JOIN wks_tp_salescube_mat1 mat
                ON base.BRAND = MAT.BRAND
            AND base.REGION = MAT.REGION
            AND base.ZONE = MAT.ZONE
            AND base.SALES_AREA = MAT.SALES_AREA
            AND mat.MAT_YEAR_MONTH =  base.YEAR_MONTH
    GROUP BY 1,2,3,4,5
)
select * from final