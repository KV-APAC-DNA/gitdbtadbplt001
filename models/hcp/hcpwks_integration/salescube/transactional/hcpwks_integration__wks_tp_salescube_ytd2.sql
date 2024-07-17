with wks_tp_salescube_base as
(
    select * from snapindwks_integration.wks_tp_salescube_base
),
wks_tp_salescube_ytd1 as
(
    select * from snapindwks_integration.wks_tp_salescube_ytd1
),
final as
(
    SELECT YTD.BRAND
        ,YTD.YEAR_MONTH
        ,YTD.REGION
        ,YTD.ZONE
        ,YTD.SALES_AREA
        ,COUNT(DISTINCT base.num_buying_retailer) AS "ret_cnt_ytd"
    FROM (SELECT BRAND
                ,YEAR_MONTH
                ,REGION
                ,ZONE
                ,SALES_AREA
                ,num_buying_retailer
        FROM  wks_tp_salescube_base
        GROUP BY 1,2,3,4,5,6) base 
    INNER JOIN wks_tp_salescube_ytd1 YTD
                ON base.BRAND = YTD.BRAND
            AND base.REGION = YTD.REGION
            AND base.ZONE = YTD.ZONE
            AND base.SALES_AREA = YTD.SALES_AREA
            AND YTD.YTD_YEAR_MONTH =  base.YEAR_MONTH
    GROUP BY 1,2,3,4,5
)
select * from final