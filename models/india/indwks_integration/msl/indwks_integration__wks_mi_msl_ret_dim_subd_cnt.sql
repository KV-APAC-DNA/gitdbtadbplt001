with edw_retailer_dim as(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
edw_customer_dim as(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
transformed as(
    SELECT ret.region_name,
        ret.zone_name,
        ret.territory_name,
        retailer_category_name,
        COUNT(DISTINCT ret.retailer_code) AS total_subd
    FROM edw_retailer_dim ret
    INNER JOIN edw_customer_dim cust ON ret.customer_code = cust.customer_code
        AND cust.active_flag = 'Y'
    WHERE UPPER(ret.channel_name) = 'SUB-STOCKIEST'
        AND ret.actv_flg = 'Y'
        AND ret.status_desc = 'Active'
    GROUP BY 1,
        2,
        3,
        4
)
select * from transformed