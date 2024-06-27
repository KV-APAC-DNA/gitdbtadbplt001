with wks_issue_pf_weekly_sales_flag_pivot as
(
    select * from {{ ref('indwks_integration__wks_issue_pf_weekly_sales_flag_pivot') }}
),
edw_retailer_dim as
(
    select * from snapindedw_integration.edw_retailer_dim
    --{{ ref('indedw_integration__edw_retailer_dim') }}
),
final as
(
    SELECT tbl.*,
       ret.region_name,
       ret.zone_name,
       ret.territory_name
    FROM wks_issue_pf_weekly_sales_flag_pivot tbl
    LEFT JOIN (SELECT retailer_code,
                        customer_code,
                        region_name,
                        zone_name,
                        territory_name,
                        row_number() OVER (PARTITION BY customer_code,retailer_code ORDER BY end_date DESC) AS rn
                FROM edw_retailer_dim) ret
            ON COALESCE (ret.retailer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE (tbl.retailer_cd,'0'::CHARACTER VARYING)::TEXT
            AND COALESCE (ret.customer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE (tbl.cust_cd,'0'::CHARACTER VARYING)::TEXT
            AND ret.rn = 1
)
select * from final