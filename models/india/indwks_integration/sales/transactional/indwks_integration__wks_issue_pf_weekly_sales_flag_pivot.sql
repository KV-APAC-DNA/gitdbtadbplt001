with wks_issue_rev_pf_weekly_sales_flag as
(
    select * from {{ ref('indwks_integration__wks_issue_rev_pf_weekly_sales_flag') }}
),
final as 
(
    SELECT mnth_id,
       cust_cd,
       retailer_cd,
       channel_name,
       class_desc,
       retailer_channel_level_3,
       salesman_code,
       salesman_name,
       unique_sales_code,
       mother_sku_cd,
       ms_flag,
       SUM(CASE WHEN week = 0 THEN sales_flag ELSE 0 END) AS week0_sales_flag,
       SUM(CASE WHEN week = 1 THEN sales_flag ELSE 0 END) AS week1_sales_flag,
       SUM(CASE WHEN week = 2 THEN sales_flag ELSE 0 END) AS week2_sales_flag,
       SUM(CASE WHEN week = 3 THEN sales_flag ELSE 0 END) AS week3_sales_flag,
       SUM(CASE WHEN week = 4 THEN sales_flag ELSE 0 END) AS week4_sales_flag,
       SUM(CASE WHEN week = 5 THEN sales_flag ELSE (CASE WHEN RIGHT(mnth_id,2) IN (03,06,09,12) THEN 0 ELSE NULL END) END) AS week5_sales_flag
    FROM wks_issue_rev_pf_weekly_sales_flag
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
select * from final