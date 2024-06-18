with source as
(
    select * from {{ ref('indwks_integration__wks_issue_pf_os_flag') }}
),
final as 
(
    SELECT mnth_id,
       channel_name,
       class_desc,
       retailer_channel_level_3,
       region_name,
       zone_name,
       territory_name,
       cust_cd,
       salesman_code,
       salesman_name,
       unique_sales_code,
       SUM(os_flag_week0) AS os_week0,
       SUM(os_flag_week1) AS os_week1,
       SUM(os_flag_week2) AS os_week2,
       SUM(os_flag_week3) AS os_week3,
       SUM(os_flag_week4) AS os_week4,
       SUM(os_flag_week5) AS os_week5,
       COUNT(os_flag_week1) AS total_stores,
       (SUM(os_flag_week0)::NUMERIC(18,2)/COUNT(os_flag_week0))*100 AS os_perc_week0,
       (SUM(os_flag_week1)::NUMERIC(18,2)/COUNT(os_flag_week1))*100 AS os_perc_week1,
       (SUM(os_flag_week2)::NUMERIC(18,2)/COUNT(os_flag_week2))*100 AS os_perc_week2,
       (SUM(os_flag_week3)::NUMERIC(18,2)/COUNT(os_flag_week3))*100 AS os_perc_week3,
       (SUM(os_flag_week4)::NUMERIC(18,2)/COUNT(os_flag_week4))*100 AS os_perc_week4,
       (SUM(os_flag_week5)::NUMERIC(18,2)/COUNT(os_flag_week5))*100 AS os_perc_week5
    FROM  source 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
select * from final