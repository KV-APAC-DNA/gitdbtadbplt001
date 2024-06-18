with wks_issue_rev_sku_recom_tmp3 as
(
    select * from {{ ref('indwks_integration__wks_issue_rev_sku_recom_tmp3') }}
),
final as 
(
    SELECT mnth_id,
       week,  
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
       CASE
         WHEN achievement_nr_val > 0 THEN 1
         ELSE 0
       END AS sales_flag
FROM wks_issue_rev_sku_recom_tmp3
)
select * from final