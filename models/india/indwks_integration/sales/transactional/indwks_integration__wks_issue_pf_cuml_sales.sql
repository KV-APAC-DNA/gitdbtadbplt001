with source as
(
    select * from {{ ref('indwks_integration__wks_issue_pf_orng_perc') }}
),
final as
(
    SELECT op.*, 
        CASE WHEN (week0_sales_flag = 1) THEN 1 ELSE 0 END AS week0_sales_cuml,
        CASE WHEN (week0_sales_flag = 1 OR week1_sales_flag = 1) THEN 1 ELSE 0 END AS week1_sales_cuml,
        CASE WHEN (week0_sales_flag = 1 OR week1_sales_flag = 1 OR week2_sales_flag = 1) THEN 1 ELSE 0 END AS week2_sales_cuml,
        CASE WHEN (week0_sales_flag = 1 OR week1_sales_flag = 1 OR week2_sales_flag = 1 OR week3_sales_flag = 1) THEN 1 ELSE 0 END AS week3_sales_cuml,
        CASE WHEN (week0_sales_flag = 1 OR week1_sales_flag = 1 OR week2_sales_flag = 1 OR week3_sales_flag = 1 OR week4_sales_flag = 1) THEN 1 ELSE 0 END AS week4_sales_cuml,
        CASE WHEN (week0_sales_flag = 1 OR week1_sales_flag = 1 OR week2_sales_flag = 1 OR week3_sales_flag = 1 OR week4_sales_flag = 1 OR week5_sales_flag = 1) THEN 1 ELSE 0 END AS week5_sales_cuml  
    FROM source op
)
select * from final