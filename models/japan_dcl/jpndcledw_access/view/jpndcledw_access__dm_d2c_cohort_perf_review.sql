with cohort_perf_review AS (
    SELECT * 
    FROM {{ ref('jpndcledw_integration__dm_d2c_cohort_perf_review') }}
)
SELECT
month_id as "month_id",  
birthday_yearmonth as "birthday_yearmonth",
total_costomer as "total_costomer",
total_order as "total_order", 
total_nts as "total_nts", 
total_gts as "total_gts", 
status as "status", 
inserted_date,
inserted_by ,
updated_date,
updated_by
FROM cohort_perf_review

