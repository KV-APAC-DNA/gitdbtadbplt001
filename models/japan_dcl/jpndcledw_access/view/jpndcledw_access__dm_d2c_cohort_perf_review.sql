with cohort_perf_review AS (
    SELECT * 
    FROM {{ ref('jpndcledw_integration__dm_d2c_cohort_perf_review') }}
)
SELECT
month_id as "month id",  
birthday_yearmonth as "birthday yearmonth",
total_costomer as "total customer",
total_order as "total order", 
total_nts as "total nts", 
total_gts as "total gts", 
status as "status", 
inserted_date,
inserted_by ,
updated_date,
updated_by
FROM cohort_perf_review

