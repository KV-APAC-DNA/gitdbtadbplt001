with source as
(
    select * from {{ ref('indwks_integration__wks_issue_pf_cuml_sales') }}
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
       region_name,
       zone_name,
       territory_name,
       compl_week0,
       compl_week1,
       compl_week2,
       compl_week3,
       compl_week4,
       compl_week5,
       CASE
         WHEN compl_week0 >= (orange_percentage / 100) THEN 1
         ELSE 0
       END AS os_flag_week0,
       CASE
         WHEN compl_week1 >= (orange_percentage / 100) THEN 1
         ELSE 0
       END AS os_flag_week1,
       CASE
         WHEN compl_week2 >= (orange_percentage / 100) THEN 1
         ELSE 0
       END AS os_flag_week2,
       CASE
         WHEN compl_week3 >= (orange_percentage / 100) THEN 1
         ELSE 0
       END AS os_flag_week3,
       CASE
         WHEN compl_week4 >= (orange_percentage / 100) THEN 1
         ELSE 0
       END AS os_flag_week4,
       CASE
         WHEN compl_week5 >= (orange_percentage / 100) THEN 1
         ELSE 0
       END AS os_flag_week5
    FROM (SELECT mnth_id,
                cust_cd,
                retailer_cd,
                channel_name,
                class_desc,
                retailer_channel_level_3,
                salesman_code,
                salesman_name,
                unique_sales_code,
                region_name,
                zone_name,
                territory_name,
                orange_percentage,
                (sales_week0::NUMERIC(18,2) / NULLIF(reco,0)) AS compl_week0,
                (sales_week1::NUMERIC(18,2) / NULLIF(reco,0)) AS compl_week1,
                (sales_week2::NUMERIC(18,2) / NULLIF(reco,0)) AS compl_week2,
                (sales_week3::NUMERIC(18,2) / NULLIF(reco,0)) AS compl_week3,
                (sales_week4::NUMERIC(18,2) / NULLIF(reco,0)) AS compl_week4,
                (sales_week5::NUMERIC(18,2) / NULLIF(reco,0)) AS compl_week5
        FROM (SELECT mnth_id,
                    cust_cd,
                    retailer_cd,
                    channel_name,
                    class_desc,
                    retailer_channel_level_3,
                    salesman_code,
                    salesman_name,
                    unique_sales_code,
                    region_name,
                    zone_name,
                    territory_name,
                    SUM(ms_flag::INTEGER) AS reco,
                    AVG(orange_percentage) AS orange_percentage,
                    SUM(week0_sales_cuml) AS sales_week0,
                    SUM(week1_sales_cuml) AS sales_week1,
                    SUM(week2_sales_cuml) AS sales_week2,
                    SUM(week3_sales_cuml) AS sales_week3,
                    SUM(week4_sales_cuml) AS sales_week4,
                    SUM(week5_sales_cuml) AS sales_week5
                FROM source
                GROUP BY 1,
                        2,
                        3,
                        4,
                        5,
                        6,7,8,9,10,11,12) agg) compl
)
select * from final