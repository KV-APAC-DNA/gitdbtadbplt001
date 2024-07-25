with wks_sss_msl_sales_combined_master_data as(
    select * from {{ ref('indwks_integration__wks_sss_msl_sales_combined_master_data') }}
),
wks_sss_sales_base_data_for_msl as(
    select * from {{ ref('indwks_integration__wks_sss_sales_base_data_for_msl') }}
),
transformed as(
    SELECT 
        'INDIA' AS country,
        MSL1.region_name,
        MSL1.zone_name,
        MSL1.territory_name,
        MSL1.channel_name,
        MSL1.retail_environment,
        MSL1.salesman_code,
        MSL1.salesman_name,
        MSL1.distributor_code,
        MSL1.distributor_name,
        MSL1.qtr,
        MSL1.cal_yr,
        MSL1.store_code,
        MSL1.store_name,
        MSL1.rtruniquecode,
        MSL1.prod_hier_l3,
        MSL1.prod_hier_l4,
        MSL1.prod_hier_l5,
        MSL1.prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        MSL1.prod_hier_l9,
        'Y' AS MSL_FLAG,
        CASE 
            WHEN SALES1.ACHIEVEMENT_NR > 0
                THEN 'Y'
            ELSE NULL
            END AS MSL_HIT
    FROM wks_sss_msl_sales_combined_master_data MSL1
    LEFT JOIN wks_sss_sales_base_data_for_msl SALES1 ON UPPER(MSL1.prod_hier_l9) = UPPER(SALES1.PRODUCT_CODE)
        AND UPPER(MSL1.retail_environment) = UPPER(SALES1.RETAIL_ENVIRONMENT)
        AND MSL1.cal_yr = SALES1.cal_yr
        AND MSL1.qtr = SALES1.qtr
        -- AND  UPPER(MSL1.store_code )        =  UPPER(SALES1.store_code )
        AND UPPER(MSL1.rtruniquecode) = UPPER(SALES1.rtruniquecode)

    --limit msl data with sss list received and join with the table to get the program type.
    --however no of records will increase per store code depending on no of unique latest outlet names
)
select * from transformed