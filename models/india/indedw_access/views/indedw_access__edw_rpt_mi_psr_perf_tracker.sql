with source as 
(
    select * from {{ ref('indedw_integration__edw_rpt_mi_psr_perf_tracker') }}
)
select
    year as "year",
    month as "month",
    qtr as "qtr",
    rtruniquecode_rt_dim as "rtruniquecode_rt_dim",
    customer_code_rt_dim as "customer_code_rt_dim",
    retailer_code_rt_dim as "retailer_code_rt_dim",
    smcode_rt_dim as "smcode_rt_dim",
    smname_rt_dim as "smname_rt_dim",
    uniquesalescode_rt_dim as "uniquesalescode_rt_dim",
    region_name_rt_dim as "region_name_rt_dim",
    zone_name_rt_dim as "zone_name_rt_dim",
    territory_name_rt_dim as "territory_name_rt_dim",
    channel_name as "channel_name",
    achievement_amt_sales_cube as "achievement_amt_sales_cube",
    rtruniquecode_sales_cube as "rtruniquecode_sales_cube",
    retailer_code_sales_cube as "retailer_code_sales_cube",
    latest_customer_code as "latest_customer_code",
    latest_customer_name as "latest_customer_name",
    latest_salesman_code as "latest_salesman_code",
    latest_salesman_name as "latest_salesman_name",
    latest_uniquesalescode as "latest_uniquesalescode",
    rtruniquecode_rt_cube as "rtruniquecode_rt_cube",
    subd_retailer_code as "subd_retailer_code",
    total_number_of_bills as "total_number_of_bills",
    no_of_packs as "no_of_packs",
    achievement_amt_rt_cube as "achievement_amt_rt_cube",
    total_outlets_subd_dim as "total_outlets_subd_dim",
    total_outlets_cust_subd_dim as "total_outlets_cust_subd_dim",
    os_flag as "os_flag",
    crt_dttm as "crt_dttm",
    updt_dttm as "updt_dttm"
from source
