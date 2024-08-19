with source as 
(
    select * from {{ ref('indedw_integration__edw_rpt_mi_rt_sales_details') }}
)
select
    data_source as "data_source",
    week as "week",
    month as "month",
    qtr as "qtr",
    year as "year",
    mth_mm as "mth_mm",
    superstockiest_code as "superstockiest_code",
    superstockiest_name as "superstockiest_name",
    subd_cmp_code as "subd_cmp_code",
    subd_name as "subd_name",
    retailer_code as "retailer_code",
    retailer_name as "retailer_name",
    region_name as "region_name",
    zone_name as "zone_name",
    territory_name as "territory_name",
    salesman_code as "salesman_code",
    salesman_name as "salesman_name",
    salesman_status as "salesman_status",
    district_name as "district_name",
    town_name as "town_name",
    rt_retailer_status as "rt_retailer_status",
    rt_subd_status as "rt_subd_status",
    sc_status_desc as "sc_status_desc",
    sc_active_flag as "sc_active_flag",
    achievement_amt as "achievement_amt",
    franchise_name as "franchise_name",
    product_category_name as "product_category_name",
    variant_name as "variant_name"
from source
