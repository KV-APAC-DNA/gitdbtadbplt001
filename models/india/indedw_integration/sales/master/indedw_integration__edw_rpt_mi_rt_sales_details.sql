with source as
(
    select  * from {{ ref('indedw_integration__v_rpt_mi_rt_sales_details') }}
),
final as 
(
    select data_source::varchar(10) as data_source,
            week::number(18,0) as week,
            "month"::varchar(3) as "month",
            qtr::number(18,0) as qtr,
            "year"::number(18,0) as "year",
            mth_mm::number(18,0) as mth_mm,
            superstockiest_code::varchar(50) as superstockiest_code,
            superstockiest_name::varchar(225) as superstockiest_name,
            subd_cmp_code::varchar(100) as subd_cmp_code,
            subd_name::varchar(100) as subd_name,
            retailer_code::varchar(50) as retailer_code,
            retailer_name::varchar(150) as retailer_name,
            region_name::varchar(50) as region_name,
            zone_name::varchar(50) as zone_name,
            territory_name::varchar(50) as territory_name,
            salesman_code::varchar(151) as salesman_code,
            salesman_name::varchar(200) as salesman_name,
            salesman_status::varchar(20) as salesman_status,
            district_name::varchar(200) as district_name,
            town_name::varchar(200) as town_name,
            rt_retailer_status::varchar(10) as rt_retailer_status,
            rt_subd_status::varchar(10) as rt_subd_status,
            sc_status_desc::varchar(10) as sc_status_desc,
            sc_active_flag::varchar(8) as sc_active_flag,
            achievement_amt::number(38,6) as achievement_amt,
            franchise_name::varchar(50) as franchise_name,
            product_category_name::varchar(150) as product_category_name,
            variant_name::varchar(150) as variant_name
from source
)

select * from final