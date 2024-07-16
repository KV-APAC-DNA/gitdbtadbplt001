with source as 
(
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }} 
),
final as 
(
    SELECT week::number(18,0) as week,
        month::varchar(3) as month,
        qtr::number(18,0) as qtr,
        fisc_yr::number(18,0) as fisc_yr,
        customer_code::varchar(50) as customer_code,
        customer_name::varchar(150) as customer_name,
        retailer_code::varchar(100) as retailer_code,
        retailer_name::varchar(100) as retailer_name,
        region_name::varchar(50) as region_name,
        zone_name::varchar(50) as zone_name,
        territory_name::varchar(50) as territory_name,
        salesman_code::varchar(100) as salesman_code,
        salesman_name::varchar(200) as salesman_name,
        salesman_status::varchar(20) as salesman_status,
        status_desc::varchar(10) as status_desc,
        active_flag::varchar(8) as active_flag,
        channel_name::varchar(150) as channel_name,
        achievement_amt::number(38,6) as achievement_amt,
        franchise_name::varchar(50) as franchise_name,
        product_category_name::varchar(150) as product_category_name,
        variant_name::varchar(50) as variant_name,
        mth_mm::number(18,0) as mth_mm
FROM source
)

select * from final