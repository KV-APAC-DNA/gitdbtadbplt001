with source as (
    select * from {{ ref('ntaedw_integration__edw_vw_kr_dads_analysis') }}
),
final as (
    select
        brand::varchar(200) as brand,
        keyword::varchar(200) as keyword,
        search_area::varchar(200) as search_area,
        ad_types::varchar(200) as ad_types,
        retailer::varchar(200) as retailer,
        sub_retailer::varchar(200) as sub_retailer,
        product_name::varchar(200) as product_name,
        barcode::varchar(200) as barcode,
        keyword_group::varchar(60) as keyword_group,
        fisc_wk_num::varchar(10) as fisc_wk_num,
        fisc_mnth::number(18,0) as fisc_mnth,
        fisc_per::number(18,0) as fisc_per,
        fisc_day::date as fisc_day,
        category_1::varchar(200) as category_1,
        category_2::varchar(200) as category_2,
        category_3::varchar(200) as category_3,
        ranking::varchar(250) as ranking,
        click::number(20,4) as click,
        impression::number(20,4) as impression,
        ad_cost::number(20,4) as ad_cost,
        total_order::number(20,4) as total_order,
        total_order_sku::number(20,4) as total_order_sku,
        total_conversion_sales::number(20,4) as total_conversion_sales,
        total_conversion_sales_sku::number(20,4) as total_conversion_sales_sku,
        sales_gmv::number(20,4) as sales_gmv,
        sales_gmv_sku::number(20,4) as sales_gmv_sku,
        pa_cost::number(20,4) as pa_cost,
        bpa_cost::number(20,4) as bpa_cost,
        sa_cost::number(20,4) as sa_cost,
        observed_price::number(20,4) as observed_price,
        rocket_wow_price::number(20,4) as rocket_wow_price,
        total_monthly_search_volume::number(20,4) as total_monthly_search_volume,
        payment_amount::number(20,4) as payment_amount
    from source
)
select * from final