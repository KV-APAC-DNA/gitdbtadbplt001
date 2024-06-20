with source as (
    select * from {{ ref('ntaedw_integration__edw_kr_dads_analysis') }}
),
final as (
    select
        brand as "brand",
        keyword as "keyword",
        search_area as "search_area",
        ad_types as "ad_types",
        retailer as "retailer",
        sub_retailer as "sub_retailer",
        product_name as "product_name",
        barcode as "barcode",
        keyword_group as "keyword_group",
        fisc_wk_num as "fisc_wk_num",
        fisc_mnth as "fisc_mnth",
        fisc_per as "fisc_per",
        fisc_day as "fisc_day",
        category_1 as "category_1",
        category_2 as "category_2",
        category_3 as "category_3",
        ranking as "ranking",
        click as "click",
        impression as "impression",
        ad_cost as "ad_cost",
        total_order as "total_order",
        total_order_sku as "total_order_sku",
        total_conversion_sales as "total_conversion_sales",
        total_conversion_sales_sku as "total_conversion_sales_sku",
        sales_gmv as "sales_gmv",
        sales_gmv_sku as "sales_gmv_sku",
        pa_cost as "pa_cost",
        bpa_cost as "bpa_cost",
        sa_cost as "sa_cost",
        observed_price as "observed_price",
        rocket_wow_price as "rocket_wow_price",
        total_monthly_search_volume as "total_monthly_search_volume",
        payment_amount as "payment_amount"
    from source
)
select * from final