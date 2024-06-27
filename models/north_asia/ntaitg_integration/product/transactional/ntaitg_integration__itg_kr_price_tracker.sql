{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where data_collection_date in (select distinct data_collection_date from {{ source('ntasdl_raw', 'sdl_kr_price_tracker') }});
        {% endif %}"
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_price_tracker') }}
),
transformed as (
    select 
        option,
        id__by_product_page,
        id_by_option_on_page,
        data_collection_date,
        data_collection_day,
        product_description_on_the_website,
        option_description,
        product_url,
        bundle_or_not,
        bundle_type,
        coalesce(cast(decode(total_volume_1, '', null, total_volume_1) as float8), 0.0, cast(decode(total_volume_1, '', null, total_volume_1) as float8)) as total_volume_1,
        gift,
        coalesce(cast(decode(price, '', null, price) as float8), 0.0, cast(decode(price, '', null, price) as float8)) as price,
        coalesce(cast(decode(option_price, '', null, option_price) as float8), 0.0, cast(decode(option_price, '', null, option_price) as float8)) as option_price,
        coalesce(cast(replace(decode(final_selling_price__including_shipping_fee, '', null, final_selling_price__including_shipping_fee), ',', '') as float8), 0.0, cast(replace(decode(final_selling_price__including_shipping_fee, '', null, final_selling_price__including_shipping_fee), ',', '') as float8)) as final_selling_price__including_shipping_fee,
        coalesce(cast(decode(shipping_fee, '', null, shipping_fee) as float8), 0.0, cast(decode(shipping_fee, '', null, shipping_fee) as float8)) as shipping_fee,
        shipping_fee_included_or_not,
        coalesce(cast(decode(sell_out_qty, '', null, sell_out_qty) as float8), 0.0, cast(decode(sell_out_qty, '', null, sell_out_qty) as float8)) as sell_out_qty,
        coalesce(cast(decode(total_sales_price, '', null, total_sales_price) as float8), 0.0, cast(decode(total_sales_price, '', null, total_sales_price) as float8)) as total_sales_price,
        promotional_site,
        customer_rating,
        count_of_product_review,
        sku_id,
        product_category,
        manufacturer__including_competitors,
        brand_name,
        product_description,
        qty_of_bundle,
        volume_per_unit_qty,
        total_volume_2,
        coalesce(cast(decode(price_per_ml, '', null, price_per_ml) as float8), 0.0, cast(decode(price_per_ml, '', null, price_per_ml) as float8)) as price_per_ml,
        coalesce(cast(decode(price_per_qty, '', null, price_per_qty) as float8), 0.0, cast(decode(price_per_qty, '', null, price_per_qty) as float8)) as price_per_qty,
        coalesce(cast(decode(recommended_consumer_price, '', null, recommended_consumer_price) as float8), 0.0, cast(decode(recommended_consumer_price, '', null, recommended_consumer_price) as float8)) as recommended_consumer_price,
        coalesce(cast(decode(selling_price_relative_to_the_regular_price, '', null, selling_price_relative_to_the_regular_price) as float8), 0.0, cast(decode(selling_price_relative_to_the_regular_price, '', null, selling_price_relative_to_the_regular_price) as float8)) as selling_price_relative_to_the_regular_price,
        coalesce(cast(decode(total_sales_qty, '', null, total_sales_qty) as float8), 0.0, cast(decode(total_sales_qty, '', null, total_sales_qty) as float8)) as total_sales_qty,
        type_of_ecom__open_market__social_commerce,
        ecom_mall_name,
        name_of_seller,
        null as name_of_representative,
        name_of_seller_s_company,
        seller_type__black_seller___official_seller
    from source
),
final as (
    select
        option::varchar(20) as option,
        id__by_product_page::varchar(255) as id__by_product_page,
        id_by_option_on_page::varchar(255) as id_by_option_on_page,
        data_collection_date::varchar(8) as data_collection_date,
        data_collection_day::varchar(20) as data_collection_day,
        product_description_on_the_website::varchar(255) as product_description_on_the_website,
        option_description::varchar(255) as option_description,
        product_url::varchar(2000) as product_url,
        bundle_or_not::varchar(20) as bundle_or_not,
        bundle_type::varchar(255) as bundle_type,
        total_volume_1::float as total_volume_1,
        gift::varchar(1) as gift,
        price::float as price,
        option_price::float as option_price,
        final_selling_price__including_shipping_fee::float as final_selling_price__including_shipping_fee,
        shipping_fee::float as shipping_fee,
        shipping_fee_included_or_not::varchar(5) as shipping_fee_included_or_not,
        sell_out_qty::float as sell_out_qty,
        total_sales_price::float as total_sales_price,
        promotional_site::varchar(255) as promotional_site,
        customer_rating::varchar(20) as customer_rating,
        count_of_product_review::varchar(20) as count_of_product_review,
        sku_id::varchar(255) as sku_id,
        product_category::varchar(255) as product_category,
        manufacturer__including_competitors::varchar(255) as manufacturer__including_competitors,
        brand_name::varchar(255) as brand_name,
        product_description::varchar(255) as product_description,
        qty_of_bundle::varchar(255) as qty_of_bundle,
        volume_per_unit_qty::varchar(20) as volume_per_unit_qty,
        total_volume_2::varchar(20) as total_volume_2,
        price_per_ml::float as price_per_ml,
        price_per_qty::float as price_per_qty,
        recommended_consumer_price::float as recommended_consumer_price,
        selling_price_relative_to_the_regular_price::float as selling_price_relative_to_the_regular_price,
        total_sales_qty::float as total_sales_qty,
        type_of_ecom__open_market__social_commerce::varchar(20) as type_of_ecom__open_market__social_commerce,
        ecom_mall_name::varchar(255) as ecom_mall_name,
        name_of_seller::varchar(255) as name_of_seller,
        name_of_representative::varchar(255) as name_of_representative,
        name_of_seller_s_company::varchar(255) as name_of_seller_s_company,
        seller_type__black_seller___official_seller::varchar(255) as seller_type__black_seller___official_seller
    from transformed
)
select * from final