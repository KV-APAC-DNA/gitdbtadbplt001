{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}} WHERE yearmonth IN (SELECT DISTINCT yearmonth
		FROM {{source('chnsdl_raw', 'sdl_cn_selfcare_sellout_fact')}});
        {% endif %}"
    )
}}
with sdl_cn_selfcare_sellout_fact as
(
    select * from {{source('chnsdl_raw', 'sdl_cn_selfcare_sellout_fact')}}
),
itg_mds_cn_otc_product_mapping as
(
    select * from {{ ref('chnitg_integration__itg_mds_cn_otc_product_mapping') }} 
),
itg_mds_cn_otc_soldto_mapping as
(
    select * from {{ ref('chnitg_integration__itg_mds_cn_otc_soldto_mapping') }}
),
trans as
(
    select
    selfcare.yearmonth,
	selfcare.day,
	selfcare.sellercode,
	selfcare.sellername,
	selfcare.seller_tier,
	selfcare.buyercode,
	selfcare.buyername,
	otc_prdt_map.jntl_code AS sku,
	otc_sold_to_map.jntl_soldto AS sap_sold_to,
	selfcare.buyer_tier,
	selfcare.channel1,
	selfcare.channel2,
	selfcare.channel3,
	selfcare.channel4,
	selfcare.brand_code,
	selfcare.brand_name,
	selfcare.product_code,
	selfcare.product_name,
	selfcare.region,
	selfcare.seller_province_code,
	selfcare.seller_province_name,
	selfcare.seller_city_code,
	selfcare.seller_city_name,
	selfcare.seller_country_code,
	selfcare.seller_country_name,
	selfcare.buyer_province_code,
	selfcare.buyer_province_name,
	selfcare.buyer_city_code,
	selfcare.buyer_city_name,
	selfcare.buyer_country_code,
	selfcare.buyer_country_name,
	selfcare.rx_otc,
	selfcare.product_belong,
	selfcare.division_code,
	selfcare.division_name,
	selfcare.franchise_code,
	selfcare.franchise_name,
	selfcare.tcd_channel,
	selfcare.customer_level,
	selfcare.key_customer,
	selfcare.amount,
	selfcare.qty,
	selfcare.stp_amount,
	selfcare.STATUS,
	selfcare.cbd_ndc,
	selfcare.cbd_ka,
	selfcare.etl_insert_date,
	selfcare.etl_update_date,
	current_timestamp()::TIMESTAMP_NTZ(9) AS crtd_dttm,
	current_timestamp()::TIMESTAMP_NTZ(9) AS updt_dttm,
	selfcare.file_name
    FROM sdl_cn_selfcare_sellout_fact selfcare
    LEFT JOIN itg_mds_cn_otc_product_mapping as otc_prdt_map ON LTRIM(otc_prdt_map.code) = LTRIM(selfcare.product_code)
    LEFT JOIN itg_mds_cn_otc_soldto_mapping as otc_sold_to_map ON otc_sold_to_map.code = selfcare.sellercode
),
final as
(
    select
    yearmonth::varchar(20) as yearmonth,
	day::varchar(20) as day,
	sellercode::varchar(50) as sellercode,
	sellername::varchar(255) as sellername,
	seller_tier::varchar(50) as seller_tier,
	buyercode::varchar(20) as buyercode,
	buyername::varchar(255) as buyername,
	sku::varchar(50) as sku,
	sap_sold_to::varchar(50) as sap_sold_to,
	buyer_tier::varchar(50) as buyer_tier,
	channel1::varchar(100) as channel1,
	channel2::varchar(100) as channel2,
	channel3::varchar(100) as channel3,
	channel4::varchar(100) as channel4,
	brand_code::varchar(50) as brand_code,
	brand_name::varchar(100) as brand_name,
	product_code::varchar(40) as product_code,
	product_name::varchar(255) as product_name,
	region::varchar(50) as region,
	seller_province_code::varchar(50) as seller_province_code,
	seller_province_name::varchar(50) as seller_province_name,
	seller_city_code::varchar(50) as seller_city_code,
	seller_city_name::varchar(50) as seller_city_name,
	seller_country_code::varchar(50) as seller_country_code,
	seller_country_name::varchar(50) as seller_country_name,
	buyer_province_code::varchar(50) as buyer_province_code,
	buyer_province_name::varchar(50) as buyer_province_name,
	buyer_city_code::varchar(50) as buyer_city_code,
	buyer_city_name::varchar(50) as buyer_city_name,
	buyer_country_code::varchar(50) as buyer_country_code,
	buyer_country_name::varchar(50) as buyer_country_name,
	rx_otc::varchar(50) as rx_otc,
	product_belong::varchar(50) as product_belong,
	division_code::varchar(50) as division_code,
	division_name::varchar(50) as division_name,
	franchise_code::varchar(50) as franchise_code,
	franchise_name::varchar(50) as franchise_name,
	tcd_channel::varchar(50) as tcd_channel,
	customer_level::varchar(50) as customer_level,
	key_customer::varchar(50) as key_customer,
	amount::number(38,6) as amount,
	qty::number(38,6) as qty,
	stp_amount::number(38,6) as stp_amount,
	status::number(38,0) as status,
	cbd_ndc::varchar(50) as cbd_ndc,
	cbd_ka::varchar(50) as cbd_ka,
	etl_insert_date::varchar(50) as etl_insert_date,
	etl_update_date::varchar(50) as etl_update_date,
	crtd_dttm,
	updt_dttm,
	file_name::varchar(255) as file_name
    from trans
)
select * from final