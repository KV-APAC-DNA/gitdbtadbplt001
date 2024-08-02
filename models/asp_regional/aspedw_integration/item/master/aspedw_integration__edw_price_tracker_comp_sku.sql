{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                    DELETE FROM {{this}}
                    WHERE kpi = 'PRICE TRACKER COMP SKU DATA';
                    {% endif %}"
    )
}}
with v_rpt_price_tracker_comp_sku as 
(
    select * from dev_dna_core.aspedw_integration.v_rpt_price_tracker_comp_sku
),
final as 
(
    select
    data_source::varchar(200) as data_source,
	kpi::varchar(200) as kpi,
	cluster::varchar(200) as cluster,
	market::varchar(200) as market,
	channel::varchar(200) as channel,
	retail_environment::varchar(200) as retail_environment,
	parent_customer::varchar(200) as parent_customer,
	manufacturer::varchar(200) as manufacturer,
	competitor::varchar(200) as competitor,
	platform::varchar(200) as platform,
	store::varchar(200) as store,
	sub_store_1::varchar(200) as sub_store_1,
	sub_store_2::varchar(200) as sub_store_2,
	report_date::date as report_date,
	jj_sku_desc::varchar(1000) as jj_sku_desc,
	jj_packsize::number(31,3) as jj_packsize,
	jj_upc::varchar(510) as jj_upc,
	comp_upc::varchar(510) as comp_upc,
	comp_rpc::varchar(100) as comp_rpc,
	comp_sku_desc::varchar(1000) as comp_sku_desc,
	comp_brand::varchar(500) as comp_brand,
	comp_packsize::number(31,3) as comp_packsize,
	msrp_lcy::number(15,2) as msrp_lcy,
	msrp_usd::number(25,7) as msrp_usd,
	mrp_lcy::number(15,2) as mrp_lcy,
	mrp_usd::number(25,7) as mrp_usd,
	mrp_type::varchar(100) as mrp_type,
	asp_lcy::number(38,2) as asp_lcy,
	asp_usd::number(38,7) as asp_usd,
	observed_price_lcy::number(15,2) as observed_price_lcy,
	observed_price_usd::number(25,7) as observed_price_usd,
	bcp_lcy::number(25,7) as bcp_lcy,
	bcp_usd::number(33,10) as bcp_usd,
	jj_promo_flag::number(38,0) as jj_promo_flag,
	cust_comp_promo_flag::number(38,0) as cust_comp_promo_flag,
	price_comp_promo_flag::number(38,0) as price_comp_promo_flag,
	NULL as price_per_volume_lcy,
	NULL as price_per_volume_usd,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from v_rpt_price_tracker_comp_sku
)
select * from final