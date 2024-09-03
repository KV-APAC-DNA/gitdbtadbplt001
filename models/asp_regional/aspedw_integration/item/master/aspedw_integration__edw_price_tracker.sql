{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where kpi='PRICE TRACKER SKU DATA';
        {% endif %}"
    )
}}

with source as
(
    select * from {{ ref('aspedw_integration__v_rpt_price_tracker') }}
),
final as
(
    select *,
    (mrp_lcy/nullif(final_packsize,0)) as price_per_volume_lcy,
    (mrp_usd/nullif(final_packsize,0)) as price_per_volume_usd,
    convert_timezone('Asia/Singapore',current_timestamp()) as crt_dttm  
    from source
)
select data_source::varchar(100) as data_source,
    kpi::varchar(200) as kpi,
    cluster::varchar(200) as cluster,
    market::varchar(200) as market,
    channel::varchar(200) as channel,
    retail_environment::varchar(200) as retail_environment,
    parent_customer::varchar(200) as parent_customer,
    distributor::varchar(200) as distributor,
    manufacturer::varchar(200) as manufacturer,
    competitor::varchar(75) as competitor,
    platform::varchar(200) as platform,
    store::varchar(200) as store,
    sub_store_1::varchar(100) as sub_store_1,
    sub_store_2::varchar(100) as sub_store_2,
    report_date::date as report_date,
    cal_week::number(38,0) as cal_week,
    fisc_yr_per::number(38,0) as fisc_yr_per,
    source_prod_hier_l1::varchar(200) as source_prod_hier_l1,
    source_prod_hier_l2::varchar(200) as source_prod_hier_l2,
    source_prod_hier_l3::varchar(200) as source_prod_hier_l3,
    source_prod_hier_l4::varchar(200) as source_prod_hier_l4,
    source_prod_hier_l5::varchar(200) as source_prod_hier_l5,
    prod_hier_l1::varchar(200) as prod_hier_l1,
    prod_hier_l2::varchar(200) as prod_hier_l2,
    prod_hier_l3::varchar(200) as prod_hier_l3,
    prod_hier_l4::varchar(200) as prod_hier_l4,
    prod_hier_l5::varchar(200) as prod_hier_l5,
    prod_hier_l6::varchar(200) as prod_hier_l6,
    prod_hier_l7::varchar(200) as prod_hier_l7,
    prod_hier_l8::varchar(200) as prod_hier_l8,
    prod_hier_l9::varchar(1000) as prod_hier_l9,
    prod_hier_l10::varchar(200) as prod_hier_l10,
    pka_rootcode::varchar(200) as pka_rootcode,
    pka_productdesc::varchar(255) as pka_productdesc,
    material_number::varchar(200) as material_number,
    ean_upc::varchar(510) as ean_upc,
    rpc::varchar(510) as rpc,
    base_packsize::number(38,0) as base_packsize,
    packsize_multiplier::number(38,0) as packsize_multiplier,
    market_input_packsize::number(31,3) as market_input_packsize,
    final_packsize::float as final_packsize,
    msl_flag::varchar(1) as msl_flag,
    greenlight_flag::number(38,0) as greenlight_flag,
    from_crncy::varchar(50) as from_crncy,
    to_crncy::varchar(50) as to_crncy,
    exch_rate_version::varchar(100) as exch_rate_version,
    msrp_lcy::number(15,2) as msrp_lcy,
    msrp_usd::number(25,7) as msrp_usd,
    mrp_lcy::number(15,2) as mrp_lcy,
    mrp_usd::number(25,7) as mrp_usd,
    mrp_type::varchar(100) as mrp_type,
    trunc(asp_lcy,5)::number(38,5) as asp_lcy,
    asp_usd::number(38,7) as asp_usd,
    observed_price_lcy::number(15,2) as observed_price_lcy,
    observed_price_usd::number(25,7) as observed_price_usd,
    bcp_lcy::number(25,7) as bcp_lcy,
    bcp_usd::number(25,7) as bcp_usd,
    target_index::number(31,3) as target_index,
    variance::number(31,3) as variance,
    benchmark_flag::varchar(1) as benchmark_flag,
    cust_promo_flag::number(38,0) as cust_promo_flag,
    price_promo_flag::number(38,0) as price_promo_flag,
    promo_desc::varchar(2000) as promo_desc,
    additional_info::varchar(200) as additional_info,
    price_per_volume_lcy::number(15,2) as price_per_volume_lcy,
    price_per_volume_usd::number(15,2) as price_per_volume_usd,
    crt_dttm::timestamp_ntz(9) as crt_dttm
 from final