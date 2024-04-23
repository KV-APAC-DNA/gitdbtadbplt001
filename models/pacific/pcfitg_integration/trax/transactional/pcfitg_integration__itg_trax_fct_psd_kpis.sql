{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        unique_key = "visit_date",
        pre_hook="delete from {{this}} where visit_date in (select distinct visit_date from {{ source('pcfsdl_raw', 'sdl_trax_fct_psd_kpis') }})"
    )
}}
with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_trax_fct_psd_kpis') }}
),
final as
(    
    select 
        datasource::varchar(124) as datasource,
        source::varchar(128) as source,
        filename::varchar(128) as filename,
        providerid::number(18,0) as providerid,
        provider::varchar(64) as provider,
        prv_product_level::varchar(64) as prv_product_level,
        prv_channel::varchar(64) as prv_channel,
        prv_country::varchar(64) as prv_country,
        countryid::varchar(16) as countryid,
        country::varchar(64) as country,
        channelid::varchar(16) as channelid,
        channel::varchar(64) as channel,
        visit_date::date as visit_date,
        categoryid::number(18,0) as categoryid,
        category::varchar(256) as category,
        subcategoryid::number(18,0) as subcategoryid,
        subcategory::varchar(256) as subcategory,
        manufacturer::varchar(256) as manufacturer,
        brand::varchar(256) as brand,
        subbrand::varchar(256) as subbrand,
        productid::varchar(64) as productid,
        product::varchar(256) as product,
        storeid::varchar(64) as storeid,
        store::varchar(256) as store,
        region::varchar(256) as region,
        reid::number(18,0) as reid,
        re::varchar(256) as re,
        retailerid::number(18,0) as retailerid,
        retailer::varchar(256) as retailer,
        rgid::number(18,0) as rgid,
        retailer_group::varchar(256) as retailer_group,
        postalcode::varchar(32) as postalcode,
        salesagentid::varchar(64) as salesagentid,
        salesagent::varchar(64) as salesagent,
        pic_link::varchar(2048) as pic_link,
        orig_task_name::varchar(256) as orig_task_name,
        jj_shelf::float as jj_shelf,
        all_shelf::float as all_shelf,
        jj_oos::float as jj_oos,
        all_oos::float as all_oos,
        jj_he::float as jj_he,
        all_he::float as all_he,
        jj_promo::float as jj_promo,
        all_promo::float as all_promo,
        availability::float as availability,
        jj_msl::float as jj_msl,
        all_msl::float as all_msl,
        jj_sp::float as jj_sp,
        all_sp::float as all_sp,
        value_sales::float as value_sales,
        wd::float as wd,
        msl_target::float as msl_target,
        kpi_ssp::float as kpi_ssp,
        current_timestamp::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final

