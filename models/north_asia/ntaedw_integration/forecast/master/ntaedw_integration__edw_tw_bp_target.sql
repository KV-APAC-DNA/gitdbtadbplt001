with source as(
    select * from {{ ref('ntaitg_integration__itg_tw_bp_target') }} 
),
final as(
    select
        bp_version::varchar(5) as bp_version,
        forecast_on_year::varchar(10) as forecast_on_year,
        forecast_on_month::varchar(10) as forecast_on_month,
        forecast_for_year::varchar(10) as forecast_for_year,
        forecast_for_mnth::varchar(10) as forecast_for_mnth,
        sls_grp::varchar(100) as sls_grp,
        channel::varchar(100) as channel,
        sls_ofc::varchar(100) as sls_ofc,
        sls_ofc_desc::varchar(255) as sls_ofc_desc,
        strategy_customer_hierachy_name::varchar(255) as strategy_customer_hierachy_name,
        region::varchar(100) as region,
        lph_level_6::varchar(255) as lph_level_6,
        pre_sales::float as pre_sales,
        tp::float as tp,
        nts::float as nts,
        load_date::timestamp_ntz(9) as load_date
    from source
)
select * from final