{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} WHERE country = 'IN';
                    {% endif %}"
    )
}}

with itg_hcp360_in_iqvia_sales as 
(
    select * from dev_dna_core.snapinditg_integration.itg_hcp360_in_iqvia_sales
),
final as 
(
    SELECT 
        'IN' as country,
        STATE::varchar(50) as state,
        region::varchar(50) as region,
        product_description::varchar(100) as product_description,
        brand::varchar(50) as brand,
        pack_description::varchar(100) as pack_description,
        brand_category::varchar(20) as brand_category,
        year_month::date as year_month,
        total_units::number(18,5) as total_units,
        value::number(18,5) as value,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        data_source::varchar(20) as data_source
    FROM itg_hcp360_in_iqvia_sales
)
select * from final
