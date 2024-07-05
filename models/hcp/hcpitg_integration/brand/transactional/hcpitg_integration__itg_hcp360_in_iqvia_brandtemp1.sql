{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} WHERE
  data_source = '/source_type'
  and country = 'IN'
        {% endif %}"
    )
}}
with sdl_hcp360_in_iqvia_brand as(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_brand') }}
),
final as(
    select
        'IN' as country,
        product_description::varchar(50) as product_description,
        brand::varchar(50) as brand,
        pack_description::varchar(100) as pack_description,
        brand_category::varchar(50) as brand_category,
        zone::varchar(20) as zone,
        year_month::date as year_month,
        no_of_prescriptions::number(18,5) as no_of_prescriptions,
        no_of_prescribers::number(18,5) as no_of_prescribers,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
        '/source_type' as data_source
    from
    sdl_hcp360_in_iqvia_brand
)
select * from final
 