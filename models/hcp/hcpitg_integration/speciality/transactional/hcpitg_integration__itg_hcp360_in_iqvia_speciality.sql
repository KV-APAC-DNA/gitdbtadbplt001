{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}}
        WHERE data_source='/source_type' and country = 'IN';
        {% endif %}"
    )
}}
with source1 as (
    select * from {{ source('snapindsdl_raw', 'sdl_hcp360_in_iqvia_speciality') }}  
),
final as (
    select 
        zone::varchar(20) as zone,
        product_description::varchar(50) as product_description,
        brand::varchar(50) as brand,
        speciality::varchar(100) as speciality,
        year_month::date as year_month,
        no_of_prescriptions::number(18,5) as no_of_prescriptions,
        no_of_prescribers::number(18,5) as no_of_prescribers,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        '/source_type'::varchar(20) as data_source,
        NULL as pack_volume,
        'IN' as country
    from source1
)
select * from final