{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        where country = 'IN';
        {% endif %}"
    )
}}

with itg_hcp360_in_iqvia_speciality as
(
    select * from snapinditg_integration.itg_hcp360_in_iqvia_speciality
),
final as
(
    SELECT 
    zone,
    product_description,
    brand,
    speciality,
    year_month,
    no_of_prescriptions,
    no_of_prescribers,
    convert_timezone('UTC',current_timestamp()) as crt_dttm,
    convert_timezone('UTC',current_timestamp()) as updt_dttm,
    data_source,
    null as pack_volume,
    'IN' as country
    FROM itg_hcp360_in_iqvia_speciality
)
select zone::varchar(20) as zone,
    product_description::varchar(50) as product_description,
    brand::varchar(50) as brand,
    speciality::varchar(100) as speciality,
    year_month::date as year_month,
    no_of_prescriptions::number(18,5) as no_of_prescriptions,
    no_of_prescribers::number(18,5) as no_of_prescribers,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    data_source::varchar(20) as data_source,
    pack_volume::varchar(20) as pack_volume,
    country::varchar(50) as country
 from final