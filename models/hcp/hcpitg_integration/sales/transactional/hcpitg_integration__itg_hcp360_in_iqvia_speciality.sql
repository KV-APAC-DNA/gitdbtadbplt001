{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook =" {% if is_incremental() %}
                    delete from {{this}} WHERE data_source = 'ORSL'
                    AND country = 'IN';
                    {% endif %}"
    )
}}

with sdl_hcp360_in_iqvia_speciality as
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_speciality') }}
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_iqvia_speciality__test_format') }}
    )
),
transformed as
(
    SELECT
        zone::varchar(20) as zone,
        product_description::varchar(50) as product_description,
        brand::varchar(50) as brand,
        speciality::varchar(100) as speciality,
        year_month::date as year_month,
        no_of_prescriptions::number(18,5) as no_of_prescriptions,
        no_of_prescribers::number(18,5) as no_of_prescribers,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm,
        'ORSL' as data_source,
        null::varchar(20) as pack_volume,
        'IN' as country,
        filename::varchar(255) as file_name
    FROM sdl_hcp360_in_iqvia_speciality
)
select * from transformed
