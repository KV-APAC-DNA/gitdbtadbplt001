{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} WHERE data_source = 'ORSL'and country = 'IN'
        and 0 != (select count(*) 
        from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_indication') }}
        where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_iqvia_indication__test_format') }}
        )
        )
        {% endif %}"
    )
}}
with sdl_hcp360_in_iqvia_indication as(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_indication') }}
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_iqvia_indication__test_format') }}
    )

),
final as(
    select
        zone::varchar(20) as zone,
        product_description::varchar(50) as product_description,
        brand::varchar(50) as brand,
        diagnosis::varchar(100) as diagnosis,
        year_month::date as year_month,
        no_of_prescriptions::number(18,5) as no_of_prescriptions,
        trunc(no_of_prescribers,6)::number(18,5) as no_of_prescribers,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
        'ORSL' as data_source,
        'IN' as country,
        filename as filename
    from sdl_hcp360_in_iqvia_indication
    )
select * from final