{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} WHERE
        country = 'IN'
        {% endif %}"
    )
}}
with itg_hcp360_in_iqvia_indication as(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_iqvia_indication') }}
),
final as(
select
    zone::varchar(20) as zone,
    product_description::varchar(50) as product_description,
    brand::varchar(50) as brand,
    diagnosis::varchar(100) as diagnosis,
    year_month::date as year_month,
    no_of_prescriptions::number(18,5) as no_of_prescriptions,
    no_of_prescribers::number(18,5) as no_of_prescribers,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
    data_source::varchar(20) as data_source,
    'IN'::varchar(50) as country
FROM itg_hcp360_in_iqvia_indication
)
select * from final
















