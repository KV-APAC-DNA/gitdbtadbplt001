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
with itg_hcp360_in_iqvia_brand as(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_in_iqvia_brand') }}
),
final as(
select
product_description::varchar(50) as product_description,
brand::varchar(50) as brand,
pack_description::varchar(100) as pack_description,
null::VARCHAR(50) as pack_form,
brand_category::varchar(50) as brand_category,
zone::varchar(20) as zone,
year_month::date as year_month,
no_of_prescriptions::number(18,5) as no_of_prescriptions,
no_of_prescribers::number(18,5) as no_of_prescribers,
convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
data_source::varchar(20) as data_source,
null::VARCHAR(20) as pack_volume,
'IN'::varchar(50) as country,
from itg_hcp360_in_iqvia_brand
)
select * from final