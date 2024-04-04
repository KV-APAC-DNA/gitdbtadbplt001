{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="delete from {{this}} where cntry_cd='TH' and crtd_dttm < (select min(crtd_dttm) from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master_additional') }})"
    )
}}
with source as
(
    select * from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master_additional') }}
),

final as
(
    select
        'TH'::varchar(10) as cntry_cd,
        subscriber_key::varchar(100) as subscriber_key,
        attribute_name::varchar(100) as attribute_name,
        attribute_value::varchar(250) as attribute_value,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)

select * from final