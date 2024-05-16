{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=["customer_name", "calendardate"],
        pre_hook= "delete from {{this}} WHERE coalesce(concat(rtrim(ltrim(customer_name)),rtrim(ltrim(calendardate))),'NA') IN (SELECT coalesce(concat(rtrim(ltrim(customer_name)),rtrim(ltrim(calendardate))),'NA') from {{ source('ntasdl_raw', 'sdl_mds_hk_pos_promo_calendar') }});"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_hk_pos_promo_calendar') }}
),
final as(
    select 
        calendardate::timestamp_ntz(9) as calendardate
        ,promoyear::number(31,0) as promoyear 
        ,promomonth::number(31,0) as promomonth 
        ,promoweekday::number(31,0) as promoweekday 
        ,promomonthweeknumber::number(31,0) as promomonthweeknumber 
        ,promoweeknumber::number(31,0) as promoweeknumber 
        ,customer_name::varchar(500) as customer_name
        ,current_timestamp()::timestamp_ntz(9) as crtd_dttm
    FROM source
)
select * from final