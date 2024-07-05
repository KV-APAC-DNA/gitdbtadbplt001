{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where UPPER(TRIM(date)) || UPPER(TRIM(outlet_no)) || UPPER(TRIM(brand)) || UPPER(TRIM(yearmo)) in (select distinct UPPER(TRIM(date)) || UPPER(TRIM(outlet_no)) || UPPER(TRIM(brand)) || UPPER(TRIM(yearmo)) from {{ source('myssdl_raw','sdl_my_perfectstore_promocomp') }});
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('myssdl_raw','sdl_my_perfectstore_promocomp') }}
),
final as
(
    select 
        date::date as date ,
        channel::varchar(255) as channel ,
        chain::varchar(255) as chain ,
        region::varchar(255) as region ,
        outlet::varchar(255) as outlet ,
        outlet_no::varchar(255) as outlet_no ,
        category::varchar(255) as category ,
        brand::varchar(255) as brand ,
        activation::varchar(255) as activation ,
        promo_comp_on_time::varchar(255) as promo_comp_on_time ,
        promo_comp_on_time_in_full::varchar(255) as promo_comp_on_time_in_full ,
        promo_comp_successfully_set_up::varchar(255) as promo_comp_successfully_set_up ,
        non_compliance_reason::varchar(255) as non_compliance_reason ,
        run_id::number(14,0) as run_id ,
        file_name::varchar(255) as file_name ,
        yearmo::varchar(255) as yearmo,
        convert_timezone('Asia/Singapore',current_timestamp()::timestamp_ntz(9)) as crtd_dttm
        from source
)
select * from final