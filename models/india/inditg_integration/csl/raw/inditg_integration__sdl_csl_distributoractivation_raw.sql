{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as
(
    select * from {{source('indsdl_raw', 'sdl_csl_distributoractivation')}}
),
final as
(
    select 
        distcode::varchar(400) as distcode,
        activefromdate::timestamp_ntz(9) as activefromdate,
        activatedby::number(18,0) as activatedby,
        activatedon::timestamp_ntz(9) as activatedon,
        inactivefromdate::timestamp_ntz(9) as inactivefromdate,
        inactivatedby::number(18,0) as inactivatedby,
        inactivatedon::timestamp_ntz(9) as inactivatedon,
        activestatus::number(18,0) as activestatus,
        createddate::timestamp_ntz(9) as createddate,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        file_name::varchar(50) as file_name
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %})
select * from final
