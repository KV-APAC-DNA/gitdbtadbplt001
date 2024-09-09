{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with sdl_ph_iop_trgt as (
    select *
    from {{ source('phlsdl_raw', 'sdl_ph_iop_trgt') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_iop_trgt__null_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_iop_trgt__duplicate_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_iop_trgt__format_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_iop_trgt__lookup_test_brand')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_iop_trgt__lookup_test_segment')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_iop_trgt__lookup_test_customer_code')}}
    )
),
final as (
    select *
    from sdl_ph_iop_trgt 
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
 )
select * from final 