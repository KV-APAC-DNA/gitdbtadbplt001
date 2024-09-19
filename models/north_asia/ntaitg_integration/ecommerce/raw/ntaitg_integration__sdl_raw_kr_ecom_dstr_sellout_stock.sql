{{
    config(
        materialized="incremental",
        incremental_strategy="append"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_ecom_dstr_sellout_stock') }} 
       where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_ecom_dstr_sellout_stock__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_ecom_dstr_sellout_stock__lookup_test_sap') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_ecom_dstr_sellout_stock__lookup_test_dstr_cd') }}
     )
),
final as (
    select * from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final