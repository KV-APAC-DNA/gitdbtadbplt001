{{
    config(
        materialized="incremental",
        incremental_strategy="append"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_ecommerce_trexi_sellout') }} 
     where file_name not in (
        select distinct file_name from 
        {{ source('ntawks_integration', 'TRATBL_sdl_kr_ecommerce_trexi_sellout__null_test') }}
        union all
        select distinct file_name from 
        {{ source('ntawks_integration', 'TRATBL_sdl_kr_ecommerce_trexi_sellout__lookup_test') }}
    )
),
final as (
    select * from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final