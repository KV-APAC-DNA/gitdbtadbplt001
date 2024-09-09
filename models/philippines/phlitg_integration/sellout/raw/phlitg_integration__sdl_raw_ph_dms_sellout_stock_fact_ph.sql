{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_ph_dms_sellout_stock_fact') }}
    where file_name not in (
    select distinct file_name from {{ source('phlwks_integration', 'TRATBL_sdl_ph_dms_sellout_stock_fact__lookup_test') }} 
    union all
    select distinct file_name from {{ source('phlwks_integration', 'TRATBL_sdl_ph_dms_sellout_stock_fact__null_test') }} )
),
final as
(
    select *, null as filename
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.curr_date > (select max(curr_date) from {{ this }}) 
    {% endif %}
)
select * from final
