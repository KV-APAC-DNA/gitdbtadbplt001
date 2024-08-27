{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('phlsdl_raw','sdl_ph_clobotics_task_raw_data') }}
    where filename not in (
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__null_test')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__format_test1')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__format_test2')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_clobotics_task_raw_data__format_test3')}}
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