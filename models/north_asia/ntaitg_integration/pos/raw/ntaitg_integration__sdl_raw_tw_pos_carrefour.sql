{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_tw_pos_carrefour') }}
      where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_pos_carrefour__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_pos_carrefour__test_date_format_odd_eve_leap') }}
    ) 
     ),
     
final as (
    select *,null as run_id from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

select * from final