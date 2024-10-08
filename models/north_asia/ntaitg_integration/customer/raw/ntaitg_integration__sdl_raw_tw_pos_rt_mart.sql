{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_rt_mart') }}
     where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_pos_rt_mart__null_test') }}
    )
),

final as
(
    select
        *,
        null as run_id
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
{% endif %} 
)

select * from final