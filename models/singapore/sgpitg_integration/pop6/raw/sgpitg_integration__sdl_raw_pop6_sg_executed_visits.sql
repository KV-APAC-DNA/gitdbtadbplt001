
{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}
with sdl_pop6_sg_executed_visits as (
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_executed_visits') }}
    where file_name not in 
    (
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_executed_visits__duplicate_test') }}
        union all 
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_executed_visits__null_test') }}
    )
),
final as (
SELECT *
FROM sdl_pop6_sg_executed_visits
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }})
{% endif %})
select * from final