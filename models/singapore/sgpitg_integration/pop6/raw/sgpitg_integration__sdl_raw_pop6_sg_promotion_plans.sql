
{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}
with sdl_pop6_sg_promotion_plans as (
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_promotion_plans') }}
    where file_name not in 
    (
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_promotion_plans__duplicate_test') }}
        union all 
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_promotion_plans__null_test')}}
    )
),
final as (
SELECT *
FROM sdl_pop6_sg_promotion_plans
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }})
{% endif %})
select * from final