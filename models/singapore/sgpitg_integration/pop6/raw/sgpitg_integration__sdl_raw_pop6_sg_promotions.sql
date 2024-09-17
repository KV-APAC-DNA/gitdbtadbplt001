
{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}
with sdl_pop6_sg_promotions as (
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_promotions') }}
    where file_name not in 
    (
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_promotions__duplicate_test') }}
        union all
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_promotions__null_test') }}
    )
),
final as (
SELECT *
FROM sdl_pop6_sg_promotions
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }})
{% endif %})
select * from final