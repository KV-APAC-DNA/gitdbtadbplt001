{{
    config
    (
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{ this }} where 0 != (
                select count(*) from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_item_cdd') }});
                {% endif %}"

    )
}}


with source as (
    select * from {{ source('jpnsdl_raw','sdl_mds_jp_mt_item_cdd') }}
),

result as(
    select 
        item_cd::varchar(18) as item_cd,
        null::number(5,0) as pc,
        null::number(6,0) as unit_prc,
        jpcd_ph::varchar(18) as jpcd_ph,
        jan_cd::varchar(18) as jan_cd,
        current_timestamp()::timestamp_ntz(9) as update_dt,
        null::varchar(30) as update_user
    from source
)

select * from result

