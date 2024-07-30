{{
    config
    (
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{ this }} where 0 != (
                select count(*) from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_frnch_cdd') }});
                {% endif %}"

    )
}}


with source as (
    select * from {{ source('jpnsdl_raw','sdl_mds_jp_mt_frnch_cdd') }}
),

result as(
    select 
        code::varchar(18) as ph_cd,
        ph_lvl::varchar(2) as ph_lvl,
        name::varchar(40) as ph_nm,
        ph_srt::varchar(18) as ph_srt,
        current_timestamp()::timestamp_ntz(9) as update_dt,
        NULL::varchar(30) as update_user
    from source
)

select * from result

