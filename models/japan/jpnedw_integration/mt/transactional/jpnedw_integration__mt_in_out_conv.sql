{{
    config
    (
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{ this }} where 0 != (
                select count(*) from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_in_out_conv') }});
                {% endif %}"

    )
}}


with source as (
    select * from {{ source('jpnsdl_raw','sdl_mds_jp_mt_in_out_conv') }}
),

result as(
    select 
        SLD_TO::varchar(10) AS SLD_TO,
        STR_CD::varchar(9) AS STR_CD,
        BL_DT_FROM::varchar(8) AS BL_DT_FROM,
        BL_DT_TO::varchar(8) AS BL_DT_TO,
        current_timestamp()::timestamp_ntz(9) as update_dt,
        NULL::varchar(30) as update_user
    from source
)

select * from result

