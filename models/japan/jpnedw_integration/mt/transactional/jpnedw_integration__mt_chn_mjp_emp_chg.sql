{{
    config
    (
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{ this }} where 0 != (
                select count(*) from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_chn_mjp_emp_chg') }});
                {% endif %}"

    )
}}

with source as(
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_chn_mjp_emp_chg') }}
)
,
result as (
    select 
        cd_code::varchar(256) as chn_cd,
        franchisecode::varchar(256) as fc_cd,
        emp_cd::varchar(256) as emp_cd,
        division::varchar(256) as division,
        division_group::varchar(256) as division_group,
        current_timestamp()::timestamp_ntz(9) as update_dt,
        null::varchar(30) as update_user
    from source
)

select * from result