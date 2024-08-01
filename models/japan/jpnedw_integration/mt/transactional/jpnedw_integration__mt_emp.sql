{{
    config
    (
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{ this }} where 0 != (
                select count(*) from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_emp') }});
                {% endif %}"

    )
}}


with source as (
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_emp') }}
),
result as(
    select
        code::varchar(8) as emp_cd,
        emp_nm_knj::varchar(80) as emp_nm_knj,
        name::varchar(80) as emp_nm,
        wwid::varchar(20) as wwid,
        slm_cd::varchar(8) as slm_cd,
        org_cd::varchar(10) as org_cd,
        cstctr_cd::varchar(4) as cstctr_cd,
        emp_typ::varchar(2) as emp_typ,
        current_timestamp()::timestamp_ntz(9) as update_dt,
        NULL::varchar(30) as update_user
    from source
)

select * from result
