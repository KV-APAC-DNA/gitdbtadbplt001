{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['file_name']
    )
}}

with source as
(
    select *, dense_rank() over(PARTITION BY null order by file_name desc) as rnk from {{ source('thasdl_raw', 'sdl_jnj_consumerreach_sfm') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_sfm__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_sfm__test_date_format_odd_eve') }}
    ) qualify rnk =1
),

final as
(
    select
        id::varchar(255) as id,
        cdate::varchar(255) as cdate,
        retail::varchar(255) as retail,
        retailname::varchar(255) as retailname,
        retailbranch::varchar(255) as retailbranch,
        retailprovince::varchar(255) as retailprovince,
        jjskubarcode::varchar(255) as jjskubarcode,
        jjskuname::varchar(255) as jjskuname,
        jjcore::varchar(255) as jjcore,
        distribution::varchar(255) as distribution,
        status::varchar(255) as status,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        yearmo::varchar(255) as yearmo,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)

select * from final