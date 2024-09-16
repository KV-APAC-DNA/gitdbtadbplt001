{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_jnj_consumerreach_711') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_711__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_711__test_date_format_odd_eve') }}
    )
),

final as
(
    select
        id,
        cdate,
        retail,
        retailname,
        retailbranch,
        retailprovince,
        jjskubarcode,
        jjskuname,
        jjcore,
        distribution,
        status,
        run_id,
        file_name,
        yearmo
    from source
)

select * from final