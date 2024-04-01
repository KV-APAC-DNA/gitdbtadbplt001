{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_jnj_consumerreach_711') }}
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