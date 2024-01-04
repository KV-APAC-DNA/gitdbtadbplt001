{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}

with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_0account') }}
),

final as (
    select
        chrt_accts,
        account,
        objvers,
        changed,
        bal_flag,
        cstel_flag,
        glacc_flag,
        logsys,
        sem_posit,
        zbravol1,
        zbravol2,
        zbravol3,
        zbravol4,
        zbravol5,
        glacctext,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source

)

select * from final
