{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}

with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_ecc_account_text') }}
),

final as (
    select
        chrt_accts,
        account,
        langu,
        txtsh,
        txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source

)

select * from final
