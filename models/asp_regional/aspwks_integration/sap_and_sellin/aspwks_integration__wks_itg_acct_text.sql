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
        updt_dttm
    from source

)

select * from final
