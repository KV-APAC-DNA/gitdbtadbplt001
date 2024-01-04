with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_account_text') }}

),

final as (

    select
        chrt_accts,
        account,
        langu,
        txtsh,
        txtmd,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
