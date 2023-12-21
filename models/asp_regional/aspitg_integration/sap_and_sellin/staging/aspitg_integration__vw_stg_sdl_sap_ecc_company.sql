

with 
source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_company') }}

),

final as (

    select
        mandt,
        bukrs,
        land1,
        waers,
        ktopl,
        kkber,
        periv,
        rcomp,
        crt_dttm,
        updt_dttm
        from source
)

select * from final
