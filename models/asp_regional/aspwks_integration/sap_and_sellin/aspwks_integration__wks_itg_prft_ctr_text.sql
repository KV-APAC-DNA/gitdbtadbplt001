with source as (
    select *
    from {{ ref('aspitg_integration__vw_stg_sdl_sap_ecc_profit_center_text') }}
),

final as (
    select
        langu,
        kokrs,
        prctr,
        dateto,
        datefrom,
        txtsh,
        txtmd,
        updt_dttm
    from source
)

select * from final
