with source as (
    select *
    from {{ ref('aspitg_integration__vw_stg_sdl_sap_ecc_profit_center') }}
),

final as (
    select
        kokrs,
        prctr,
        dateto,
        datefrom,
        verak,
        waers,
        updt_dttm
    from source
)

select * from final
