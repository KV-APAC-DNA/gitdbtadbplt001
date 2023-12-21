with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_ecc_sales_org') }}
),

final as (
    select
        mandt,
        vkorg,
        waers,
        bukrs,
        kunnr,
        land1,
        waers1,
        periv,
        -- current_timestamp() as tgt_crt_dttm,
        updt_dttm
    from source
)

select * from final
