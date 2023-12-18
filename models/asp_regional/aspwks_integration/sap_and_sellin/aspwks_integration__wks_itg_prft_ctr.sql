{{
    config(
        alias="wks_itg_prft_ctr",
        tags=["daily"]
    )
}}

with 

source as (

    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_ecc_profit_center') }}
),

final as (
    select
    kokrs,
    prctr,
    dateto,
    datefrom,
    verak,
    waers,
    -- current_timestamp() as tgt_crt_dttm,
    updt_dttm as updt_dttm
  from source
)

select * from final