{{
    config(
        alias="wks_itg_prft_ctr_text",
        tags=["daily"]
    )
}}

with 

source as (

    select * from {{ ref('aspitg_integration__stg_sdl_sap_ecc_profit_center_text') }}
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
    -- current_timestamp() as tgt_crt_dttm,
    updt_dttm as updt_dttm
  from source
)

select * from final