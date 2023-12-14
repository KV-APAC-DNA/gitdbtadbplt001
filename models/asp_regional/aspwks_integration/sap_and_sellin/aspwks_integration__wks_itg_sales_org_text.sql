{{
    config(
        alias="wks_itg_sales_org_text",
        tags=[""]
    )
}}

with 

source as (

    select * from {{ ref('aspitg_integration__stg_sdl_sap_ecc_sales_org_text') }}
),

final as (
    select
    mandt,
    spras,
    vkorg,
    vtext,
    -- current_timestamp() as tgt_crt_dttm,
    updt_dttm as updt_dttm
  from source
)

select * from final