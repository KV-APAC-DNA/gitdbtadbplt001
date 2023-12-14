{{
    config(
        alias="wks_edw_sales_org_dim",
        tags=[""]
    )
}}

with 

source as (

    select * from {{ ref('aspitg_integration__itg_sls_org') }}
),

final as (
    select
    
    -- null as tgt_crt_dttm,
    updt_dttm as updt_dttm
    --null as chng_flg
  from source
)

select * from final