{{
    config(
        alias="wks_edw_profit_center_dim",
        tags=[""]
    )
}}

with 

itg_prft_ctr as (

    select * from {{ ref('aspitg_integration__itg_prft_ctr') }}
),

itg_prft_ctr_text as (

    select * from {{ ref('aspitg_integration__itg_prft_ctr_text') }}
),

final as (
    select
    lang_key,
    cntl_area,
    prft_ctr,
    vld_to_dt,
    vld_from_dt,
    shrt_desc,
    med_desc,
    prsn_resp,
    crncy_key,
    -- null as tgt_crt_dttm,
    updt_dttm as updt_dttm
    --null as chng_flg
  from source
)

select * from final