{{
    config(
        alias= "wks_itg_brnd_text",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        tags= ["daily"]
    )
}}


--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_ecc_brand_text') }}
),

--Logical CTE

--Final CTE
final as (
    select
        mandt,
        spras,
        mvgr5,
        bezei,
        --tgt.crt_dttm as tgt_crt_dttm,
        updt_dttm
        --case when tgt.crt_dttm is null then 'i' else 'u' end as chng_flg
  from source
)

--Final select
select * from source