{{
    config(
        alias="wks_edw_sales_org_dim",
        tags=["daily"]
    )
}}

with 

itg_sls_org as (

    select * from {{ ref('aspitg_integration__itg_sls_org') }}
),

itg_sls_org_text as (
    select * from {{ ref('aspitg_integration__itg_sls_org_text') }}
),

--Join
final_temp as (
    select
      a.*,
      b.sls_org_nm as sls_org_nm
    from itg_sls_org as a
    left outer join itg_sls_org_text as b
      on a.sls_org = b.sls_org and b.lang_key = 'E'
),

final as (
    select
    clnt,
    sls_org,
    sls_org_nm,
    stats_crncy,
    sls_org_co_cd,
    cust_no_intco_bill,
    ctry_key,
    crncy_key,
    fisc_yr_vrnt,
    -- null as tgt_crt_dttm,
    updt_dttm as updt_dttm
    --null as chng_flg
  from final_temp
)

select * from final