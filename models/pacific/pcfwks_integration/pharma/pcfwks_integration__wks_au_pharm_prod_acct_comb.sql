{{
config(
    materialized='table',
    cluster_by= ['delvry_dt','prod_key','acct_key','unit_qty']
  )
}}


with edw_pacific_perenso_ims_analysis as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PACIFIC_PERENSO_IMS_ANALYSIS
),
edw_time_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_TIME_DIM
),
EDW_PERENSO_PROD_PROBEID_DIM as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_PROD_PROBEID_DIM
),
edw_perenso_account_probeid_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_ACCOUNT_PROBEID_DIM
),
EDW_PERENSO_PROD_DIM as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_PROD_DIM--2119
),
EDW_PERENSO_ACCOUNT_DIM as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_ACCOUNT_DIM--14622
),
edw_ps_msl_items as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PS_MSL_ITEMS
),
wks_au_pharm_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_monthly') }}
),
wks_au_pharm_prod_acct_comb 
as (
select *
from (select distinct prod_key::varchar as prod_key
      from edw_perenso_prod_probeid_dim
      -- all the products from dimension
      union
      select distinct prod_key::varchar as prod_key
      from wks_au_pharm_monthly
      -- all the products from the fact
      where delvry_dt >(select distinct dateadd(month,-24,eppia.delvry_dt)
                        from wks_au_pharm_monthly eppia,
                             (select max(delvry_dt) delvry_dt
                              from wks_au_pharm_monthly) maxd
                        where eppia.delvry_dt = maxd.delvry_dt)) prod,
     (select distinct acct_id::varchar as acct_key
      from edw_perenso_account_probeid_dim
      -- all the stores from dimension
      union
      select distinct acct_key::varchar as acct_key
      from wks_au_pharm_monthly
      -- all the stores from fact
      where delvry_dt >(select distinct dateadd(month,-24,eppia.delvry_dt)
                        from wks_au_pharm_monthly eppia,
                             (select max(delvry_dt) delvry_dt
                              from wks_au_pharm_monthly) maxd
                        where eppia.delvry_dt = maxd.delvry_dt)) acct
)
select * from wks_au_pharm_prod_acct_comb