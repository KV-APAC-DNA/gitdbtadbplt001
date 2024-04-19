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
wks_au_pharm_prod_acct_comb as (
    select * from {{ ref('pcfwks_integration__wks_au_pharm_prod_acct_comb') }}
),
--	combination of all peresno products, accounts and last 24 dates(months)
-- 	the output would be cartesian product of perenso products, perenso stores & 24 dates.
-- 	(distinct number of perenso product id's)*(distinct number of perenso account id's)*(delivery dates)
subquery_1 as ((select prod_key, acct_key from wks_au_pharm_prod_acct_comb)),

wks_au_pharm_prod_acct_date_comb
as (
(select delvry_dt,
       prod_key,
       acct_key
from (select distinct delvry_dt as delvry_dt
      from wks_au_pharm_monthly
      where delvry_dt >(select distinct dateadd(month,-24,eppia.delvry_dt)
                        from wks_au_pharm_monthly eppia,
                             (select max(delvry_dt) delvry_dt
                              from wks_au_pharm_monthly) maxd
                        where eppia.delvry_dt = maxd.delvry_dt)),
     subquery_1)
) 
select * from wks_au_pharm_prod_acct_date_comb