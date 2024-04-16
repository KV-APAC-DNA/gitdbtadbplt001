with edw_pacific_perenso_ims_analysis as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PACIFIC_PERENSO_IMS_ANALYSIS 
),
edw_time_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_TIME_DIM--matching
),
EDW_PERENSO_PROD_DIM as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_PROD_DIM--2119
),
EDW_PERENSO_ACCOUNT_METCASH_DIM as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_ACCOUNT_METCASH_DIM--3359
),
EDW_PERENSO_ACCOUNT_DIM as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_ACCOUNT_DIM--14622
),
edw_ps_msl_items as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PS_MSL_ITEMS
),
wks_au_metcash_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_metcash_monthly') }}
),
wks_au_metcash_prod_acct_comb as (
    select * from {{ ref('pcfwks_integration__wks_au_metcash_prod_acct_comb') }}
),
subquery_1 as (
(select prod_key, acct_key from wks_au_metcash_prod_acct_comb)
),
subquery_2 as (
(select distinct delvry_dt as delvry_dt
      from wks_au_metcash_monthly
      where delvry_dt >(select distinct dateadd(month,-24,eppia.delvry_dt)
                        from wks_au_metcash_monthly eppia,
                             (select max(delvry_dt) delvry_dt
                              from wks_au_metcash_monthly) maxd
                        where eppia.delvry_dt = maxd.delvry_dt))
),
wks_au_metcash_prod_acct_date_comb as (
select delvry_dt,
       prod_key,
       acct_key
from subquery_2,subquery_1
     )
select * from wks_au_metcash_prod_acct_date_comb