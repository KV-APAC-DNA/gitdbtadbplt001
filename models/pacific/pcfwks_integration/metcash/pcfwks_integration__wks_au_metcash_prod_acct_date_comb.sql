with edw_pacific_perenso_ims_analysis as (
select * from {{ ref('pcfedw_integration__edw_pacific_perenso_ims_analysis') }}
),
edw_time_dim as (
select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_perenso_prod_dim as (
select * from {{ ref('pcfedw_integration__dly_sls_cust_attrb_lkp') }}
),
edw_perenso_account_metcash_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_metcash_dim') }}
),
edw_perenso_account_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_dim') }}
),
edw_ps_msl_items as (
select * from {{ ref('pcfedw_integration__edw_ps_msl_items') }}
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