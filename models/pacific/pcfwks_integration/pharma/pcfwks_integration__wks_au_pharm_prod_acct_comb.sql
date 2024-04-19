
with edw_pacific_perenso_ims_analysis as (
select * from {{ ref('pcfedw_integration__edw_pacific_perenso_ims_analysis') }}
),
edw_time_dim as (
select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_perenso_prod_probeid_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_prod_probeid_dim') }}
),
edw_perenso_account_probeid_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_probeid_dim') }}
),
edw_perenso_prod_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_prod_dim') }}
),
edw_perenso_account_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_dim') }}
),
edw_ps_msl_items as (
select * from {{ ref('pcfedw_integration__edw_ps_msl_items') }}
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