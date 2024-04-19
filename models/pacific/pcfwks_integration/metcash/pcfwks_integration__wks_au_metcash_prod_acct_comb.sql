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
--	Combination of all Peresno Products and Accounts. 
-- 	The Output would be Cartesian Product of Perenso Products and Perenso Stores.
-- 	(Distinct Number of Perenso Product ID's)*(Distinct Number of Perenso Account ID's)
select *
from (select distinct prod_key::varchar as prod_key
      from edw_perenso_prod_dim
	  where prod_metcash_code is not null
	  and   prod_metcash_code != 'NOT ASSIGNED'
      -- all the products from dimension where metcash code is available for perenso prod-key
      union
      select distinct prod_key::varchar as prod_key
      from wks_au_metcash_monthly
      -- all the products from the fact
      where delvry_dt >(select distinct dateadd(month,-24,eppia.delvry_dt)
                        from wks_au_metcash_monthly eppia,
                             (select max(delvry_dt) delvry_dt
                              from wks_au_metcash_monthly) maxd
                        where eppia.delvry_dt = maxd.delvry_dt)) prod,
     (select distinct acct_id::varchar as acct_key
      from edw_perenso_account_metcash_dim
      -- all the stores from dimension
      union
      select distinct acct_key::varchar as acct_key
      from wks_au_metcash_monthly
      -- all the stores from fact
      where delvry_dt >(select distinct dateadd(month,-24,eppia.delvry_dt)
                        from wks_au_metcash_monthly eppia,
                             (select max(delvry_dt) delvry_dt
                              from wks_au_metcash_monthly) maxd
                        where eppia.delvry_dt = maxd.delvry_dt)) acct
                        )
select * from wks_au_metcash_prod_acct_comb