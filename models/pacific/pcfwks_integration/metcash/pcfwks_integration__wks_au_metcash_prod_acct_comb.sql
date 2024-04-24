-- /*	Combination of all Peresno Products and Accounts. 
-- 	The Output would be Cartesian Product of Perenso Products and Perenso Stores.
-- 	(Distinct Number of Perenso Product ID's)*(Distinct Number of Perenso Account ID's) 
    --   -- all the products from dimension where metcash code is available for perenso prod-key
    --   -- all the stores from dimension
    --   -- all the products from the fact
    --   -- all the stores from fact
    --   */
with edw_pacific_perenso_ims_analysis as (
select * from {{ ref('pcfedw_integration__edw_pacific_perenso_ims_analysis') }}
),
edw_time_dim as (
select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_perenso_prod_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_prod_dim') }}
),
edw_perenso_account_metcash_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_metcash_dim') }}
),
wks_au_metcash_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_metcash_monthly') }}
),
wks_au_metcash_prod_acct_comb as (
select *
from (select distinct prod_key::varchar as prod_key
      from edw_perenso_prod_dim
	  where prod_metcash_code is not null
	  and   prod_metcash_code != 'NOT ASSIGNED'
      union
      select distinct prod_key::varchar as prod_key
      from wks_au_metcash_monthly

      where delvry_dt >(select distinct dateadd(month,-24,eppia.delvry_dt)
                        from wks_au_metcash_monthly eppia,
                             (select max(delvry_dt) delvry_dt
                              from wks_au_metcash_monthly) maxd
                        where eppia.delvry_dt = maxd.delvry_dt)) prod,
     (select distinct acct_id::varchar as acct_key
      from edw_perenso_account_metcash_dim

      union
      select distinct acct_key::varchar as acct_key
      from wks_au_metcash_monthly

      where delvry_dt >(select distinct dateadd(month,-24,eppia.delvry_dt)
                        from wks_au_metcash_monthly eppia,
                             (select max(delvry_dt) delvry_dt
                              from wks_au_metcash_monthly) maxd
                        where eppia.delvry_dt = maxd.delvry_dt)) acct
                        )
select * from wks_au_metcash_prod_acct_comb