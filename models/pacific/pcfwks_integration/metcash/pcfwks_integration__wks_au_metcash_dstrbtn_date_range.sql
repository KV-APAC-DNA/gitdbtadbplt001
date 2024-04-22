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

wks_au_metcash_dstrbtn_date_range as (
--	Date Range for each Delivery Dates. We are considering last 24 Months.
--	For each Delivery Date of a Month Identify rolling Months(Dates) - Last Month, Last Three Month, Last Six Month and Last Twelve Month Dates
select distinct delvry_dt as delvry_dt,
       dateadd(month,-1,delvry_dt) lst_mnth_delvry_dt,
       dateadd(month,-3,delvry_dt) lst3_mnth_delvry_dt,
       dateadd(month,-6,delvry_dt) lst6_mnth_delvry_dt,
       dateadd(month,-12,delvry_dt) lst12_mnth_delvry_dt
from wks_au_metcash_monthly
)
select * from wks_au_metcash_dstrbtn_date_range