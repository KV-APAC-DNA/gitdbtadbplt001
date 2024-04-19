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