with wks_au_pharma_curnt_mnth as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_curnt_mnth') }}
),
wks_au_pharma_lst_mnth as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_lst_mnth') }}
),
wks_au_pharma_lst3_mnth as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_lst3_mnth') }}
),
wks_au_pharma_lst6_mnth as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_lst6_mnth') }}
),
wks_au_pharma_lst12_mnth as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_lst12_mnth') }}
),
wks_au_pharma_curnt_mnth_dstrbtn_check as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_curnt_mnth_dstrbtn_check') }}
),
wks_au_pharma_lst_mnth_dstrbtn_check as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_lst_mnth_dstrbtn_check') }}
),
wks_au_pharma_lst3_mnth_dstrbtn_check as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_lst3_mnth_dstrbtn_check') }}
),
wks_au_pharma_lst6_mnth_dstrbtn_check as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_lst6_mnth_dstrbtn_check') }}
),
wks_au_pharma_lst12_mnth_dstrbtn_check as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_lst12_mnth_dstrbtn_check') }}
),
wks_au_pharma_dstrbtn_set 
as (
select wapcm.delvry_dt,
       wapcm.acct_key,
       wapcm.prod_key,
       wapcm.curnt_mnth_nis,
       wapcm.curnt_mnth_qty,
       wapcm.curnt_mnth_avg_lp,
       waplm.lst_mnth_nis,
       waplm.lst_mnth_qty,
       waplm.lst_mnth_avg_lp,
       wapl3m.lst3_mnth_nis,
       wapl3m.lst3_mnth_qty,
       wapl3m.lst3_mnth_avg_lp,
       wapl6m.lst6_mnth_nis,
       wapl6m.lst6_mnth_qty,
       wapl6m.lst6_mnth_avg_lp,
       wapl12m.lst12_mnth_nis,
       wapl12m.lst12_mnth_qty,
       wapl12m.lst12_mnth_avg_lp,
       wapcmdc.prod_curnt_mnth_qty,
       wapcmdc.acct_curnt_mnth_qty,
       waplmdc.prod_lst_mnth_qty,
       waplmdc.acct_lst_mnth_qty,
       wapl3mdc.prod_lst3_mnth_qty,
       wapl3mdc.acct_lst3_mnth_qty,
       wapl6mdc.prod_lst6_mnth_qty,
       wapl6mdc.acct_lst6_mnth_qty,
       wapl12mdc.prod_lst12_mnth_qty,
       wapl12mdc.acct_lst12_mnth_qty
from wks_au_pharma_curnt_mnth wapcm,
     wks_au_pharma_lst_mnth waplm,
     wks_au_pharma_lst3_mnth wapl3m,
     wks_au_pharma_lst6_mnth wapl6m,
     wks_au_pharma_lst12_mnth wapl12m,
     wks_au_pharma_curnt_mnth_dstrbtn_check wapcmdc,
     wks_au_pharma_lst_mnth_dstrbtn_check waplmdc,
     wks_au_pharma_lst3_mnth_dstrbtn_check wapl3mdc,
     wks_au_pharma_lst6_mnth_dstrbtn_check wapl6mdc,
     wks_au_pharma_lst12_mnth_dstrbtn_check wapl12mdc
where wapcm.delvry_dt = waplm.delvry_dt
and   wapcm.acct_key = waplm.acct_key
and   wapcm.prod_key = waplm.prod_key
and   wapcm.delvry_dt = wapl3m.delvry_dt
and   wapcm.acct_key = wapl3m.acct_key
and   wapcm.prod_key = wapl3m.prod_key
and   wapcm.delvry_dt = wapl6m.delvry_dt
and   wapcm.acct_key = wapl6m.acct_key
and   wapcm.prod_key = wapl6m.prod_key
and   wapcm.delvry_dt = wapl12m.delvry_dt
and   wapcm.acct_key = wapl12m.acct_key
and   wapcm.prod_key = wapl12m.prod_key
and   wapcm.delvry_dt = wapcmdc.delvry_dt
and   wapcm.acct_key = wapcmdc.acct_key
and   wapcm.prod_key = wapcmdc.prod_key
and   wapcm.delvry_dt = waplmdc.delvry_dt
and   wapcm.acct_key = waplmdc.acct_key
and   wapcm.prod_key = waplmdc.prod_key
and   wapcm.delvry_dt = wapl3mdc.delvry_dt
and   wapcm.acct_key = wapl3mdc.acct_key
and   wapcm.prod_key = wapl3mdc.prod_key
and   wapcm.delvry_dt = wapl6mdc.delvry_dt
and   wapcm.acct_key = wapl6mdc.acct_key
and   wapcm.prod_key = wapl6mdc.prod_key
and   wapcm.delvry_dt = wapl12mdc.delvry_dt
and   wapcm.acct_key = wapl12mdc.acct_key
and   wapcm.prod_key = wapl12mdc.prod_key
)
select * from wks_au_pharma_dstrbtn_set