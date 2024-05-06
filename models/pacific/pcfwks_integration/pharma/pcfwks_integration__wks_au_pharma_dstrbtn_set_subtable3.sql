with wks_au_pharma_curnt_mnth as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_curnt_mnth') }}
),
wks_au_pharma_lst6_mnth_dstrbtn_check as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_lst6_mnth_dstrbtn_check') }}
),
wks_au_pharma_lst12_mnth_dstrbtn_check as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_lst12_mnth_dstrbtn_check') }}
),
wks_au_pharma_dstrbtn_set_8 as (
select wapcm.delvry_dt,
       wapcm.acct_key,
       wapcm.prod_key,
       wapl6mdc.prod_lst6_mnth_qty,
       wapl6mdc.acct_lst6_mnth_qty,
from 
wks_au_pharma_curnt_mnth wapcm,
wks_au_pharma_lst6_mnth_dstrbtn_check wapl6mdc
  where   wapcm.delvry_dt = wapl6mdc.delvry_dt
and   wapcm.acct_key = wapl6mdc.acct_key
and   wapcm.prod_key = wapl6mdc.prod_key
),
wks_au_pharma_dstrbtn_set_9 as (
select wap_8.delvry_dt,
       wap_8.acct_key,
       wap_8.prod_key,
       wap_8.prod_lst6_mnth_qty,
       wap_8.acct_lst6_mnth_qty,
       wapl12mdc.prod_lst12_mnth_qty,
       wapl12mdc.acct_lst12_mnth_qty
from 
wks_au_pharma_dstrbtn_set_8 wap_8,
wks_au_pharma_lst12_mnth_dstrbtn_check wapl12mdc
  where   wap_8.delvry_dt = wapl12mdc.delvry_dt
and   wap_8.acct_key = wapl12mdc.acct_key
and   wap_8.prod_key = wapl12mdc.prod_key
) 
select * from wks_au_pharma_dstrbtn_set_9