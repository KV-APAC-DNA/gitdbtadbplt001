with wks_au_pharma_curnt_mnth as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_curnt_mnth') }}
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
wks_au_pharma_dstrbtn_set_5 as (
select wapcm.delvry_dt,
       wapcm.acct_key,
       wapcm.prod_key,
       wapcmdc.prod_curnt_mnth_qty,
       wapcmdc.acct_curnt_mnth_qty,
from 
wks_au_pharma_curnt_mnth wapcm,
wks_au_pharma_curnt_mnth_dstrbtn_check wapcmdc
  where   wapcm.delvry_dt = wapcmdc.delvry_dt
and   wapcm.acct_key = wapcmdc.acct_key
and   wapcm.prod_key = wapcmdc.prod_key
) ,
wks_au_pharma_dstrbtn_set_6 as (
select wap_5.delvry_dt,
       wap_5.acct_key,
       wap_5.prod_key,
       wap_5.prod_curnt_mnth_qty,
       wap_5.acct_curnt_mnth_qty,
       waplmdc.prod_lst_mnth_qty,
       waplmdc.acct_lst_mnth_qty,
from 
wks_au_pharma_dstrbtn_set_5 wap_5,
wks_au_pharma_lst_mnth_dstrbtn_check waplmdc
  where   wap_5.delvry_dt = waplmdc.delvry_dt
and   wap_5.acct_key = waplmdc.acct_key
and   wap_5.prod_key = waplmdc.prod_key
),
wks_au_pharma_dstrbtn_set_7 as (
select wap_6.delvry_dt,
       wap_6.acct_key,
       wap_6.prod_key,
       wap_6.prod_curnt_mnth_qty,
       wap_6.acct_curnt_mnth_qty,
       wap_6.prod_lst_mnth_qty,
       wap_6.acct_lst_mnth_qty,
       wapl3mdc.prod_lst3_mnth_qty,
       wapl3mdc.acct_lst3_mnth_qty
from 
wks_au_pharma_dstrbtn_set_6 wap_6,
wks_au_pharma_lst3_mnth_dstrbtn_check wapl3mdc
  where   wap_6.delvry_dt = wapl3mdc.delvry_dt
and   wap_6.acct_key = wapl3mdc.acct_key
and   wap_6.prod_key = wapl3mdc.prod_key
) 
select * from wks_au_pharma_dstrbtn_set_7