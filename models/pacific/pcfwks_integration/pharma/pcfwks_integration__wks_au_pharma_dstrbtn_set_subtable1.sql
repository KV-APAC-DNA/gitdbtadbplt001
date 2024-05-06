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
wks_au_pharma_dstrbtn_set_1 as (
select wapcm.delvry_dt,
       wapcm.acct_key,
       wapcm.prod_key,
       wapcm.curnt_mnth_nis,
       wapcm.curnt_mnth_qty,
       wapcm.curnt_mnth_avg_lp,
       waplm.lst_mnth_nis,
       waplm.lst_mnth_qty,
       waplm.lst_mnth_avg_lp
from wks_au_pharma_curnt_mnth wapcm,
     wks_au_pharma_lst_mnth waplm
where wapcm.delvry_dt = waplm.delvry_dt
and   wapcm.acct_key = waplm.acct_key
and   wapcm.prod_key = waplm.prod_key
) ,
wks_au_pharma_dstrbtn_set_2 as (
select 
       wap_1.delvry_dt,
       wap_1.acct_key,
       wap_1.prod_key,
       wap_1.curnt_mnth_nis,
       wap_1.curnt_mnth_qty,
       wap_1.curnt_mnth_avg_lp,
       wap_1.lst_mnth_nis,
       wap_1.lst_mnth_qty,
       wap_1.lst_mnth_avg_lp,
       wapl3m.lst3_mnth_nis,
       wapl3m.lst3_mnth_qty,
       wapl3m.lst3_mnth_avg_lp

from wks_au_pharma_dstrbtn_set_1 wap_1,
     wks_au_pharma_lst3_mnth wapl3m
  where   wap_1.delvry_dt = wapl3m.delvry_dt
and   wap_1.acct_key = wapl3m.acct_key
and   wap_1.prod_key = wapl3m.prod_key
),
wks_au_pharma_dstrbtn_set_3 as (
select wap_2.delvry_dt,
       wap_2.acct_key,
       wap_2.prod_key,
       wap_2.curnt_mnth_nis,
       wap_2.curnt_mnth_qty,
       wap_2.curnt_mnth_avg_lp,
       wap_2.lst_mnth_nis,
       wap_2.lst_mnth_qty,
       wap_2.lst_mnth_avg_lp,
       wap_2.lst3_mnth_nis,
       wap_2.lst3_mnth_qty,
       wap_2.lst3_mnth_avg_lp,
       wapl6m.lst6_mnth_nis,
       wapl6m.lst6_mnth_qty,
       wapl6m.lst6_mnth_avg_lp,
       from wks_au_pharma_dstrbtn_set_2 wap_2,
     wks_au_pharma_lst6_mnth wapl6m
  where   wap_2.delvry_dt = wapl6m.delvry_dt
and   wap_2.acct_key = wapl6m.acct_key
and   wap_2.prod_key = wapl6m.prod_key
),
wks_au_pharma_dstrbtn_set_4 as (
select wap_3.delvry_dt,
       wap_3.acct_key,
       wap_3.prod_key,
       wap_3.curnt_mnth_nis,
       wap_3.curnt_mnth_qty,
       wap_3.curnt_mnth_avg_lp,
       wap_3.lst_mnth_nis,
       wap_3.lst_mnth_qty,
       wap_3.lst_mnth_avg_lp,
       wap_3.lst3_mnth_nis,
       wap_3.lst3_mnth_qty,
       wap_3.lst3_mnth_avg_lp,
       wap_3.lst6_mnth_nis,
       wap_3.lst6_mnth_qty,
       wap_3.lst6_mnth_avg_lp,
       wapl12m.lst12_mnth_nis,
       wapl12m.lst12_mnth_qty,
       wapl12m.lst12_mnth_avg_lp
       from wks_au_pharma_dstrbtn_set_3 wap_3,
     wks_au_pharma_lst12_mnth wapl12m
  where   wap_3.delvry_dt = wapl12m.delvry_dt
and   wap_3.acct_key = wapl12m.acct_key
and   wap_3.prod_key = wapl12m.prod_key
) 
select * from wks_au_pharma_dstrbtn_set_4