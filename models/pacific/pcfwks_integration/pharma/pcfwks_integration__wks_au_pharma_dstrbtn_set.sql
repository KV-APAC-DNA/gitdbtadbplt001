with wks_au_pharma_dstrbtn_set_subtable1 as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_dstrbtn_set_subtable1') }}
),
wks_au_pharma_dstrbtn_set_subtable4 as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_dstrbtn_set_subtable4') }}
),
wks_au_pharma_dstrbtn_set as (
select a.delvry_dt,
       a.acct_key,
       a.prod_key,
       a.curnt_mnth_nis,
       a.curnt_mnth_qty,
       a.curnt_mnth_avg_lp,
       a.lst_mnth_nis,
       a.lst_mnth_qty,
       a.lst_mnth_avg_lp,
       a.lst3_mnth_nis,
       a.lst3_mnth_qty,
       a.lst3_mnth_avg_lp,
       a.lst6_mnth_nis,
       a.lst6_mnth_qty,
       a.lst6_mnth_avg_lp,
       a.lst12_mnth_nis,
       a.lst12_mnth_qty,
       a.lst12_mnth_avg_lp,
       b.prod_curnt_mnth_qty,
       b.acct_curnt_mnth_qty,
       b.prod_lst_mnth_qty,
       b.acct_lst_mnth_qty,
       b.prod_lst3_mnth_qty,
       b.acct_lst3_mnth_qty,
       b.prod_lst6_mnth_qty,
       b.acct_lst6_mnth_qty,
       b.prod_lst12_mnth_qty,
       b.acct_lst12_mnth_qty
       from wks_au_pharma_dstrbtn_set_subtable1 a
       inner join wks_au_pharma_dstrbtn_set_subtable4  b
       on 
       a.delvry_dt = b.delvry_dt
and   a.acct_key = b.acct_key
and   a.prod_key = b.prod_key
)
select * from wks_au_pharma_dstrbtn_set