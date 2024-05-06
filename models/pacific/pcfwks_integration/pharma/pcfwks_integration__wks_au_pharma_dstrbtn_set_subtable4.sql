with wks_au_pharma_dstrbtn_set_subtable2 as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_dstrbtn_set_subtable2') }}
),
wks_au_pharma_dstrbtn_set_subtable3 as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_dstrbtn_set_subtable3') }}
),
wks_au_pharma_dstrbtn_set_subtable4 as (
select 
a.delvry_dt,
       a.acct_key,
       a.prod_key,
       a.prod_curnt_mnth_qty,
       a.acct_curnt_mnth_qty,
       a.prod_lst_mnth_qty,
       a.acct_lst_mnth_qty,
       a.prod_lst3_mnth_qty,
       a.acct_lst3_mnth_qty,
       b.prod_lst6_mnth_qty,
       b.acct_lst6_mnth_qty,
       b.prod_lst12_mnth_qty,
       b.acct_lst12_mnth_qty
from wks_au_pharma_dstrbtn_set_subtable2 a
inner join wks_au_pharma_dstrbtn_set_subtable3 b on
a.delvry_dt = b.delvry_dt
and   a.acct_key = b.acct_key
and   a.prod_key = b.prod_key
)
select * from wks_au_pharma_dstrbtn_set_subtable4