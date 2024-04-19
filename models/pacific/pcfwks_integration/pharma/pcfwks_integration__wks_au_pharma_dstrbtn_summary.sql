with wks_au_pharma_dstrbtn_set as (
    select * from {{ ref('pcfwks_integration__wks_au_pharma_dstrbtn_set') }}
),
wks_au_pharma_dstrbtn_summary 
as (
select delvry_dt,
       acct_key,
       prod_key,
       curnt_mnth_nis,
       curnt_mnth_qty,
       curnt_mnth_avg_lp,
       lst_mnth_nis,
       lst_mnth_qty,
       lst_mnth_avg_lp,
       lst3_mnth_nis,
       lst3_mnth_qty,
       lst3_mnth_avg_lp,
       lst6_mnth_nis,
       lst6_mnth_qty,
       lst6_mnth_avg_lp,
       lst12_mnth_nis,
       lst12_mnth_qty,
       lst12_mnth_avg_lp,
       case
         when curnt_mnth_qty > 0 then 1
         else 0
       end as curnt_mnth_dstrbtn_flag,
       case
         when lst_mnth_qty > 0 then 1
         else 0
       end as lst_mnth_dstrbtn_flag,
       case
         when lst3_mnth_qty > 0 then 1
         else 0
       end as lst3_mnth_dstrbtn_flag,
       case
         when lst6_mnth_qty > 0 then 1
         else 0
       end as lst6_mnth_dstrbtn_flag,
       case
         when lst12_mnth_qty > 0 then 1
         else 0
       end as lst12_mnth_dstrbtn_flag,
       case
         when prod_curnt_mnth_qty > 0 and acct_curnt_mnth_qty > 0 then 1
         else 0
       end as curnt_mnth_dstrbtn_trgt,
       case
         when prod_lst_mnth_qty > 0 and acct_lst_mnth_qty > 0 then 1
         else 0
       end as lst_mnth_dstrbtn_trgt,
       case
         when prod_lst3_mnth_qty > 0 and acct_lst3_mnth_qty > 0 then 1
         else 0
       end as lst3_mnth_dstrbtn_trgt,
       case
         when prod_lst6_mnth_qty > 0 and acct_lst6_mnth_qty > 0 then 1
         else 0
       end as lst6_mnth_dstrbtn_trgt,
       case
         when prod_lst12_mnth_qty > 0 and acct_lst12_mnth_qty > 0 then 1
         else 0
       end as lst12_mnth_dstrbtn_trgt
from wks_au_pharma_dstrbtn_set
)
select * from wks_au_pharma_dstrbtn_summary