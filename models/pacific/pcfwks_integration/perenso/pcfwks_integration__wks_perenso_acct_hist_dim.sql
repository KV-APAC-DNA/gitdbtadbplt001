with edw_perenso_account_hist_dim_temp as (
    select * from {{ ref('pcfedw_integration__edw_perenso_account_hist_dim_temp') }}
),
edw_perenso_account_dim as (
    select * from {{ ref('pcfedw_integration__edw_perenso_account_dim') }}
),
new_rec as 
(select distinct acct_id from edw_perenso_account_hist_dim_temp) 

select distinct t.*,
       case
         when new_rec.acct_id is null then cast('2019-01-01' as date)
         when h.start_date is null then current_date
         else h.start_date
       end as start_date,
       cast('2099-12-31' as date) end_date,
       'N' hist_flg,
       case
         when h.crt_dttm is null then current_timestamp()
         else h.crt_dttm
       end as crt_dttm,
       current_timestamp() as upd_dttm
from edw_perenso_account_dim t
  left join new_rec on t.acct_id = new_rec.acct_id
  left join (select *
             from edw_perenso_account_hist_dim_temp
             where hist_flg = 'N') h
         on t.acct_id = h.acct_id
        and t.acct_store_code = h.acct_store_code