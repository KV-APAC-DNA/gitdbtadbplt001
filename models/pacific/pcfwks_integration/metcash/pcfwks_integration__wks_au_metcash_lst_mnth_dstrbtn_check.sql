with wks_au_metcash_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_metcash_monthly') }}
),
wks_au_metcash_prod_acct_date_comb as (
    select * from {{ ref('pcfwks_integration__wks_au_metcash_prod_acct_date_comb') }}
),
wks_au_metcash_dstrbtn_date_range as (
    select * from {{ ref('pcfwks_integration__wks_au_metcash_dstrbtn_date_range') }}
),
subquery_1 as (SELECT a.delvry_dt,
       a.acct_key,
       a.prod_key,
       COUNT(DISTINCT CASE WHEN c1.unit_qty <> 0 THEN c1.delvry_dt END) AS prod_lst_mnth_qty,
       COUNT(DISTINCT CASE WHEN c2.unit_qty <> 0 THEN c2.delvry_dt END) AS acct_lst_mnth_qty
FROM wks_au_metcash_prod_acct_date_comb a
JOIN wks_au_metcash_dstrbtn_date_range b ON a.delvry_dt = b.delvry_dt
LEFT JOIN wks_au_metcash_monthly c1 ON c1.prod_key = a.prod_key
                                      AND c1.delvry_dt > b.lst_mnth_delvry_dt
                                      AND c1.delvry_dt <= b.delvry_dt
LEFT JOIN wks_au_metcash_monthly c2 ON c2.acct_key = a.acct_key
                                      AND c2.delvry_dt > b.lst_mnth_delvry_dt
                                      AND c2.delvry_dt <= b.delvry_dt
GROUP BY a.delvry_dt, a.acct_key, a.prod_key
),
wks_au_metcash_lst_mnth_dstrbtn_check as (
select delvry_dt,
       acct_key,
       prod_key,
       prod_lst_mnth_qty,
       acct_lst_mnth_qty
from subquery_1
where delvry_dt >(select distinct dateadd(month,-24,wapm.delvry_dt)
                  from wks_au_metcash_monthly wapm,
                       (select max(delvry_dt) delvry_dt
                        from wks_au_metcash_monthly) maxd
                  where wapm.delvry_dt = maxd.delvry_dt)
)
select * from wks_au_metcash_lst_mnth_dstrbtn_check