with edw_vw_vn_sellout_sales_fact as(
    select * from {{ ref('vnmedw_integration__edw_vw_vn_sellout_sales_fact') }} 
),
wrk_vn_mnth_week as(
    select * from {{ ref('vnmwks_integration__wrk_vn_mnth_week') }}
),
transformed as(
    select
        dstrbtr_grp_cd as dstrbtr_id,
        slsmn_cd as salesrep_id,
        w.mnth_id,
        mnth_wk_no,
        sum(jj_net_sls) as amount
    from edw_vw_vn_sellout_sales_fact as f, wrk_vn_mnth_week as w
    where
    f.bill_date between to_date(w.frst_day, 'YYYYMMDD')::date and to_date(w.lst_day, 'YYYYMMDD')
    and jj_net_sls > 0
    group by
    dstrbtr_id,
    salesrep_id,
    w.mnth_id,
    mnth_wk_no
)
select * from transformed