with wrk_vn_mnth_week as 
(
    select * from {{ ref('vnmwks_integration__wrk_vn_mnth_week') }}
),
edw_vw_vn_sellthrgh_sales_fact as
(
    select * from {{ ref('vnmedw_integration__edw_vw_vn_sellthrgh_sales_fact') }}
),
itg_vn_dms_distributor_dim as
(
    select * from {{ ref('vnmitg_integration__itg_vn_dms_distributor_dim') }}
),

sellth as 
(
        select *
        from edw_vw_vn_sellthrgh_sales_fact
        where dstrbtr_id not in(
                select distinct dstrbtr_id
                from itg_vn_dms_distributor_dim
                where dstrbtr_type = 'SPK'
            )
            and jj_net_trd_sls > 0
),
final as 
(
select 
    dstrbtr_id::varchar(30) as dstrbtr_id,
    mapped_spk::varchar(30) as mapped_spk,
    w.mnth_id::varchar(23) as mnth_id,
    mnth_wk_no::number(38,0) as mnth_wk_no,
    cast (sum(sellth.jj_net_trd_sls) as numeric(21, 8)) amount
from  sellth,
    wrk_vn_mnth_week w
where sellth.bill_date between to_date(w.frst_day,'YYYYMMDD')
     and to_date(w.lst_day,'YYYYMMDD')
    and dstrbtr_id != 'TTHNoi' 
group by dstrbtr_id,
    mapped_spk,
    w.mnth_id,
    mnth_wk_no
)
select * from final