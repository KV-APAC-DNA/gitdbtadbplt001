with itg_vn_dms_kpi_sellin_sellthrgh as
(
    select * from {{ ref('vnmitg_integration__itg_vn_dms_kpi_sellin_sellthrgh') }}
),
wrk_vn_mnth_week as
(
    select * from {{ ref('vnmwks_integration__wrk_vn_mnth_week') }}
),
wrk_vn_sellin_by_wkmnth_for_target as
(
    select * from {{ ref('vnmwks_integration__wrk_vn_sellin_by_wkmnth_for_target') }}
),
t2 as
(
    select distinct p_2,
        dstrbtr_name,
        i.cycle,
        i.sellin_tg as target_value,
        w.mnth_wk_no
    from itg_vn_dms_kpi_sellin_sellthrgh i,
        wrk_vn_mnth_week w
    where ordertype = 'SELLIN'
        and dstrbtr_type = 'TD'
        and i.cycle = w.mnth_id
),
final as
(
    select 
        p_2::varchar(23) as p_2,
        nvl(st2.territory_dist, t2.dstrbtr_name)::varchar(100) as territory_dist,
        t2.cycle::number(18,0) as target_cyc,
        t2.mnth_wk_no::number(38,0) as target_wk,
        t2.target_value::number(15,4) as target_value,
        st2.mnth_id::varchar(23) as sales_mnth,
        st2.mnth_wk_no::number(38,0) as sales_wk,
        st2.amount::number(38,4) as amt_by_wk
    from t2,
        wrk_vn_sellin_by_wkmnth_for_target st2
    where st2.mnth_id (+) = t2.p_2
        and st2.territory_dist (+) = t2.dstrbtr_name
        and st2.mnth_wk_no (+) = t2.mnth_wk_no
)
select * from final