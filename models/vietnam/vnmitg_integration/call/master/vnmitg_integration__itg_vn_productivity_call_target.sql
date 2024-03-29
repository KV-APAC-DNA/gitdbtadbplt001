with edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_vn_dms_kpi as
(
    select * from {{ ref('vnmitg_integration__itg_vn_dms_kpi') }}
),
kpi as 
(
    select 
        dstrbtr_id,
        saleman_code,
        target_value,
        cycle
    from itg_vn_dms_kpi
    where kpi_type = 'PC'
),
wk_mnth as
(
    select "year",
        mnth_id,
        count(distinct cal_date) as no_of_days,
        count(distinct mnth_wk_no) as no_of_week
    from edw_vw_os_time_dim
    group by "year",
        mnth_id
),
final as
(
    SELECT 
        edw_time."year"::number(18,0) as jj_year,
        edw_time.qrtr::varchar(14) as jj_qrtr,
        edw_time.mnth_id::varchar(23) as jj_mnth_id,
        edw_time.mnth_no::number(18,0) as jj_mnth_no,
        edw_time.mnth_wk_no::number(18,0) as jj_mnth_wk_no,
        wk_mnth.no_of_week::number(18,0) as no_of_week,
        wk_mnth.no_of_days::number(18,0) as no_of_days,
        edw_time.cal_date::date as visit_date,
        kpi.dstrbtr_id::varchar(30) as dstrbtr_id,
        kpi.saleman_code::varchar(30) as salesrep_id,
        trunc((target_value / no_of_days),4)::number(20,4) as target_by_day,
        trunc((target_value / no_of_week),4)::number(20,4) as target_by_week,
        target_value::number(20,4) as target_by_month
    FROM edw_vw_os_time_dim edw_time
    left join kpi on kpi.cycle = cast(edw_time.mnth_id as numeric)
    left join wk_mnth on wk_mnth."year" = edw_time."year"
    and wk_mnth.mnth_id = edw_time.mnth_id
)
select * from final