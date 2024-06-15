with itg_direct_sales_rep_route_plan as
(
    select * from {{ ref('ntaitg_integration__itg_direct_sales_rep_route_plan') }}
),

v_intrm_calendar_ims as
(
    select * from {{ ref('ntaedw_integration__v_intrm_calendar_ims') }}
),
t1 as
(
      select distinct ctry_cd,
            dstr_cd,
            effctv_strt_dt,
            decode(effctv_end_dt, to_date('9999-12-31', 'YYYY-MM-DD'), 
            to_date(convert_timezone('UTC', current_timestamp())), 
            effctv_end_dt) as effctv_end_dt
      from itg_direct_sales_rep_route_plan
),
t2 as
(
    select cal_day,
            fisc_wk_num as jj_wk_num,
            wkday,
            cast((substring(fisc_per, 1, 4) || substring(fisc_per, 6, 2)) as integer) as jj_mnth_id
    from v_intrm_calendar_ims
),
transformed as
(
    select 
        t1.ctry_cd,
        t1.dstr_cd,
        t1.effctv_strt_dt,
        t1.effctv_end_dt,
        t2.cal_day,
        t2.jj_wk_num,
        t2.wkday,
        t2.jj_mnth_id,
        case 
                when (
                            dense_rank() over (
                                partition by t1.effctv_strt_dt order by jj_wk_num
                                )
                            ) % 4 = 0
                    then 4
                else (
                            dense_rank() over (
                                partition by t1.effctv_strt_dt order by jj_wk_num
                                )
                            ) % 4
                end as week_no
    from t1,t2
    where t2.cal_day between t1.effctv_strt_dt
            and t1.effctv_end_dt
    order by ctry_cd,
        dstr_cd,
        effctv_strt_dt,
        effctv_end_dt,
        wkday,
        week_no
),
final as
(
    select
        ctry_cd::varchar(5) as ctry_cd,
        dstr_cd::varchar(10) as dstr_cd,
        effctv_strt_dt::date as effctv_strt_dt,
        effctv_end_dt::date as effctv_end_dt,
        cal_day::date as cal_day,
        jj_wk_num::number(38,0) as jj_wk_num,
        wkday::number(18,0) as wkday,
        jj_mnth_id::number(18,0) as jj_mnth_id,
        week_no::number(38,0) as week_no
    from transformed
)
select * from final