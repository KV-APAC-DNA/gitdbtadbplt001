with itg_direct_sales_rep_route_plan as
(
    select * from {{ ref('ntaitg_integration__itg_direct_sales_rep_route_plan') }}
),

vw_dir_route_plan_week_map as
(
    select * from {{ ref('ntaedw_integration__vw_dir_route_plan_week_map') }}
),

itg_indirect_sales_rep_route_plan as
(
    select * from {{ ref('ntaitg_integration__itg_indirect_sales_rep_route_plan') }}
),

vw_indir_route_plan_week_map as
(
    select * from {{ ref('ntaedw_integration__vw_indir_route_plan_week_map') }}
),

direct as
(
    SELECT ctry_cd,
      dstr_cd,
      sls_rep_cd,
      sls_rep_nm,
      sls_rep_typ,
      store_cd,
      store_nm,
      store_class,
      visit_jj_mnth_id,
      visit_jj_wk_num,
      visit_jj_wk_day,
      visit_Dt,
      visit_end_dt,
      effctv_strt_dt,
      effctv_end_dt,
      crt_dttm,
      updt_dttm
    FROM (
        SELECT t1.ctry_cd,
                t1.dstr_cd,
                t1.sls_rep_cd,
                t1.sls_rep_nm,
                'Direct' AS sls_rep_typ,
                t1.store_cd,
                t1.store_nm,
                t1.store_class,
                t2.jj_mnth_id AS visit_jj_mnth_id,
                t2.jj_wk_num AS visit_jj_wk_num,
                t2.wkday AS visit_jj_wk_day,
                t2.cal_day AS visit_Dt,
                LEAD(t2.cal_day, 1) OVER (
                    PARTITION BY t1.store_cd ORDER BY t2.cal_day
                    ) - 1 AS visit_end_dt,
                t1.effctv_strt_dt,
                t1.effctv_end_dt,
                t1.crt_dttm,
                t1.updt_dttm
        FROM itg_direct_sales_rep_route_plan t1,
                vw_dir_route_plan_week_map t2
        WHERE t1.ctry_cd = t2.ctry_cd
                AND t1.dstr_cd = t2.dstr_cd
                AND t1.effctv_strt_dt = t2.effctv_strt_dt
                AND DECODE(t1.effctv_end_dt, TO_DATE('9999-12-31', 'YYYY-MM-DD'), to_date(convert_timezone('UTC', current_timestamp())), t1.effctv_end_dt) = t2.effctv_end_dt
                AND (
                    CASE 
                            WHEN t1.DAY = 'Monday'
                                THEN 1
                            WHEN t1.DAY = 'Tuesday'
                                THEN 2
                            WHEN t1.DAY = 'Wednesday'
                                THEN 3
                            WHEN t1.DAY = 'Thursday'
                                THEN 4
                            WHEN t1.DAY = 'Friday'
                                THEN 5
                            END
                    ) = t2.wkday
                AND t1.week = t2.week_no
                AND t1.effctv_strt_dt < t1.effctv_end_dt
      )t1
),

indirect as
(
    SELECT ctry_cd,
      dstr_cd,
      sls_rep_cd,
      sls_rep_nm,
      sls_rep_typ,
      store_cd,
      store_nm,
      store_class,
      visit_jj_mnth_id,
      visit_jj_wk_num,
      visit_jj_wk_day,
      visit_Dt,
      visit_end_dt,
      effctv_strt_dt,
      effctv_end_dt,
      crt_dttm,
      updt_dttm
    FROM (
        SELECT t1.ctry_cd,
                t1.dstr_cd,
                t1.sls_rep_cd,
                t1.sls_rep_nm,
                'InDirect' AS sls_rep_typ,
                t1.store_cd,
                t1.store_nm,
                t1.store_class,
                t2.jj_mnth_id AS visit_jj_mnth_id,
                t2.jj_wk_num AS visit_jj_wk_num,
                t2.wkday AS visit_jj_wk_day,
                t2.cal_day AS visit_Dt,
                LEAD(t2.cal_day, 1) OVER (
                    PARTITION BY t1.store_cd ORDER BY t2.cal_day
                    ) - 1 AS visit_end_dt,
                t1.effctv_strt_dt,
                t1.effctv_end_dt,
                t1.crt_dttm,
                t1.updt_dttm
        FROM itg_indirect_sales_rep_route_plan t1,
                vw_indir_route_plan_week_map t2
        WHERE t1.ctry_cd = t2.ctry_cd
                AND t1.dstr_cd = t2.dstr_cd
                AND t1.effctv_strt_dt = t2.effctv_strt_dt
                AND DECODE(t1.effctv_end_dt, TO_DATE('9999-12-31', 'YYYY-MM-DD'), to_date(convert_timezone('UTC', current_timestamp())), t1.effctv_end_dt) = t2.effctv_end_dt
                AND (
                    CASE 
                            WHEN t1.DAY = 'Monday'
                                THEN 1
                            WHEN t1.DAY = 'Tuesday'
                                THEN 2
                            WHEN t1.DAY = 'Wednesday'
                                THEN 3
                            WHEN t1.DAY = 'Thursday'
                                THEN 4
                            WHEN t1.DAY = 'Friday'
                                THEN 5
                            END
                    ) = t2.wkday
                AND t1.week = t2.week_no
                AND t1.effctv_strt_dt < t1.effctv_end_dt
      )t1
),

transformed as
(
    select * from direct
    union all
    select * from indirect
),

final as
(
    select 
        ctry_cd::varchar(5) as ctry_cd,
        dstr_cd::varchar(10) as dstr_cd,
        sls_rep_cd::varchar(20) as sls_rep_cd,
        sls_rep_nm::varchar(50) as sls_rep_nm,
        sls_rep_typ::varchar(20) as sls_rep_typ,
        store_cd::varchar(20) as store_cd,
        store_nm::varchar(100) as store_nm,
        store_class::varchar(3) as store_class,
        visit_jj_mnth_id::number(18,0) as visit_jj_mnth_id,
        visit_jj_wk_num::number(18,0) as visit_jj_wk_no,
        visit_jj_wk_day::number(18,0) as visit_jj_wkdy_no,
        visit_dt::date as visit_dt,
        visit_end_dt::date as visit_end_dt,
        effctv_strt_dt::date as effctv_strt_dt,
        effctv_end_dt::date as effctv_end_dt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from transformed
)

select * from final
