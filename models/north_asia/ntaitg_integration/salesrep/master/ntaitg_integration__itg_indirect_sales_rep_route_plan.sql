{{
    config
    (
        materialized ='incremental',
        incremental_strategy = 'append',
        pre_hook = "update {{this}} a
                    set updt_dttm = convert_timezone('UTC', current_timestamp())::timestamp_ntz(9),
                        effctv_end_dt = (
                                select max(file_eff_dt) - 1
                                from {{ ref('ntawks_integration__wks_indirect_sales_rep_route_plan') }}
                                )
                    from (
                        select t1.*
                        from (
                                select *
                                from {{this}}
                                where effctv_end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
                                ) t1,        
                                (
                                    select *
                                    from {{ ref('ntawks_integration__wks_indirect_sales_rep_route_plan') }}
                                    where file_eff_dt = (
                                                select max(file_eff_dt)
                                                from {{ ref('ntawks_integration__wks_indirect_sales_rep_route_plan') }}
                                                )
                                    ) t2
                        where trim(t1.ctry_cd) = trim(t2.ctry_cd(+))
                                and trim(t1.dstr_cd) = trim(t2.dstr_cd(+))
                                and trim(t1.sls_rep_cd) = trim(t2.sls_rep_cd(+))
                                and trim(t1.store_cd) = trim(t2.store_cd(+))
                                and trim(t1.store_class) = trim(t2.store_class(+))
                                and trim(t1.week) = trim(t2.week(+))
                                and trim(t1.day) = trim(t2.day(+))
                                and (
                                    (t2.ctry_cd is null)
                                    and (t2.dstr_cd is null)
                                    and (t2.sls_rep_cd is null)
                                    and (t2.store_cd is null)
                                    and (t2.store_class is null)
                                    and (t2.week is null)
                                    and (t2.day is null)
                                    )
                        ) as t
                    where a.ctry_cd = t.ctry_cd
                        and a.dstr_cd = t.dstr_cd
                        and a.sls_rep_cd = t.sls_rep_cd
                        and a.store_cd = t.store_cd
                        and a.store_class = t.store_class
                        and a.week = t.week
                        and a.day = t.day;
                    "
    )
}}




with wks_indirect_sales_rep_route_plan as
(
    select * from {{ ref('ntawks_integration__wks_indirect_sales_rep_route_plan') }}
),

transformed as
(
    SELECT t2.ctry_cd,
      t2.dstr_cd,
      t2.sls_rep_cd_nm,
      t2.sls_rep_cd,
      t2.sls_rep_nm,
      t2.store_cd,
      t2.store_nm,
      t2.store_class,
      t2.visit_freq,
      t2.week,
      t2.day,
      t2.file_eff_dt,
      TO_DATE('9999-12-31', 'YYYY-MM-DD') AS effctv_end_dt,
      convert_timezone('UTC', current_timestamp()) AS crt_dttm,
      CAST(NULL AS DATE) AS updt_dttm FROM 
      (
        SELECT *
        FROM {{this}}
        WHERE effctv_end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
      ) t1,
      (
            SELECT *
            FROM wks_indirect_sales_rep_route_plan
            WHERE file_eff_dt = (
                        SELECT MAX(file_eff_dt) 
                        FROM wks_indirect_sales_rep_route_plan
                        )
            ) t2 WHERE TRIM(t1.ctry_cd(+)) = TRIM(t2.ctry_cd)
      AND TRIM(t1.dstr_cd(+)) = TRIM(t2.dstr_cd)
      AND TRIM(t1.sls_rep_cd(+)) = TRIM(t2.sls_rep_cd)
      AND TRIM(t1.store_cd(+)) = TRIM(t2.store_cd)
      AND TRIM(t1.store_class(+)) = TRIM(t2.store_class)
      AND TRIM(t1.week(+)) = TRIM(t2.week)
      AND TRIM(t1.day(+)) = TRIM(t2.day)
      AND (
            (t1.ctry_cd IS NULL)
            AND (t1.dstr_cd IS NULL)
            AND (t1.sls_rep_cd IS NULL)
            AND (t1.store_cd IS NULL)
            AND (t1.store_class IS NULL)
            AND (t1.week IS NULL)
            AND (t1.day IS NULL)
            )
),

final as
(
    select
        ctry_cd::varchar(5) as ctry_cd,
        dstr_cd::varchar(10) as dstr_cd,
        sls_rep_cd_nm::varchar(100) as sls_rep_cd_nm,
        sls_rep_cd::varchar(20) as sls_rep_cd,
        sls_rep_nm::varchar(50)as sls_rep_nm,
        store_cd::varchar(20) as store_cd,
        store_nm::varchar(100)as store_nm,
        store_class::varchar(3) as store_class,
        visit_freq::number(18,0) as visit_freq,
        week::number(18,0) as week,
        day::varchar(20) as day,
        null as effctv_strt_dt,
        effctv_end_dt::date as effctv_end_dt,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from transformed
)

select * from final