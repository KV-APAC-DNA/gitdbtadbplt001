{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "{% if is_incremental() %} 
                    delete from {{this}} where file_rec_dt = to_date(convert_timezone('UTC', current_timestamp())) and ctry_cd = 'HK' and dstr_cd = '110256';
                    delete from {{this}} where period >= (select distinct period from select * from {{ source('ntasdl_raw', 'sdl_hk_wingkeung_direct_sales_rep_route_plan')}} ) and ctry_cd = 'HK' and dstr_cd = '110256';
                    {% endif %}"
    )
}}


with source as
(
    select * from {{ source("ntasdl_raw", "sdl_hk_wingkeung_direct_sales_rep_route_plan")}}
),

v_intrm_calendar_ims as
(
    select * from {{ ref("ntaedw_integration__v_intrm_calendar_ims")}}
),

transformed as
(
    select
        'HK' as ctry_cd, 
        '110256' as dstr_cd,
        sls_rep_cd_nm,
        sls_rep_cd,
        sls_rep_nm,
        store_cd,
        store_nm,
        store_class,
        visit_freq,
        week,
        day,
        to_date(convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) ,
        period,
        (
            select min(cal_day)
            from v_intrm_calendar_ims b
            where cast((substring(fisc_per, 1, 4) || substring(fisc_per, 6, 2)) as integer) = a.period
                    and b.wkday = 1
        ) as file_eff_dt
    from source a
),

final as
(
    select 
        ctry_cd::varchar(5)  as ctry_cd,
		dstr_cd::varchar(10)  as dstr_cd,
		sls_rep_cd_nm::varchar(100)  as sls_rep_cd_nm,
		sls_rep_cd::varchar(20)  as sls_rep_cd,
		sls_rep_nm::varchar(50)  as sls_rep_nm,
		store_cd::varchar(20)  as store_cd,
		store_nm::varchar(100) as store_nm,
		store_class::varchar(3)  as store_class,
		visit_freq::number(18,0) as visit_freq,
		week::number(18,0)  as week,
		day::varchar(20)  as day,
		to_date(convert_timezone('UTC', current_timestamp())::timestamp_ntz(9))  as file_rec_dt,
		period::number(18,0)  as period,
		file_eff_dt::date as file_eff_dt
    from transformed
)

select * from final