{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "{% if is_incremental() %} 
                    delete from {{this}} where file_rec_dt = to_date(convert_timezone('UTC', current_timestamp())) and ctry_cd = 'HK' and dstr_cd = '110256';
                    delete from {{this}} where period >= (select distinct period from dev_dna_load.snapntasdl_raw.sdl_hk_wingkeung_direct_sales_rep_route_plan) and ctry_cd = 'HK' and dstr_cd = '110256';
                    {% endif %}"
    )
}}


with source as
(
    --select * from {{ source("ntasdl_raw", "sdl_hk_wingkeung_direct_sales_rep_route_plan")}}
    select * from dev_dna_load.snapntasdl_raw.sdl_hk_wingkeung_direct_sales_rep_route_plan
),

v_intrm_calendar_ims as
(
    -- select * from {{ ref("ntaedw_integration__v_intrm_calendar_ims")}}
    select * from dev_dna_core.snapntaedw_integration.v_intrm_calendar_ims
),

final as
(
    select
        'HK' as ctry_cd, 
        '110256' as dstr_cd,
        sl_no as sl_no,
        sls_rep_cd_nm as sls_rep_cd_nm,
        sls_rep_cd as sls_rep_cd,
        sls_rep_nm as sls_rep_nm,
        store_cd as store_cd,
        store_nm as store,
        store_class as store_class,
        week as week,
        day as day,
        to_date(convert_timezone('UTC', current_timestamp())) as file_rec_dt ,
        period as period,
        (
            select min(cal_day)
            from v_intrm_calendar_ims b
            where cast((substring(fisc_per, 1, 4) || substring(fisc_per, 6, 2)) as integer) = a.period
                    and b.wkday = 1
        ) as file_eff_dt
    from source a
)

select * from final