with edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),

final as (
    select
        cal_day as "cal_day",
        fisc_yr_vrnt as "fisc_yr_vrnt",
        wkday as "wkday",
        cal_wk as "cal_wk",
        cal_mo_1 as "cal_mo_1",
        cal_mo_2 as "cal_mo_2",
        cal_qtr_1 as "cal_qtr_1",
        cal_qtr_2 as "cal_qtr_2",
        half_yr as "half_yr",
        cal_yr as "cal_yr",
        fisc_per as "fisc_per",
        pstng_per as "pstng_per",
        fisc_yr as "fisc_yr",
        rec_mode as "rec_mode",
        crt_dttm as "crt_dttm",
        updt_dttm as "updt_dttm"
        from edw_calendar_dim
)

select * from final
