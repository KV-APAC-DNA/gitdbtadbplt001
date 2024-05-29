with source as (
    select * from ntaedw_integration.v_intrm_calendar
),
final as (
    select
        cal_day::date as cal_day,
        fisc_yr_vrnt::varchar(2) as fisc_yr_vrnt,
        wkday::number(18,0) as wkday,
        cal_wk::number(18,0) as cal_wk,
        cal_mo_1::number(18,0) as cal_mo_1,
        cal_mo_2::number(18,0) as cal_mo_2,
        cal_qtr_1::number(18,0) as cal_qtr_1,
        cal_qtr_2::number(18,0) as cal_qtr_2,
        half_yr::number(18,0) as half_yr,
        cal_yr::number(18,0) as cal_yr,
        fisc_per::number(18,0) as fisc_per,
        pstng_per::number(18,0) as pstng_per,
        fisc_yr::number(18,0) as fisc_yr,
        rec_mode::varchar(1) as rec_mode,
        promo_week::number(38,0) as promo_week,
        promo_month::varchar(3) as promo_month,
        promo_per::varchar(13) as promo_per,
        fisc_wk_num::number(38,0) as fisc_wk_num,
        max_wk_flg::varchar(1) as max_wk_flg
    from source
)
select * from final