{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["cal_day","fisc_yr_vrnt"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE 
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_time') }}
),

--Logical CTE

--Final CTE
trans as (
    select
        calday as cal_day,
        fiscvarnt as fisc_yr_vrnt,
        weekday1 as wkday,
        calweek as cal_wk,
        calmonth as cal_mo_1,
        calmonth2 as cal_mo_2,
        calquart1 as cal_qtr_1,
        calquarter as cal_qtr_2,
        halfyear1 as half_yr,
        calyear as cal_yr,
        fiscper as fisc_per,
        fiscper3 as pstng_per,
        fiscyear as fisc_yr,
        recordmode as rec_mode,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
),

final as(
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
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)

--Final select
select * from final