{{
    config(
        alias= "itg_time",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["calday","fisc_yr_vrnt"],
        merge_exclude_columns= ["crt_dttm"],
        tags= ["daily"]
    )
}}

--Import CTE 
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_time') }}
),

--Logical CTE

--Final CTE
final as (
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
)

--Final select
select * from final