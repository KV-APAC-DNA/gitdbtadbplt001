{{
    config(
        alias= "edw_calendar_dim",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["cal_day","fisc_yr_vrnt"],
        merge_exclude_columns= ["crt_dttm"],
        tags= ["daily"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_edw_calendar_dim') }}
),

--Logical CTE

--Final CTE
final as (
    select
        cal_day,
        fisc_yr_vrnt,
        wkday,
        cal_wk,
        cal_mo_1,
        cal_mo_2,
        cal_qtr_1,
        cal_qtr_2,
        half_yr,
        cal_yr,
        fisc_per,
        pstng_per,
        fisc_yr,
        rec_mode,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

--Final select
select * from final 