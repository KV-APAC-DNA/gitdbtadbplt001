{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}


--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_time') }}
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
        --tgt.crt_dttm as tgt_crt_dttm,
        updt_dttm
        --case when tgt.crt_dttm is null then 'i' else 'u' end as chng_flg
  from source
)

--Final select
select * from final