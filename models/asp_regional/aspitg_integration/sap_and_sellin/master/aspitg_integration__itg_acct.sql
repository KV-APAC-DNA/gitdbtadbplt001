{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["chrt_acct","acct_num","obj_ver"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_acct') }}
),

final as (
    select
    chrt_accts as chrt_acct,
    account as acct_num,
    objvers as obj_ver,
    changed as chg_flg,
    bal_flag as bal_flag,
    cstel_flag as cstel_flag,
    glacc_flag as glacc_flag,
    logsys as src_sys,
    sem_posit as sem_posit,
    zbravol1 as bravo_acct_l1,
    zbravol2 as bravo_acct_l2,
    zbravol3 as bravo_acct_l3,
    zbravol4 as bravo_acct_l4,
    zbravol5 as bravo_acct_l5,
    glacctext as fin_stat_itm,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final