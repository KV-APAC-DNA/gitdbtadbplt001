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

    select * from {{ ref('aspwks_integration__wks_edw_account_dim') }}
),

final as (
    select
        chrt_acct,
        acct_num,
        acct_nm,
        obj_ver,
        chg_flg,
        bal_flag,
        cstel_flag,
        glacc_flag,
        src_sys,
        sem_posit,
        bravo_acct_l1,
        bravo_acct_l2,
        bravo_acct_l3,
        bravo_acct_l4,
        bravo_acct_l5,
        fin_stat_itm, 
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final