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
        chrt_acct :: varchar(4) as chrt_acct,
        acct_num :: varchar(10) as acct_num,
        acct_nm :: varchar(5000) as acct_nm,
        obj_ver :: varchar(1) as obj_ver,
        chg_flg :: varchar(1) as chg_flg,
        bal_flag :: varchar(1) as bal_flag,
        cstel_flag :: varchar(1) as cstel_flag,
        glacc_flag :: varchar(1) as glacc_flag,
        src_sys :: varchar(10) as src_sys,
        sem_posit :: varchar(16) as sem_posit,
        bravo_acct_l1 :: varchar(18) as bravo_acct_l1,
        bravo_acct_l2 :: varchar(18) as bravo_acct_l2,
        bravo_acct_l3 :: varchar(18) as bravo_acct_l3,
        bravo_acct_l4 :: varchar(18) as bravo_acct_l4,
        bravo_acct_l5 :: varchar(18) as bravo_acct_l5,
        fin_stat_itm :: varchar(18) as fin_stat_itm,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final