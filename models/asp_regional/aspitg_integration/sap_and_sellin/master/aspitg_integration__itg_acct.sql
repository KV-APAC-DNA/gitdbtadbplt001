{{
    config(
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
    chrt_accts :: varchar(4) as chrt_acct,
    account :: varchar(10) as acct_num,
    objvers :: varchar(1) as obj_ver,
    changed :: varchar(1) as chg_flg,
    bal_flag :: varchar(1) as bal_flag,
    cstel_flag :: varchar(1) as cstel_flag,
    glacc_flag :: varchar(1) as glacc_flag,
    logsys :: varchar(10) as src_sys,
    sem_posit :: varchar(16) as sem_posit,
    zbravol1 :: varchar(18) as bravo_acct_l1,
    zbravol2 :: varchar(18) as bravo_acct_l2,
    zbravol3 :: varchar(18) as bravo_acct_l3,
    zbravol4 :: varchar(18) as bravo_acct_l4,
    zbravol5 :: varchar(18) as bravo_acct_l5,
    glacctext :: varchar(18) as fin_stat_itm,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final