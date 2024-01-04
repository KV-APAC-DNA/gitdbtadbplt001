{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= [],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_account_attr_ciw') }}
),

--Logical CTE

--Final CTE
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
zbravol6 as bravo_acct_l6,
zciwhl1 as ciw_acct_l1,
zciwhl2 as ciw_acct_l2,
zciwhl3 as ciw_acct_l3,
zciwhl4 as ciw_acct_l4,
zciwhl5 as ciw_acct_l5,
zciwhl6 as ciw_acct_l6,

  current_timestamp()::timestamp_ntz(9) as crt_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source )


--Final select
select * from final 