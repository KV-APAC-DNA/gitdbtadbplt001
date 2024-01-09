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
edw_account_dim as 
(
    select * from {{ source('asing012_workspace' , 'edw_account_dim') }}
),
itg_ciw_acct_hier_name_mapping as (
    select * from {{ source('asing012_workspace' , 'itg_ciw_acct_hier_name_mapping')}}
),
--Logical CTE

--Final CTE
final as (
    select
  chrt_accts as chrt_acct,
account as acct_num,
objvers as obj_ver,
changed as chg_flg,
source.bal_flag as bal_flag,
source.cstel_flag as cstel_flag,
source.glacc_flag as glacc_flag,
logsys as src_sys,
source.sem_posit as sem_posit,
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
a.ciw_acct_hier_nm as ciw_acct_l1_txt,
b.ciw_acct_hier_nm as ciw_acct_l2_txt,
c.ciw_acct_hier_nm as ciw_acct_l3_txt,
d.ciw_acct_hier_nm as ciw_acct_l4_txt,
e.ciw_acct_hier_nm as ciw_acct_l5_txt,
f.ciw_acct_hier_nm as ciw_acct_l6_txt,
g.acct_nm as acct_nm,
  current_timestamp()::timestamp_ntz(9) as crt_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source 
  left join itg_ciw_acct_hier_name_mapping as a on
  source.zciwhl1 = a.ciw_acct_hier_no
  left join itg_ciw_acct_hier_name_mapping as b on
  source.zciwhl2 = b.ciw_acct_hier_no
  left join itg_ciw_acct_hier_name_mapping as c on
  source.zciwhl3 = c.ciw_acct_hier_no
  left join itg_ciw_acct_hier_name_mapping as d on
  source.zciwhl4 = d.ciw_acct_hier_no
  left join itg_ciw_acct_hier_name_mapping as e on
  source.zciwhl5 = e.ciw_acct_hier_no
  left join itg_ciw_acct_hier_name_mapping as f on
  source.zciwhl6 = f.ciw_acct_hier_no
  left join edw_account_dim as g on
  g.chrt_acct = source.chrt_accts and g.acct_num = source.account
)


--Final select
select * from final 