--Overwriding default SQL header as we dont want to change timezone to Singapore
{{
    config(
        sql_header= ""
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_account_attr_ciw') }}
),
edw_account_dim as 
(
    select * from {{ ref('aspedw_integration__edw_account_dim') }}
),
itg_ciw_acct_hier_name_mapping as (
    select * from {{ source('aspitg_integration' , 'itg_ciw_acct_hier_name_mapping')}}
),
--Logical CTE

--Final CTE
final as (
select
chrt_accts::varchar(4) as chrt_acct,
account::varchar(10) as acct_num, 
g.acct_nm::varchar(5000) as acct_nm,
objvers::varchar(1) as obj_ver,
changed::varchar(1) as chg_flg,
source.bal_flag::varchar(1) as bal_flag,
source.cstel_flag::varchar(1) as cstel_flag,
source.glacc_flag::varchar(1) as glacc_flag,
logsys::varchar(10) as src_sys,
source.sem_posit::varchar(16) as sem_posit,
zbravol1::varchar(30) as bravo_acct_l1,
zbravol2::varchar(30) as bravo_acct_l2,
zbravol3::varchar(30) as bravo_acct_l3,
zbravol4::varchar(30) as bravo_acct_l4,
zbravol5::varchar(30) as bravo_acct_l5,
zbravol6::varchar(30) as bravo_acct_l6,
zciwhl1::varchar(30) as ciw_acct_l1,
zciwhl2::varchar(30) as ciw_acct_l2,
zciwhl3::varchar(30) as ciw_acct_l3,
zciwhl4::varchar(30) as ciw_acct_l4,
zciwhl5::varchar(30) as ciw_acct_l5,
zciwhl6::varchar(30) as ciw_acct_l6,
null::varchar(60) as bravo_acct_l1_txt,
null::varchar(60) as bravo_acct_l2_txt,
null::varchar(60) as bravo_acct_l3_txt,
null::varchar(60) as bravo_acct_l4_txt,
null::varchar(60) as bravo_acct_l5_txt,
null::varchar(60) as bravo_acct_l6_txt,
a.ciw_acct_hier_nm::varchar(60) as ciw_acct_l1_txt,
b.ciw_acct_hier_nm::varchar(60) as ciw_acct_l2_txt,
c.ciw_acct_hier_nm::varchar(60) as ciw_acct_l3_txt,
d.ciw_acct_hier_nm::varchar(60) as ciw_acct_l4_txt,
e.ciw_acct_hier_nm::varchar(60) as ciw_acct_l5_txt,
f.ciw_acct_hier_nm::varchar(60) as ciw_acct_l6_txt,
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