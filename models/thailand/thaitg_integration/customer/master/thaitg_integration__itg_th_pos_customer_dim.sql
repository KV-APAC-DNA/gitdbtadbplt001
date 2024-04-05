{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook="delete from {{this}} where lTRIM(brnch_no,0) IN (SELECT lTRIM(branch_code,0) FROM {{ ref('thaitg_integration__itg_mds_th_mt_branch_master') }} );"
    )
}}


with itg_mds_th_mt_branch_master as(
    select * from {{ ref('thaitg_integration__itg_mds_th_mt_branch_master') }}
),
itg_th_tims_region as (
    select * from {{ ref('thaitg_integration__itg_th_tims_region') }}
),
transformed as(
    select
        b.account::varchar(20) as cust_cd,
        TRIM(b.branch_code)::varchar(50) as brnch_no,
        trim(b.branch_name)::varchar(200) as branch_nm,
        trim(b.branch_type)::varchar(200) as brnch_typ,
        trim(b.allstoretype_name)::varchar(200) as all_str_typ,
        trim(b.province_code)::varchar(200) as prvnce_cd,
        trim(r.prvnce_nm)::varchar(200) as prvnce_nm,
        trim(r.prvnce_eng_nm)::varchar(200) as prvnce_eng_nm,
        trim(r.region)::varchar(200) as region,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from itg_mds_th_mt_branch_master as b
    left outer join itg_th_tims_region as r
    on ltrim(b.province_code, 0) = ltrim(r.prvnce_cd, 0)
)
select * from transformed