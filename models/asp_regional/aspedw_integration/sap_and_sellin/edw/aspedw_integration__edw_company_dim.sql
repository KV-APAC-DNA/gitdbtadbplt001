{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["co_cd","ctry_key"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}
with 
wks_edw_company_dim as (
    select * from {{ ref('aspwks_integration__wks_edw_company_dim') }}
),
itg_mds_ap_company_dim as(
    select * from {{ ref('aspitg_integration__itg_mds_ap_company_dim') }}
),
temp_table as (
    select
        clnt,
        co_cd,
        ctry_key,
        ctry_nm,
        crncy_key,
        chrt_acct,
        crdt_cntrl_area,
        fisc_yr_vrnt,
        company,
        company_nm,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from wks_edw_company_dim 
),
final as (
    select
        clnt::varchar(3) as clnt,
        co_cd::varchar(4) as co_cd,
        temp_table.ctry_key::varchar(3) as ctry_key,
        ctry_nm::varchar(40) as ctry_nm,
        crncy_key::varchar(5) as crncy_key,
        chrt_acct::varchar(4) as chrt_acct,
        crdt_cntrl_area::varchar(4) as crdt_cntrl_area,
        fisc_yr_vrnt::varchar(2) as fisc_yr_vrnt,
        company::varchar(6) as company,
        company_nm::varchar(100) as company_nm,
        case 
            when mds.ctry_group is null then  'Not Defined'::varchar(40)   
            else mds.ctry_group::varchar(40)
        end as ctry_group,
        case 
            when mds.cluster is null then  'Not Defined'::varchar(100)   
            else mds.cluster::varchar(100) 
        end as "cluster",
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from temp_table 
    left join itg_mds_ap_company_dim as mds
    on temp_table.co_cd = mds.code and temp_table.ctry_key = mds.ctry_key
)


select * from final
