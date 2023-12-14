{{
    config(
        alias="wks_edw_company_dim",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["ctry_key","co_cd"],
        merge_exclude_columns=["crt_dttm"],
        tags=[""]
    )
}}
with
    itg_comp as 
        (select * from {{ ref('aspitg_integration__itg_comp') }}
        ),
    itg_ctry_cd_text as 
        (select * from {{ ref('aspitg_integration__itg_ctry_cd_text') }}
        ),
    itg_comp_text as 
        (select * from {{ ref('aspitg_integration__itg_comp_text') }}
        ),
    final as (
        select
            src.clnt as clnt,
            src.co_cd as co_cd,
            src.ctry_key as ctry_key,
            shrt_desc as ctry_nm,
            crncy_key,
            chrt_acct,
            crdt_cntrl_area,
            fisc_yr_vrnt,
            company,
            com_cd_nm as company_nm,
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
    from itg_comp as src
    left outer join itg_ctry_cd_text as a
    on a.ctry_key = src.ctry_key and a.lang_key = 'e'
    left outer join itg_comp_text as b
    on b.co_cd = src.co_cd
    )

select * from final
