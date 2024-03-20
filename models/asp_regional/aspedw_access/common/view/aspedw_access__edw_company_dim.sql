with source as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
final as (
    select
        clnt as "clnt",
        co_cd as "co_cd",
        ctry_key as "ctry_key",
        ctry_nm as "ctry_nm",
        crncy_key as "crncy_key",
        chrt_acct as "chrt_acct",
        crdt_cntrl_area as "crdt_cntrl_area",
        fisc_yr_vrnt as "fisc_yr_vrnt",
        company as "company",
        company_nm as "company_nm",
        ctry_group as "ctry_group",
        "cluster" as "cluster",
        crt_dttm as "crt_dttm",
        updt_dttm as "updt_dttm"
    from source
)
select * from final 