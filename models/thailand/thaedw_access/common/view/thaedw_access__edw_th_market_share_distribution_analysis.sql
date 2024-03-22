with source as (
    select * from {{ ref('thaedw_integration__edw_th_market_share_distribution_analysis') }}
),
final as (
    select 
        category as "category",
        cntry_cd as "cntry_cd",
        mnfctrer as "mnfctrer",
        cntry_nm as "cntry_nm",
        yr_mnth as "yr_mnth",
        chnl as "chnl",
        measure as "measure",
        value as "value"
    from source
)
select * from final