with source as (
    select * from ntaedw_integration.edw_rpt_sfa_pm
),
final as (
    select distinct trim(country) as country,
        trim(salescyclename) as salescyclename,
        trim(salescampaignname) as salescampaignname,
        trim(customername) as customername
    from source
)
select * from final