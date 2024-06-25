with source as (
    select * from ntaedw_integration.edw_rpt_sfa_pm
),
final as (
    select 
        distinct trim(country) as country
    from source
)
select * from final