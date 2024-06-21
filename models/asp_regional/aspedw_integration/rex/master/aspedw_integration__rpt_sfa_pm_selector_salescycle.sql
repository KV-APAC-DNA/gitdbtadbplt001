with source as (
    select * from snapntaedw_integration.edw_rpt_sfa_pm
),
final as (
    select 
        distinct rpt.country as country,
        rpt.salescyclename as salescyclename
    from source as rpt
)
select * from final