with source as (
    select * from ntaedw_integration.edw_rpt_sfa_pm
),
final as (
    select *,
        rank() over (partition by filename order by id desc) as rank
    from source
)
select * from final