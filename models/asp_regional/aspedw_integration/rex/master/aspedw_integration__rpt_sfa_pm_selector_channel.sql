with source as (
    select * from snapntaedw_integration.edw_rpt_sfa_pm
),
final as (
    select distinct rpt.country as country,
        rpt.dist_chnl as dist_chnl
    from source as rpt
)
select * from final