with source as (
    select * from {{ ref('aspedw_integration__edw_rpt_sfa_pm') }}
),
final as (
    select *,
        rank() over (partition by trim(filename) order by id desc) as rank
    from source
)
select * from final