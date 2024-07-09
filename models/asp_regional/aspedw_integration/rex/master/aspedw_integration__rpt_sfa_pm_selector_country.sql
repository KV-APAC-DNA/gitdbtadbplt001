with source as (
    select * from {{ ref('aspedw_integration__edw_rpt_sfa_pm') }}
),
final as (
    select 
        distinct trim(country) as country
    from source
)
select * from final