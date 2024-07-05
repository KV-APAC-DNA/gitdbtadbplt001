with 
itg_mds_in_sss_score as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_sss_score') }}
),
itg_sss_scorecard_data as 
(
    select * from {{ ref('inditg_integration__itg_sss_scorecard_data') }}
),
trans as 
(
    select itg.program_type,
    itg.jnj_id,
    itg.outlet_name,
    itg.region,
    itg.zone,
    itg.territory,
    itg.city,
    itg.brand,
    itg.kpi,
    itg.quarter,
    itg.year,
    itg.target,
    itg.actual,
    sum(
        case
            when cast(itg.actual as decimal(15, 2)) >= cast(sco.min_value as decimal(15, 2))
            and cast(sco.max_value as decimal(15, 2)) is null then sco.value
            when cast(itg.actual as decimal(15, 2)) >= cast(sco.min_value as decimal(15, 2))
            and cast(itg.actual as decimal(15, 2)) < cast(sco.max_value as decimal(15, 2))
            and sco.max_value is not null then sco.value
            else 0
        end
    ) as score_value
from itg_sss_scorecard_data itg,
    itg_mds_in_sss_score sco
where upper(sco.store_class) = upper(itg.program_type)
    and (
        case
            when upper(sco.kpi) = 'PLANOGRAM COMPLIANCE' then 'PLANO'
        end
    ) = upper(itg.kpi)
    and itg.actual is not null
    and upper(itg.brand) = upper(sco.brand)
group by itg.program_type,
    itg.jnj_id,
    itg.outlet_name,
    itg.region,
    itg.zone,
    itg.territory,
    itg.city,
    itg.brand,
    itg.kpi,
    itg.quarter,
    itg.year,
    itg.target,
    itg.actual
),
final as 
(
    select 
        program_type::varchar(50) as program_type,
        jnj_id::varchar(50) as jnj_id,
        outlet_name::varchar(100) as outlet_name,
        region::varchar(50) as region,
        zone::varchar(50) as zone,
        territory::varchar(50) as territory,
        city::varchar(50) as city,
        brand::varchar(50) as brand,
        kpi::varchar(50) as kpi,
        quarter::varchar(50) as quarter,
        year::varchar(50) as year,
        target::varchar(50) as target,
        actual::varchar(50) as actual,
        score_value::number(38,3) as score_value
    from trans
)
select * from final