with 
sss_scorecard_kpi_promo_pos as 
(
    select * from {{ ref('indwks_integration__sss_scorecard_kpi_promo_pos') }}
),
itg_sss_scorecard_data as 
(
    select * from {{ ref('inditg_integration__itg_sss_scorecard_data') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
trans_a as 
(
    select 
    temp.program_type,
    temp.jnj_id,
    max(temp.outlet_name) as outlet_name,
    temp.region,
    temp.zone,
    temp.territory,
    temp.city,
    temp.brand,
    temp.kpi,
    temp.quarter,
    temp.year,
    sum(target)::varchar(50) as source_target,
    sum(actual)::varchar(50) as source_actual,
    sum(target) as target,
    sum(actual) as actual 
,
case
        when cast(sum(target) as numeric(30, 24)) = 0 then 0
        when cast(sum(target) as numeric(30, 24)) > 0 then cast(sum(actual) as numeric(30, 24)) / cast(sum(target) as numeric(30, 24))
    end as compliance
from (
        select program_type,
            kpi,
            jnj_id,
            upper(outlet_name) as outlet_name,
            upper(region) as region,
            upper(zone) as zone,
            territory,
            city,
            brand,
            quarter,
            year,
            sum(target) as target,
            sum(actual) as actual
        from itg_sss_scorecard_data
        where upper(kpi) in (
                select upper(parameter_value)
                from itg_query_parameters
                where upper(country_code) = 'IN'
                    and upper(parameter_name) = 'SSS_SCORECARD_KPI_NAME_1'
                    and upper(parameter_type) = 'SSS_KPI'
            ) 
            and actual is not null
            and target is not null
        group by program_type,
            kpi,
            jnj_id,
            outlet_name,
            region,
            zone,
            territory,
            city,
            brand,
            quarter,
            year
    ) temp
group by program_type,
    jnj_id,
    region,
    zone,
    territory,
    city,
    brand,
    kpi,
    quarter,
    year
),
trans_b as 
(
    select 
    itg.program_type,
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
    itg.target::varchar(50) as source_target,
    itg.actual::varchar(50) as source_actual,
    kpi.deno as target,
    cast(kpi.neu as numeric(10, 4)) as actual,
    cast(kpi.neu as numeric(10, 4)) / kpi.deno as compliance
from sss_scorecard_kpi_promo_pos kpi,
    itg_sss_scorecard_data itg
where itg.actual is not null
    and upper(kpi.kpi) = upper(itg.kpi)
    and upper(kpi.jnj_id) = upper(itg.jnj_id)
    and upper(kpi.outlet_name) = upper(itg.outlet_name)
    and upper(kpi.program_type) = upper(itg.program_type)
    and upper(kpi.quarter) = upper(itg.quarter)
    and kpi.year = itg.year
    and upper(kpi.brand) = upper(itg.brand)
),
trans as 
(
    select * from trans_a
    union all
    select * from trans_b
),
final as 
(
    select 
    program_type::varchar(50) as program_type,
	jnj_id::varchar(50) as jnj_id,
	outlet_name::varchar(50) as outlet_name,
	region::varchar(50) as region,
	zone::varchar(50) as zone,
	territory::varchar(50) as territory,
	city::varchar(50) as city,
	brand::varchar(50) as brand,
	kpi::varchar(50) as kpi,
	quarter::varchar(50) as quarter,
	year::varchar(50) as year,
	source_target::varchar(50) as source_target,
	source_actual::varchar(50) as source_actual,
	target::varchar(50) as target,
	actual::varchar(50) as actual,
	compliance::number(30,24) as compliance
    from trans
)
select * from final