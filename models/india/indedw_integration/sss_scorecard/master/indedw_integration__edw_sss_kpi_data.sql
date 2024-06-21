with 
sss_scorecard_kpi_endcap_addn_visblty_score_calc as 
(
    select * from {{ ref('indwks_integration__sss_scorecard_kpi_endcap_addn_visblty_score_calc') }}
),
sss_scorecard_kpi_plano_score_calc as 
(
    select * from {{ ref('indwks_integration__sss_scorecard_kpi_plano_score_calc') }}
),
sss_scorecard_kpi_compliance_calc as 
(
    select * from {{ ref('indwks_integration__sss_scorecard_kpi_compliance_calc') }}
),
itg_mds_in_sss_score as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_sss_score') }}
),
itg_mds_in_sss_weights as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_sss_weights') }}
),
temp_a as 
(
select 
    comp.program_type,
    comp.jnj_id,
    comp.outlet_name,
    comp.region,
    comp.zone,
    comp.territory,
    comp.city,
    comp.brand,
    comp.kpi,
    comp.quarter,
    comp.year,
    comp.source_target,
    comp.source_actual,
    comp.target,
    comp.actual,
    comp.compliance,
    wei.weight,
case
        when comp.compliance >= sco.min_value
        and sco.max_value is null then sco.value
        when comp.compliance >= sco.min_value
        and comp.compliance < sco.max_value
        and sco.max_value is not null then sco.value
        else 0
    end as score_value,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm
from sss_scorecard_kpi_compliance_calc comp,
    itg_mds_in_sss_score sco,
    itg_mds_in_sss_weights wei
where upper(comp.kpi) = case
        when upper(sco.kpi) = 'PROMO COMPLIANCE' THEN 'PROMO'
    end
    and upper(sco.store_class) = upper(comp.program_type)
    and upper(sco.brand) = upper(comp.brand)
    and upper(wei.store_class) = upper(comp.program_type)
    and upper(comp.kpi) = case
        when upper(wei.kpi) = 'PROMO COMPLIANCE' THEN 'PROMO'
    end
),
temp_b as 
(
select 
    comp.program_type,
    comp.jnj_id,
    comp.outlet_name,
    comp.region,
    comp.zone,
    comp.territory,
    comp.city,
    comp.brand,
    comp.kpi,
    comp.quarter,
    comp.year,
    comp.source_target,
    comp.source_actual,
    comp.target,
    comp.actual,
    comp.compliance,
    wei.weight,
case
        when comp.source_actual = sco.min_value then cast(sco.value as numeric(30, 24))
        else 0
    end as score_value,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm
from sss_scorecard_kpi_compliance_calc comp,
    itg_mds_in_sss_score sco,
    itg_mds_in_sss_weights wei
where (
        case
            when upper(sco.kpi) = 'SOS COMPLIANCE' THEN 'SOS'
            when upper(sco.kpi) = 'PROMO COMPLIANCE' THEN 'PROMO'
            when upper(sco.kpi) = 'ADDITIONAL VISIBILITY COMPLIANCE' THEN 'ADDITIONAL VISIBILITY'
            when upper(sco.kpi) = 'POS COMPLIANCE' THEN 'POS'
            when upper(sco.kpi) = 'ENDCAP COMPLIANCE' THEN 'ENDCAP'
        end
    ) = upper(comp.kpi)
    and upper(sco.store_class) = upper(comp.program_type)
    and upper(sco.brand) = upper(comp.brand)
    and upper(wei.store_class) = upper(comp.program_type)
    and (
        case
            when upper(wei.kpi) = 'SOS COMPLIANCE' THEN 'SOS'
            when upper(wei.kpi) = 'PROMO COMPLIANCE' THEN 'PROMO'
            when upper(wei.kpi) = 'ADDITIONAL VISIBILITY COMPLIANCE' THEN 'ADDITIONAL VISIBILITY'
            when upper(wei.kpi) = 'POS COMPLIANCE' THEN 'POS'
            when upper(wei.kpi) = 'ENDCAP COMPLIANCE' THEN 'ENDCAP'
        end
    ) = upper(comp.kpi)
    and upper(comp.kpi) = (
        select upper(parameter_value)
        from itg_query_parameters
        where upper(country_code) = 'IN'
            and upper(parameter_name) = ('SSS_SCORECARD_KPI_NAME_2')
            and upper(parameter_type) = 'SSS_KPI'
    )
),
temp_c as 
(

    select 
    comp.program_type,
    comp.jnj_id,
    comp.outlet_name,
    comp.region,
    comp.zone,
    comp.territory,
    comp.city,
    comp.brand,
    comp.kpi,
    comp.quarter,
    comp.year,
    comp.source_target,
    comp.source_actual,
    comp.target,
    comp.actual,
    comp.compliance,
    wei.weight,
    sum(
        case
            when comp.compliance >= sco.min_value
            and sco.max_value is null then sco.value
            when comp.compliance >= sco.min_value
            and comp.compliance < sco.max_value
            and sco.max_value is not null then sco.value
            else 0
        end
    ) as score_value,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm
from sss_scorecard_kpi_compliance_calc comp,
    itg_mds_in_sss_score sco,
    itg_mds_in_sss_weights wei
where upper(comp.kpi) = case
        when upper(sco.kpi) = 'SOS COMPLIANCE' THEN 'SOS'
    end
    and upper(sco.store_class) = upper(comp.program_type)
    and upper(sco.brand) = upper(comp.brand)
    and upper(wei.store_class) = upper(comp.program_type)
    and upper(comp.kpi) = case
        when upper(wei.kpi) = 'SOS COMPLIANCE' THEN 'SOS'
    end
group by comp.program_type,
    comp.jnj_id,
    comp.outlet_name,
    comp.region,
    comp.zone,
    comp.territory,
    comp.city,
    comp.brand,
    comp.kpi,
    comp.quarter,
    comp.year,
    comp.source_target,
    comp.source_actual,
    comp.target,
    comp.actual,
    comp.compliance,
    wei.weight,
    crt_dttm,
    updt_dttm
),
temp_d as 
(
select 
    calc.program_type,
    calc.jnj_id,
    calc.outlet_name,
    calc.region,
    calc.zone,
    calc.territory,
    calc.city,
    calc.brand,
    calc.kpi,
    calc.quarter,
    calc.year,
    calc.target as source_target,
    calc.actual as source_actual,
    calc.target,
    calc.actual,
    calc.score_value / (
        select max(value)
        from itg_mds_in_sss_score sco
        where upper(sco.kpi) = 'ENDCAP COMPLIANCE'
    ) as compliance,
    wei.weight,
    calc.score_value,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm
from sss_scorecard_kpi_endcap_addn_visblty_score_calc calc,
    itg_mds_in_sss_weights wei
where case
        when upper(wei.kpi) = 'ENDCAP COMPLIANCE' THEN 'ENDCAP'
    end = upper(calc.kpi)
    and upper(wei.store_class) = upper(calc.program_type)
),
temp_e as 
(
    select 
    calc.program_type,
    calc.jnj_id,
    calc.outlet_name,
    calc.region,
    calc.zone,
    calc.territory,
    calc.city,
    calc.brand,
    calc.kpi,
    calc.quarter,
    calc.year,
    calc.target as source_target,
    calc.actual as source_actual,
    calc.target,
    calc.actual,
    calc.score_value / (
        select max(value)
        from itg_mds_in_sss_score sco
        where upper(sco.kpi) = 'ADDITIONAL VISIBILITY COMPLIANCE'
    ) as compliance,
    wei.weight,
    calc.score_value,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm
from sss_scorecard_kpi_endcap_addn_visblty_score_calc calc,
    itg_mds_in_sss_weights wei
where case
        when upper(wei.kpi) = 'ADDITIONAL VISIBILITY COMPLIANCE' THEN 'ADDITIONAL VISIBILITY'
    end = upper(calc.kpi)
    and upper(wei.store_class) = upper(calc.program_type)
),
temp_f as 
(
select 
    calc.program_type,
    calc.jnj_id,
    calc.outlet_name,
    calc.region,
    calc.zone,
    calc.territory,
    calc.city,
    calc.brand,
    calc.kpi,
    calc.quarter,
    calc.year,
    calc.target as source_target,
    calc.actual as source_actual,
    calc.target,
    calc.actual,
    calc.score_value / (
        select max(value)
        from itg_mds_in_sss_score sco
        where upper(sco.kpi) = 'PLANOGRAM COMPLIANCE'
    ) as compliance,
    wei.weight,
    calc.score_value,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm
from sss_scorecard_kpi_plano_score_calc calc,
    itg_mds_in_sss_weights wei
where case
        when wei.kpi = 'Planogram Compliance' THEN 'PLANO'
    end = calc.kpi
    and upper(wei.store_class) = upper(calc.program_type)
),
trans as 
(
    select * from temp_a 
    union all
    select * from temp_b 
    union all
    select * from temp_c 
    union all
    select * from temp_d 
    union all
    select * from temp_e 
    union all
    select * from temp_f 
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
	compliance::number(30,24) as compliance,
	weight::number(31,2) as weight,
	score_value::number(31,3) as score_value,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final