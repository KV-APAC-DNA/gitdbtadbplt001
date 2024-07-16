with 
edw_sss_kpi_data as 
(
    select * from {{ ref('indedw_integration__edw_sss_kpi_data') }}
),
itg_mds_in_sss_score as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_sss_score') }}
),
trans as 
(
    select 
    sss.program_type,
    sss.jnj_id,
    sss.outlet_name,
    sss.region,
    sss.zone,
    sss.territory,
    sss.city,
    sss.brand,
    sss.kpi,
    sss.quarter,
    sss.year,
    sss.source_target,
    sss.source_actual,
    sss.target,
    sss.actual,
    sss.compliance,
    sss.weight,
    sss.score_value,
    sco.max_potential_brand_score,
    sss.crt_dttm,
    sss.updt_dttm
from edw_sss_kpi_data sss,
(
        select store_class,
            kpi,
            nvl(upper(brand), 'BRAND NOT APPLICABLE') as brand,
            max (value) as max_potential_brand_score
        from itg_mds_in_sss_score
        group by store_class,
            kpi,
            brand
    ) sco
where upper(sss.program_type) = upper(sco.store_class)
    and nvl(upper(sss.brand), 'BRAND NOT APPLICABLE') = nvl(upper(sco.brand), 'BRAND NOT APPLICABLE')
    and (
        case
            when upper(sco.kpi) = 'SOS COMPLIANCE' THEN 'SOS'
            when upper(sco.kpi) = 'PROMO COMPLIANCE' THEN 'PROMO'
            when upper(sco.kpi) = 'ADDITIONAL VISIBILITY COMPLIANCE' THEN 'ADDITIONAL VISIBILITY'
            when upper(sco.kpi) = 'POS COMPLIANCE' THEN 'POS'
            when upper(sco.kpi) = 'ENDCAP COMPLIANCE' THEN 'ENDCAP'
            when upper(sco.kpi) = 'PLANOGRAM COMPLIANCE' THEN 'PLANO'
        end
    ) = upper(sss.kpi)
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
	max_potential_brand_score::number(31,3) as max_potential_brand_score,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    from trans
)
select * from final