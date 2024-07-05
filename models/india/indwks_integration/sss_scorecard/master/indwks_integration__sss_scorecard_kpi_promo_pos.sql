with 
itg_sss_scorecard_data as 
(
    select * from {{ ref('inditg_integration__itg_sss_scorecard_data') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
trans as 
(
    SELECT 
    program_type,
    kpi,
    jnj_id,
    outlet_name,
    quarter,
    brand,
    year,
    sum(neu) as neu,
    sum(deno) as deno
FROM (
        SELECT 
            program_type,
            kpi,
            jnj_id,
            outlet_name,
            quarter,
            brand,
            year,
            count(actual) as neu,
            '0' as deno
        from itg_sss_scorecard_data scr
        where upper(scr.actual) = 'Y'
            and upper(scr.kpi) in (
                select upper(parameter_value)
                from itg_query_parameters
                where upper(country_code) = 'IN'
                    and upper(parameter_name) in (
                        'SSS_SCORECARD_KPI_NAME_2',
                        'SSS_SCORECARD_KPI_NAME_3'
                    )
                    and upper(parameter_type) = 'SSS_KPI'
            )
            and scr.actual is not null
        group by program_type,
            kpi,
            jnj_id,
            outlet_name,
            quarter,
            brand,
            year
        union all
        select 
            program_type,
            kpi,
            jnj_id,
            outlet_name,
            quarter,
            brand,
            year,
            '0' as neu,
            count(actual) as deno
        from itg_sss_scorecard_data scr
        where upper(scr.kpi) in (
                select upper(parameter_value)
                from itg_query_parameters
                where upper(country_code) = 'IN'
                    and upper(parameter_name) in (
                        'SSS_SCORECARD_KPI_NAME_2',
                        'SSS_SCORECARD_KPI_NAME_3'
                    )
                    and upper(parameter_type) = 'SSS_KPI'
            )
            and scr.actual is not null
        group by program_type,
            kpi,
            jnj_id,
            outlet_name,
            quarter,
            brand,
            year
    )
group by program_type,
    kpi,
    jnj_id,
    outlet_name,
    quarter,
    brand,
    year
),
final as 
(
    select 
    program_type::varchar(50) as program_type,
	kpi::varchar(50) as kpi,
	jnj_id::varchar(50) as jnj_id,
	outlet_name::varchar(100) as outlet_name,
	quarter::varchar(50) as quarter,
	brand::varchar(50) as brand,
	year::varchar(50) as year,
	neu::number(38,0) as neu,
	deno::number(38,0) as deno
    from trans
)
select * from final