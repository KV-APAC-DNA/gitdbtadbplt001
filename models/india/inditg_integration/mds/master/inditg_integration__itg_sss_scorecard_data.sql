{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete FROM {{this}}
        WHERE (
        program_type,
        jnj_id,
        kpi,
        COALESCE(brand, 'NA'),
        quarter,
        year
        ) IN (
        SELECT sdl.program_type,
            sdl.jnj_id,
            sdl.kpi,
            COALESCE(sdl.brand, 'NA'),
            sdl.quarter,
            sdl.year
        FROM {{ source('indsdl_raw', 'sdl_sss_scorecard_data') }} SDL
            INNER JOIN {{this}} ITG ON Upper(SDL.program_type) = Upper(ITG.program_type)
            AND Upper(SDL.jnj_id) = Upper(ITG.jnj_id)
            AND Upper(SDL.kpi) = Upper(ITG.kpi)
            AND Upper(SDL.quarter) = Upper(ITG.quarter)
            AND Upper(SDL.year) = Upper(ITG.year)
            AND CASE
                WHEN sdl.brand IS NULL
                AND itg.brand IS NULL THEN 1
                WHEN Upper(SDL.brand) = Upper(ITG.brand) THEN 1
                ELSE 0
            END = 1
        );
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_sss_scorecard_data') }}
),
final as 
(
    select 
    upper(program_type)::varchar(50) as program_type,
	upper(jnj_id)::varchar(50) as jnj_id,
	upper(rs_id)::varchar(50) as rs_id,
	upper(outlet_name)::varchar(100) as outlet_name,
	upper(region)::varchar(50) as region,
	upper(zone)::varchar(50) as zone,
	upper(territory)::varchar(50) as territory,
	upper(city)::varchar(50) as city,
	upper(brand)::varchar(50) as brand,
	upper(kpi)::varchar(50) as kpi,
	upper(quarter)::varchar(50) as quarter,
	upper(year)::varchar(50) as year,
	 CASE
        WHEN UPPER(kpi) = 'SOS'
        and target is null THEN target
        WHEN UPPER(kpi) = 'SOS'
        and target is not null THEN CAST(ROUND(target::float * 100) AS VARCHAR)
        ELSE Upper(target)
    END::varchar(50) as target,
	CASE
        WHEN UPPER(kpi) = 'SOS'
        and actual is null THEN actual
        WHEN UPPER(kpi) = 'SOS'
        and actual is not null THEN CAST(ROUND(actual::float * 100) AS VARCHAR)
        ELSE Upper(actual)
    END::varchar(50) as actual,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as upd_dttm,
	filename::varchar(100) as filename,
	run_id::varchar(14) as run_id
    from source
)
select * from final