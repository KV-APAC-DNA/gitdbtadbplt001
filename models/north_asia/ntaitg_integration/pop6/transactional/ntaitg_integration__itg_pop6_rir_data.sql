{{
    config
    (
        materialized ='incremental',
        incremental_strategy = 'append',
        pre_hook = "{% if is_incremental() %}
                    DELETE
                    FROM {{this}}
                    WHERE visit_id IN (
                            SELECT DISTINCT visit_id
                            FROM DEV_DNA_LOAD.SNAPOSESDL_RAW.SDL_POP6_TH_RIR_DATA
                            );
                    {% endif %}"
    )
}}


with source as
(
    select * from DEV_DNA_LOAD.SNAPOSESDL_RAW.SDL_POP6_TH_RIR_DATA
),

final as
(
    SELECT 
		'TH' as cntry_cd,
		visit_id::varchar(255) as visit_id,
		sku_id::varchar(255) as sku_id,
		sku::varchar(255) as sku,
		facing::number(18,0) as facing,
		is_eyelevel::number(18,0) as is_eyelevel,
		file_name::varchar(100) as file_name,
		run_id::number(14,0) as run_id,
		current_timestamp() as crtd_dttm,
		current_timestamp() as updt_dttm
    FROM source
)
select * from final