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
                            from {{ source('thasdl_raw', 'sdl_pop6_th_rir_data') }}
                            );
                    {% endif %}"
    )
}}


with source as
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_rir_data') }}
),

final as
(
    SELECT 
		'TH'::varchar(10) as cntry_cd,
		visit_id::varchar(255) as visit_id,
        photo::varchar(500) as photo,
        related_attribute::varchar(255) as related_attribute,
		sku_id::varchar(255) as sku_id,
		sku::varchar(255) as sku,
		layer::number(18,0) as layer,
		total_layer::number(18,0) as total_layer,
        facing_of_this_layer::number(18,0) as facing_of_this_layer,
		file_name::varchar(100) as file_name,
		run_id::number(14,0) as run_id,
		current_timestamp() as crtd_dttm,
		current_timestamp() as updt_dttm
    FROM source
)
select * from final