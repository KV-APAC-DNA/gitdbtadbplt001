{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",     
        pre_hook = "{% if is_incremental() %}
        DELETE
        FROM {{this}} 
        WHERE 0 != (SELECT COUNT(*) FROM {{source('idnsdl_raw','sdl_mds_id_dms_dist_margin_control')}});
        {% endif %}"
    )
}}

with source as
(
    select * from {{source('idnsdl_raw','sdl_mds_id_dms_dist_margin_control') }}
),
final as
(
    select
    upper(trim(distributorcode))::varchar(10) as distributorcode,
	upper(trim(franchise))::varchar(50) as franchise,
	upper(trim(brand))::varchar(50) as brand,
	upper(trim(type))::varchar(10) as type,
	trim(margin):: varchar(50) as margin,
    TRIM(effective_from)::varchar(6) as effective_from,
    nvl(effective_to,'999912')::varchar(6) as effective_to,
	current_timestamp()::timestamp_ntz as crtd_dttm
    from source
)

select * from final