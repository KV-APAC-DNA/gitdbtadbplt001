with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_one_jnj_data') }}
),
final as(
    select
        trim(code)::varchar(500) as code,
        trim(name)::varchar(500) as name,
        trim(changetrackingmask)::number(18,0) as changetrackingmask,
        trim(date_level_code)::varchar(200) as date_level,
        trim(date_value)::varchar(200) as date_value,
        trim(sector_function_code)::varchar(200) as sector_function,
        trim(functional_area)::varchar(200) as functional_area,
        trim(subject)::varchar(510) as subject,
        trim(kpi)::varchar(510) as kpi,
        trim(level_1_def)::varchar(200) as level_1_def,
        trim(level_1)::varchar(200) as level_1,
        trim(level_2_def)::varchar(200) as level_2_def,
        trim(level_2)::varchar(200) as level_2,
        trim(ref_value)::varchar(200) as ref_value,
        trim(actual_value)::varchar(510) as actual_value,
        trim(add_info)::varchar(510) as add_info,
        trim(default_view_flag)::varchar(2) as default_view_flag,
        trim(compliance_definition)::varchar(200) as compliance_definition,
        current_timestamp()::timestampntz(9) as crtd_dttm,
        current_timestamp()::timestampntz(9) as updt_dttm
    from source
)
select * from final