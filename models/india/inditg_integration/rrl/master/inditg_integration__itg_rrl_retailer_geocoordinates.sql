{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["rgc_id"],
        pre_hook = "{%if is_incremental()%}
                    delete from {{this}} itg where itg.file_name  in 
                    (select sdl.file_name from 
                    {{ source('indsdl_raw', 'sdl_rrl_retailer_geocoordinates') }} sdl)
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('indsdl_raw', 'sdl_rrl_retailer_geocoordinates') }}
    qualify row_number() over(partition by rgc_id order by crt_dttm desc)=1 
),
final as
(
    select
    rgc_id::number(18,0) as rgc_id,
    rgc_usercode::varchar(50) as rgc_usercode,
    rgc_distcode::varchar(50) as rgc_distcode,
    rgc_code::varchar(50) as rgc_code,
    rgc_latitude::varchar(20) as rgc_latitude,
    rgc_longtitude::varchar(20) as rgc_longtitude,
    rgc_geouniqueid::varchar(100) as rgc_geouniqueid,
    rgc_createdby::varchar(20) as rgc_createdby,
    rgc_createddate::timestamp_ntz(9) as rgc_createddate,
    rgc_modifiedby::varchar(20) as rgc_modifiedby,
    rgc_modifieddate::timestamp_ntz(9) as rgc_modifieddate,
    rgc_flag::varchar(1) as rgc_flag,
    rgc_status_flag::varchar(1) as rgc_status_flag,
    rgc_flex::varchar(200) as rgc_flex,
    filename::varchar(100) as filename,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz as updt_dttm,
    file_name::varchar(255) as file_name
    from source
)
select * from final