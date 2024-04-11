{% macro build_itg_la_gt_route_header(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started building itg table -> itg_la_gt_route_header for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    
    {{ log("Setting query to delete records from itg table -> itg_la_gt_route_header for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_from_itg_query %}
    DELETE FROM 
    {% if target.name=='prod' %}
                    thaitg_integration.itg_la_gt_route_header
                {% else %}
                    {{schema}}.thaitg_integration__itg_la_gt_route_header
                {% endif %}	
    WHERE (COALESCE(UPPER(TRIM(saleunit)),'N/A')) IN (
        SELECT DISTINCT COALESCE(UPPER(TRIM(saleunit)),'N/A') 
        from
        {% if target.name=='prod' %}
                    thawks_integration.wks_la_gt_route_header_pre_load
                {% else %}
                    {{schema}}.thawks_integration__wks_la_gt_route_header_pre_load
                {% endif %} 
        
        
        where filename = '{{filename}}' ) AND   UPPER(flag) IN ('I','U');
    {% endset %}
    { log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query set to delete records from itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running the query to delete records from itg table -> itg_la_gt_route_header for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_from_itg_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running the query to delete records from itg table -> itg_la_gt_route_header for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_itg_model %}
    insert into  {% if target.name=='prod' %}
                    thaitg_integration.itg_la_gt_route_header
                {% else %}
                    {{schema}}.thaitg_integration__itg_la_gt_route_header
                {% endif %}	
    with wks_la_gt_route_header_pre_load as (
    select * from 
        {% if target.name=='prod' %}
                    thawks_integration.wks_la_gt_route_header_pre_load
                {% else %}
                    {{schema}}.thawks_integration__wks_la_gt_route_header_pre_load
                {% endif %}	
        ),
        final as (
            select 
            route_id::varchar(50) as route_id,
            route_name::varchar(100) as route_name,
            route_desc::varchar(200) as route_desc,
            is_active::varchar(5) as is_active,
            routesale::varchar(100) as routesale,
            saleunit::varchar(100) as saleunit,
            route_code::varchar(50) as route_code,
            description::varchar(100) as description,
            effective_start_date::date as effective_start_date,
            effective_end_date::date as effective_end_date,
            flag::varchar(5) as flag,
            file_upload_date::date as file_upload_date,
            filename::varchar(50) as filename,
            run_id::varchar(14) as run_id,
            crt_dttm::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
            from wks_la_gt_route_header_pre_load
        )
        select * from final 
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query setting completed to build itg table -> itg_la_gt_route_header for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running query to build itg table -> itg_la_gt_route_header for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_itg_model) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to build itg table -> itg_la_gt_route_header for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to delete records from wks staging table -> wks_la_gt_route_header for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_wks_staging_data_by_file_query %}
    delete from {{ ref('thawks_integration__wks_la_gt_route_header') }} where filename= '{{filename}}';
    {% endset %}
    {{ log("Started running query to delete records from wks staging table -> wks_la_gt_route_header for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_wks_staging_data_by_file_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to delete records from wks staging table -> wks_la_gt_route_header for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
{% endmacro %}
m