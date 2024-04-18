{% macro build_itg_th_gt_route(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started building itg table -> itg_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    
    {{ log("Setting query to delete records from itg table -> itg_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_from_itg_query %}
    DELETE FROM 
    {% if target.name=='prod' %}
                    thaitg_integration.itg_th_gt_route
                {% else %}
                    {{schema}}.thaitg_integration__itg_th_gt_route
                {% endif %}	
    where (coalesce(upper(trim(saleunit)),'N/A')) in (
        select distinct (coalesce(upper(trim(saleunit)),'N/A')) from {{ source('thasdl_raw', 'sdl_th_gt_route') }} where filename = '{{filename}}' ) AND UPPER(flag) IN ('I','U');
    {% endset %}
    { log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query set to delete records from itg table -> itg_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running the query to delete records from itg table -> itg_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_from_itg_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running the query to delete records from itg table -> itg_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build itg table -> itg_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_itg_model %}
        insert into  {% if target.name=='prod' %}
                        thaitg_integration.itg_th_gt_route
                    {% else %}
                        {{schema}}.thaitg_integration__itg_th_gt_route
                    {% endif %}	
            with source as (
        select  * from 
                    {% if target.name=='prod' %}
                        thawks_integration.wks_th_gt_route_pre_load
                    {% else %}
                        {{schema}}.thawks_integration__wks_th_gt_route_pre_load
                    {% endif %}	
        ),
        final as (
            select 
                cntry_cd::varchar(5) as cntry_cd,
                crncy_cd::varchar(5) as crncy_cd,
                routeid::varchar(50) as routeid,
                name::varchar(100) as name,
                route_description::varchar(100) as route_description,
                isactive::varchar(10) as isactive,
                routesale::varchar(50) as routesale,
                (coalesce(upper(trim(saleunit)),'N/A'))::varchar(50)  as saleunit,
                routecode::varchar(50) as routecode,
                description::varchar(100) as description,
                effective_start_date::date as effective_start_date,
                effective_end_date::date as effective_end_date,
                flag::varchar(5) as flag,
                filename::varchar(100) as filename,
                file_uploaded_date::date as file_uploaded_date,
                run_id::varchar(50) as run_id,
                crt_dttm::timestamp_ntz(9) as crt_dttm
            from source
        )
        select * from final;
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query setting completed to build itg table -> itg_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running query to build itg table -> itg_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_itg_model) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to build itg table -> itg_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to delete records from wks staging table -> wks_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_wks_staging_data_by_file_query %}
    delete from {{ ref('thawks_integration__wks_th_gt_route') }} where filename= '{{filename}}';
    {% endset %}
    {{ log("Started running query to delete records from wks staging table -> wks_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_wks_staging_data_by_file_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to delete records from wks staging table -> wks_th_gt_route for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
{% endmacro %}