{% macro build_itg_th_gt_route_detail(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started building itg table -> itg_th_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    
    {{ log("Setting query to delete records from itg table -> itg_th_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_from_itg_query %}
    DELETE FROM 
    {% if target.name=='prod' %}
                    thaitg_integration.itg_th_gt_route_detail
                {% else %}
                    {{schema}}.thaitg_integration__itg_th_gt_route_detail
                {% endif %}	
   where (coalesce(upper(trim(saleunit)),'N/A')) in (
    select distinct (coalesce(upper(trim(saleunit)),'N/A')) from {{ source('thasdl_raw', 'sdl_th_gt_route_detail') }} where filename = '{{filename}}' ) AND   UPPER(flag) IN ('I','U');
    {% endset %}
    { log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query set to delete records from itg table -> itg_th_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running the query to delete records from itg table -> itg_th_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_from_itg_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running the query to delete records from itg table -> itg_th_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build itg table -> itg_th_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_itg_model %}
        insert into  {% if target.name=='prod' %}
                        thaitg_integration.itg_th_gt_route_detail
                    {% else %}
                        {{schema}}.thaitg_integration__itg_th_gt_route_detail
                    {% endif %}	
            with source as (
                select  * from 
                    {% if target.name=='prod' %}
                        thawks_integration.wks_th_gt_route_detail_pre_load
                    {% else %}
                        {{schema}}.thawks_integration__wks_th_gt_route_detail_pre_load
                    {% endif %}	
            ),
            final as (
                SELECT 
                    cntry_cd::varchar(5) as cntry_cd,
                    crncy_cd::varchar(5) as crncy_cd,
                    routeid::varchar(50) as routeid,
                    customerid::varchar(50) as customerid,
                    routeno::varchar(50) as routeno,
                    saleunit::varchar(50) as saleunit,
                    ship_to::varchar(50) as ship_to,
                    contact_person::varchar(100) as contact_person,
                    created_date::date as created_date,
                    last_modified_date::date as last_modified_date,
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
    {{ log("Query setting completed to build itg table -> itg_th_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running query to build itg table -> itg_th_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_itg_model) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to build itg table -> itg_th_gt_route_detail for file: "~ filename) }}
{% endmacro %}