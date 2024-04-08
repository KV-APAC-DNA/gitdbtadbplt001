{% macro build_itg_la_gt_route_detail(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started building itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    
    {{ log("Setting query to delete records from itg table -> itg_la_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_from_itg_query %}
    delete from {% if target.name=='prod' %}
                    thaitg_integration.itg_la_gt_route_detail
                {% else %}
                    {{schema}}.thaitg_integration__itg_la_gt_route_detail
                {% endif %}	
    where (coalesce(upper(trim(saleunit)),'N/A')) in (select distinct coalesce(upper(trim(saleunit)),'N/A')
                                                  from {{ source('thasdl_raw', 'sdl_la_gt_route_detail') }} where filename = '{{filename}}') and   upper(flag) in ('I','U');
    {% endset %}
    { log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query set to delete records from itg table -> itg_la_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running the query to delete records from itg table -> itg_la_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_from_itg_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running the query to delete records from itg table -> itg_la_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_itg_model %}
    insert into  {% if target.name=='prod' %}
                    thaitg_integration.itg_la_gt_route_detail
                {% else %}
                    {{schema}}.thaitg_integration__itg_la_gt_route_detail
                {% endif %}	
    with wks_la_gt_route_detail_pre_load as (
    select * from
    {% if target.name=='prod' %}
                    thawks_integration.wks_la_gt_route_detail_pre_load
                {% else %}
                    {{schema}}.thawks_integration__wks_la_gt_route_detail_pre_load
                {% endif %}	
        ),
        final as (
        SELECT 
            route_id::varchar(50) as route_id,
            customer_id::varchar(50) as customer_id,
            route_no::varchar(10) as route_no,
            saleunit::varchar(100) as saleunit,
            ship_to::varchar(50) as ship_to,
            contact_person::varchar(100) as contact_person,
            created_date::date as created_date,
            file_upload_date::date as file_upload_date,
            last_modified_date::date as last_modified_date,
            flag::varchar(5) as flag,
            filename::varchar(50) as filename,
            run_id::varchar(14) as run_id,
            crt_dttm::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm,
            FROM wks_la_gt_route_detail_pre_load 
        )
        select * from final
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query setting completed to build itg table  -> itg_la_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running query to build itg table  -> itg_la_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_itg_model) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to build itg table  -> itg_la_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to delete records from wks staging table -> wks_la_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_wks_staging_data_by_file_query %}
    delete from {{ ref('thawks_integration__wks_la_gt_route_detail') }} where filename= '{{filename}}';
    {% endset %}
    {{ log("Started running query to delete records from wks staging table -> wks_la_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_wks_staging_data_by_file_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to delete records from wks staging table -> wks_la_gt_route_detail for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
{% endmacro %}