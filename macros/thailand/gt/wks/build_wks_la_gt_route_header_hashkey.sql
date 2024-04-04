{% macro build_wks_la_gt_route_header_hashkey(filename) %}
    {% set tablename %}
    {% if target.name=='prod' %}
                    thawks_integration.wks_la_gt_route_header_hashkey
                {% else %}
                    {{schema}}.thawks_integration__wks_la_gt_route_header_hashkey
                {% endif %}	
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists {{tablename}} (
            hashkey varchar(500),
            route_id varchar(50),
            route_name varchar(100),
            route_desc varchar(200),
            is_active varchar(5),
            routesale varchar(100),
            saleunit varchar(100),
            route_code varchar(50),
            description varchar(100),
            effective_start_date date,
            effective_end_date date,
            flag varchar(5),
            file_upload_date date,
            filename varchar(50),
            run_id varchar(14),
            crt_dttm timestamp_ntz(9)
        );
        TRUNCATE TABLE {{tablename}};
        INSERT INTO {{tablename}}
            (
            hashkey,
            route_id,
            route_name,
            route_desc,
            is_active,
            routesale,
            saleunit,
            route_code,
            description,
            effective_start_date,
            effective_end_date,
            flag,
            file_upload_date,
            filename,
            run_id,
            crt_dttm
            )
        SELECT MD5(COALESCE(UPPER(TRIM(saleunit)),'N/A') ||COALESCE (UPPER(TRIM(routesale)),'N/A') ||COALESCE (UPPER(TRIM(route_id)),'N/A')) AS hashkey,
            route_id,
            route_name,
            route_desc,
            is_active,
            routesale,
            saleunit,
            route_code,
            description,
            effective_start_date,
            effective_end_date,
            flag,
            file_upload_date,
            filename,
            run_id,
            crt_dttm
    FROM 
    {% if target.name=='prod' %}
        thaitg_integration.itg_la_gt_route_header
    {% else %}
        {{schema}}.thaitg_integration__itg_la_gt_route_header
    {% endif %}
    WHERE UPPER(TRIM(saleunit)) IN (SELECT DISTINCT UPPER(TRIM(saleunit))
                                FROM {{ source('thasdl_raw', 'sdl_la_gt_route_header') }} where filename='{{filename}}')
                                AND   UPPER(flag) IN ('I','U');
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}

