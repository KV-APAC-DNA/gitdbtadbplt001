{% macro build_wks_th_gt_route_hashkey(filename) %}
        {% set tablename %}
        {% if target.name=='prod' %}
                        thawks_integration.wks_th_gt_route_hashkey
                    {% else %}
                        {{schema}}.thawks_integration__wks_th_gt_route_hashkey
                    {% endif %}	
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists {{tablename}} (
            hashkey character varying(500),
            cntry_cd character varying(5),
            crncy_cd character varying(5),
            routeid character varying(50),
            name character varying(100),
            route_description character varying(100),
            isactive character varying(10),
            routesale character varying(50),
            saleunit character varying(50),
            routecode character varying(50),
            description character varying(100),
            effective_start_date date,
            effective_end_date date,
            flag character varying(5),
            filename character varying(100),
            file_uploaded_date date,
            run_id character varying(50),
            crt_dttm timestamp without time zone
        );
        TRUNCATE TABLE {{tablename}};
        INSERT INTO {{tablename}}
            (
            hashkey,
            cntry_cd,
            crncy_cd,
            routeid,
            name,
            route_description,
            isactive,
            routesale,
            saleunit,
            routecode,
            description,
            effective_start_date,
            effective_end_date,
            flag,
            filename,
            file_uploaded_date,
            run_id,
            crt_dttm
            )
        SELECT 
            MD5(COALESCE(UPPER(TRIM(saleunit)),'N/A') ||COALESCE (UPPER(TRIM(routesale)),'N/A') ||COALESCE (UPPER(TRIM(routeid)),'N/A')) AS hashkey,
            cntry_cd,
            crncy_cd,
            routeid,
            name,
            route_description,
            isactive,
            routesale,
            saleunit,
        routecode,
        description,
        effective_start_date,
        effective_end_date,
        flag,
        filename,
        file_uploaded_date,
        run_id,
        crt_dttm
    FROM 
    {% if target.name=='prod' %}
        thaitg_integration.itg_th_gt_route
    {% else %}
        {{schema}}.thaitg_integration__itg_th_gt_route
    {% endif %}
    
    WHERE UPPER(TRIM(saleunit)) IN (SELECT DISTINCT UPPER(TRIM(saleunit))
                                    FROM {{ source('thasdl_raw', 'sdl_th_gt_route') }} where filename = '{{filename}}'
                                    )
    AND   UPPER(flag) IN ('I','U'); 
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}

