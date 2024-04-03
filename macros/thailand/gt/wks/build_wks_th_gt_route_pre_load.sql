{% macro build_wks_th_gt_route_pre_load(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build build_wks_th_gt_route_pre_load: "~ file) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_wks_th_gt_route_pre_load_query %}
    create or replace table 
        {% if target.name=='prod' %} 
                thawk_integration.wks_th_gt_route_pre_load
            {% else %}
                {{schema}}.thawks_integration__wks_th_gt_route_pre_load
            {% endif %}	
        as (
            with wks_th_gt_route_hashkey as (
                select * from 
                {% if target.name=='prod' %} 
                thawk_integration.wks_th_gt_route_hashkey
                {% else %}
                    {{schema}}.thawks_integration__wks_th_gt_route_hashkey
                {% endif %}
            ),
            sdl_th_gt_route as (
                select * from {{ source('thasdl_raw', 'sdl_th_gt_route') }} where filename= '{{filename}}'
            )
            SELECT wks.cntry_cd,
                wks.crncy_cd,
                wks.routeid,
                wks.name,
                wks.route_description,
                wks.isactive,
                wks.routesale,
                wks.saleunit,
                wks.routecode,
                wks.description,
                wks.effective_start_date,
                (sdl.last_updated_date -1) AS effective_end_date,
                'D' AS flag,
                wks.filename,
                wks.file_uploaded_date,
                wks.run_id,
                wks.crt_dttm
            FROM wks_th_gt_route_hashkey wks,
                sdl_th_gt_route sdl
            where wks.hashkey = sdl.hashkey
            AND   COALESCE(wks.effective_start_date,'9999-12-31') <>COALESCE(sdl.last_updated_date,'9999-12-31')
            AND   UPPER(wks.flag) IN ('I','U')

            UNION ALL

            ---------------**** Historical Records Marking as Delete as there is no new records found in File for PK's ****---------------------

            SELECT wks.cntry_cd,
                wks.crncy_cd,
                wks.routeid,
                wks.name,
                wks.route_description,
                wks.isactive,
                wks.routesale,
                wks.saleunit,
                wks.routecode,
                wks.description,
                wks.effective_start_date,
                sdl.file_uploaded_date - 1 AS effective_end_date,
                'D' AS flag,
                wks.filename,
                wks.file_uploaded_date,
                wks.run_id,
                wks.crt_dttm
            FROM wks_th_gt_route_hashkey wks,
                (SELECT MAX(file_uploaded_date) file_uploaded_date
                FROM sdl_th_gt_route) sdl
            WHERE NOT EXISTS (SELECT 1
                            FROM sdl_th_gt_route sdl
                            WHERE wks.hashkey = sdl.hashkey)
            AND   UPPER(wks.flag) IN ('I','U')

            UNION ALL

            ---------------**** Matched PK's**Update Records ****---------------------

            SELECT wks.cntry_cd,
                wks.crncy_cd,
                wks.routeid,
                sdl.name,
                sdl.route_description,
                sdl.isactive,
                wks.routesale,
                wks.saleunit,
                sdl.routecode,
                sdl.description,
                sdl.last_updated_date AS effective_start_date,
                '9999-12-31' AS effective_end_date,
                'U' AS flag,
                sdl.filename,
                sdl.file_uploaded_date,
                sdl.run_id,
                sdl.crt_dttm
            FROM wks_th_gt_route_hashkey wks,
                sdl_th_gt_route sdl
            WHERE wks.hashkey = sdl.hashkey
            AND   COALESCE(wks.effective_start_date,'9999-12-31') = COALESCE(sdl.last_updated_date,'9999-12-31')
            AND   UPPER(wks.flag) IN ('I','U')

            UNION ALL

            --------------------**** Matched PK's but updated modified date to mark as Insert Flag ****------------------

            SELECT sdl.cntry_cd,
                sdl.crncy_cd,
                sdl.routeid,
                sdl.name,
                sdl.route_description,
                sdl.isactive,
                sdl.routesale,
                sdl.saleunit,
                sdl.routecode,
                sdl.description,
                sdl.last_updated_date AS effective_start_date,
                '9999-12-31' AS effective_end_date,
                'I' AS flag,
                sdl.filename,
                sdl.file_uploaded_date,
                sdl.run_id,
                sdl.crt_dttm
            FROM wks_th_gt_route_hashkey wks,
                sdl_th_gt_route sdl
            WHERE wks.hashkey = sdl.hashkey
            AND   COALESCE(wks.effective_start_date,'9999-12-31') <>COALESCE(sdl.last_updated_date,'9999-12-31')
            AND   UPPER(wks.flag) IN ('I','U')

            UNION ALL

            ---------------**** New Records ****---------------------

            SELECT sdl.cntry_cd,
                sdl.crncy_cd,
                sdl.routeid,
                sdl.name,
                sdl.route_description,
                sdl.isactive,
                sdl.routesale,
                sdl.saleunit,
                sdl.routecode,
                sdl.description,
                sdl.last_updated_date AS effective_start_date,
                '9999-12-31' AS effective_end_date,
                'I' AS flag,
                sdl.filename,
                sdl.file_uploaded_date,
                sdl.run_id,
                sdl.crt_dttm
            FROM sdl_th_gt_route sdl
            WHERE NOT EXISTS (SELECT 1
                  FROM wks_th_gt_route_hashkey wks
                  WHERE wks.hashkey = sdl.hashkey
                  AND   UPPER(wks.flag) IN ('I','U'))
        );
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Set the query to build build_wks_th_gt_route_pre_load for file: "~ file) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}

    {{ log("Started building model build_wks_th_gt_route_pre_load for file: "~ file) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_wks_th_gt_route_pre_load_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Ended building model build_wks_th_gt_route_pre_load for file: "~ file) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}

{% endmacro %}