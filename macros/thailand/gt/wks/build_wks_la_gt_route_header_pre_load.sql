{% macro build_wks_la_gt_route_header_pre_load(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build build_wks_la_gt_route_header_pre_load: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_wks_la_gt_route_header_pre_load_query %}
    create or replace table 
        {% if target.name=='prod' %} 
                thawks_integration.wks_la_gt_route_header_pre_load
            {% else %}
                {{schema}}.thawks_integration__wks_la_gt_route_header_pre_load
            {% endif %}	
        as (
                with wks_la_gt_route_header_hashkey as (
            select * from {% if target.name=='prod' %} 
                thawks_integration.wks_la_gt_route_header_hashkey
                {% else %}
                {{schema}}.thawks_integration__wks_la_gt_route_header_hashkey
                {% endif %}	
        ),
        sdl_la_gt_route_header as (
            select * from {{ ref('thawks_integration__wks_la_gt_route_header') }} where filename= '{{filename}}'
        )

        SELECT wks.route_id,
            wks.route_name,
            wks.route_desc,
            wks.is_active,
            wks.routesale,
            wks.saleunit,
            wks.route_code,
            wks.description,
            wks.effective_start_date,
            (to_date(sdl.last_updated_date,'YYYYMMDD')-1) AS effective_end_date,
            'D' AS flag,
            wks.file_upload_date,
            wks.filename,
            wks.run_id,
            wks.crt_dttm
        FROM wks_la_gt_route_header_hashkey wks,
            sdl_la_gt_route_header sdl
        WHERE wks.hashkey = sdl.hashkey
        AND   COALESCE(wks.effective_start_date,'9999-12-31') <>COALESCE(to_date(sdl.last_updated_date,'YYYYMMDD' ),'9999-12-31')
        AND   UPPER(wks.flag) IN ('I','U')
        UNION ALL
        ---------------**** Historical Records Marking as Delete as there is no new records found in File for PK's ****---------------------
        SELECT wks.route_id,
            wks.route_name,
            wks.route_desc,
            wks.is_active,
            wks.routesale,
            wks.saleunit,
            wks.route_code,
            wks.description,
            wks.effective_start_date,
            sdl.file_upload_date - 1 AS effective_end_date,
            'D' AS flag,
            wks.file_upload_date,
            wks.filename,
            wks.run_id,
            wks.crt_dttm
        FROM wks_la_gt_route_header_hashkey wks,
            (SELECT MAX(file_upload_date) file_upload_date
            FROM sdl_la_gt_route_header) sdl
        WHERE NOT EXISTS (SELECT 1
                        FROM sdl_la_gt_route_header sdl
                        WHERE wks.hashkey = sdl.hashkey)
        AND   UPPER(wks.flag) IN ('I','U')
        UNION ALL
        ---------------**** Matched PK's**Update Records ****---------------------
        SELECT wks.route_id,
            sdl.route_name,
            sdl.route_desc,
            sdl.is_active,
            wks.routesale,
            wks.saleunit,
            sdl.route_code,
            sdl.description,
            to_date(sdl.last_updated_date, 'YYYYMMDD') AS effective_start_date,
            '9999-12-31' AS effective_end_date,
            'U' AS flag,
            sdl.file_upload_date,
            sdl.filename,
            sdl.run_id,
            sdl.crt_dttm
        FROM wks_la_gt_route_header_hashkey wks,
            sdl_la_gt_route_header sdl
        WHERE wks.hashkey = sdl.hashkey
        AND   COALESCE(wks.effective_start_date,'9999-12-31') = COALESCE(to_date(sdl.last_updated_date, 'YYYYMMDD'),'9999-12-31')
        AND   UPPER(wks.flag) IN ('I','U')
        UNION ALL
        --------------------**** Matched PK's but updated modified date to mark as Insert Flag ****------------------
        SELECT sdl.route_id,
            sdl.route_name,
            sdl.route_desc,
            sdl.is_active,
            sdl.routesale,
            sdl.saleunit,
            sdl.route_code,
            sdl.description,
            to_date(sdl.last_updated_date,'YYYYMMDD') AS effective_start_date,
            '9999-12-31' AS effective_end_date,
            'I' AS flag,
            sdl.file_upload_date,
            sdl.filename,
            sdl.run_id,
            sdl.crt_dttm
        FROM wks_la_gt_route_header_hashkey wks,
            sdl_la_gt_route_header sdl
        WHERE wks.hashkey = sdl.hashkey
        AND   COALESCE(wks.effective_start_date,'9999-12-31') <>COALESCE(to_date(sdl.last_updated_date,'YYYYMMDD' ),'9999-12-31')
        AND   UPPER(wks.flag) IN ('I','U')
        UNION ALL
        ---------------**** New Records ****---------------------
        SELECT sdl.route_id,
            sdl.route_name,
            sdl.route_desc,
            sdl.is_active,
            sdl.routesale,
            sdl.saleunit,
            sdl.route_code,
            sdl.description,
            to_date(sdl.last_updated_date,'YYYYMMDD' ) AS effective_start_date,
            '9999-12-31' AS effective_end_date,
            'I' AS flag,
            sdl.file_upload_date,
            sdl.filename,
            sdl.run_id,
            sdl.crt_dttm
        FROM sdl_la_gt_route_header sdl
        WHERE NOT EXISTS (SELECT 1
                        FROM wks_la_gt_route_header_hashkey wks
                        WHERE wks.hashkey = sdl.hashkey
                        AND   UPPER(wks.flag) IN ('I','U'))
        );
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Set the query to build build_wks_la_gt_route_header_pre_load for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}

    {{ log("Started building model build_wks_la_gt_route_header_pre_load for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_wks_la_gt_route_header_pre_load_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Ended building model build_wks_la_gt_route_header_pre_load for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}

{% endmacro %}