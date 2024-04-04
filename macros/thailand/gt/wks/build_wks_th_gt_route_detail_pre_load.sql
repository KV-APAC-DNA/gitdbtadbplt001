{% macro build_wks_th_gt_route_detail_pre_load(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build wks_th_gt_route_detail_pre_load: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_wks_th_gt_route_detail_pre_load_query %}
        create or replace table 
        {% if target.name=='prod' %} 
                thawks_integration.wks_th_gt_route_detail_pre_load
            {% else %}
                {{schema}}.thawks_integration__wks_th_gt_route_detail_pre_load
            {% endif %}	
            as (
                with wks_th_gt_route_detail_hashkey as (
                    select * from 
                    {% if target.name=='prod' %} 
                    thawks_integration.wks_th_gt_route_detail_hashkey
                    {% else %}
                    {{schema}}.thawks_integration__wks_th_gt_route_detail_hashkey
                    {% endif %}	
                ),
                sdl_th_gt_route_detail as (
                    select * from {{ ref('thawks_integration__wks_th_gt_route_detail') }} where filename= '{{filename}}'
                )

                SELECT 
                    wks.cntry_cd,
                    wks.crncy_cd,
                    wks.routeid,
                    wks.customerid,
                    wks.routeno,
                    wks.saleunit,
                    wks.ship_to,
                    wks.contact_person,
                    wks.created_date,
                    sdl.created_date AS last_modified_date,
                    'D' AS flag,
                    wks.filename,
                    wks.file_uploaded_date,
                    wks.run_id,
                    wks.crt_dttm
                FROM wks_th_gt_route_detail_hashkey wks,
                    sdl_th_gt_route_detail sdl
                WHERE wks.hashkey = sdl.hashkey
                AND   COALESCE(wks.created_date,'9999-12-31') <>COALESCE(sdl.created_date,'9999-12-31')
                AND   UPPER(wks.flag) IN ('I','U')

                UNION ALL

                --------------------******keeps the historic data in ITG itself which is not matched against staging & mark it as 'D'*****-------

                SELECT 
                    wks.cntry_cd,
                    wks.crncy_cd,
                    wks.routeid,
                    wks.customerid,
                    wks.routeno,
                    wks.saleunit,
                    wks.ship_to,
                    wks.contact_person,
                    wks.created_date,
                    sdl.file_uploaded_date AS last_modified_date,
                    'D' AS flag,
                    wks.filename,
                    wks.file_uploaded_date,
                    wks.run_id,
                    wks.crt_dttm
                FROM wks_th_gt_route_detail_hashkey wks,
                    (SELECT MAX(file_uploaded_date) file_uploaded_date
                    FROM sdl_th_gt_route_detail) sdl
                WHERE NOT EXISTS (SELECT 1
                                FROM sdl_th_gt_route_detail sdl
                                WHERE wks.hashkey = sdl.hashkey)
                AND   UPPER(wks.flag) IN ('I','U')

                UNION ALL

                -----------*****insert the new routes for the same pk's with staging created date(Not Required Scenario)*****----------

                SELECT 
                    sdl.cntry_cd,
                    sdl.crncy_cd,
                    sdl.routeid,
                    sdl.customerid,
                    sdl.routeno,
                    sdl.saleunit,
                    sdl.ship_to,
                    sdl.contact_person,
                    sdl.created_date,
                    '9999-12-31' AS last_modified_date,
                    'I' AS flag,
                    sdl.filename,
                    sdl.file_uploaded_date,
                    sdl.run_id,
                    sdl.crt_dttm
                FROM wks_th_gt_route_detail_hashkey wks,
                    sdl_th_gt_route_detail sdl
                WHERE wks.hashkey = sdl.hashkey
                AND   COALESCE(wks.created_date,'9999-12-31') <>COALESCE(sdl.created_date,'9999-12-31')
                AND   UPPER(wks.flag) IN ('I','U')

                UNION ALL

                -------------*****for matched pk's update the records from staging to ITG*****--------------------------

                SELECT 
                    wks.cntry_cd,
                    wks.crncy_cd,
                    wks.routeid,
                    wks.customerid,
                    wks.routeno,
                    wks.saleunit,
                    sdl.ship_to,
                    sdl.contact_person,
                    sdl.created_date,
                    '9999-12-31' AS last_modified_date,
                    'U' AS flag,
                    sdl.filename,
                    sdl.file_uploaded_date,
                    sdl.run_id,
                    sdl.crt_dttm
                FROM wks_th_gt_route_detail_hashkey wks,
                    sdl_th_gt_route_detail sdl
                WHERE wks.hashkey = sdl.hashkey
                AND   COALESCE(wks.created_date,'9999-12-31') = COALESCE(sdl.created_date,'9999-12-31')
                AND   UPPER(wks.flag) IN ('I','U')

                UNION ALL

                ------------------*****insert new records from staging to ITG*****-------------

                SELECT 
                    sdl.cntry_cd,
                    sdl.crncy_cd,
                    sdl.routeid,
                    sdl.customerid,
                    sdl.routeno,
                    sdl.saleunit,
                    sdl.ship_to,
                    sdl.contact_person,
                    sdl.created_date,
                    '9999-12-31' AS last_modified_date,
                    'I' AS flag,
                    sdl.filename,
                    sdl.file_uploaded_date,
                    sdl.run_id,
                    sdl.crt_dttm
                FROM sdl_th_gt_route_detail sdl
                WHERE NOT EXISTS (SELECT 1
                                FROM wks_th_gt_route_detail_hashkey wks
                                WHERE wks.hashkey = sdl.hashkey
                                AND   UPPER(wks.flag) IN ('I','U'))

    
        );
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Set the query to build wks_th_gt_route_detail_pre_load for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}

    {{ log("Started building model wks_th_gt_route_detail_pre_load for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_wks_th_gt_route_detail_pre_load_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Ended building model wks_th_gt_route_detail_pre_load for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% endmacro %}