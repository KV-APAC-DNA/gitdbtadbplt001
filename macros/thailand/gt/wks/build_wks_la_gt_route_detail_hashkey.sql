{% macro build_wks_la_gt_route_detail_hashkey(filename) %}
    {% set tablename %}
    {% if target.name=='prod' %}
                    thawks_integration.wks_la_gt_route_detail_hashkey
                {% else %}
                    {{schema}}.thawks_integration__wks_la_gt_route_detail_hashkey
                {% endif %}	
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists {{tablename}} (
            	hashkey varchar(500),
                route_id varchar(50),
                customer_id varchar(50),
                route_no varchar(10),
                saleunit varchar(100),
                ship_to varchar(50),
                contact_person varchar(100),
                created_date date,
                file_upload_date date,
                last_modified_date date,
                flag varchar(5),
                filename varchar(50),
                run_id varchar(14),
                crt_dttm timestamp_ntz(9)
        );
        TRUNCATE TABLE {{tablename}};
        INSERT INTO {{tablename}}
            (
            hashkey,
            route_id,
            customer_id,
            route_no,
            saleunit,
            ship_to,
            contact_person,
            created_date,
            file_upload_date,
            last_modified_date,
            flag,
            filename,
            run_id,
            crt_dttm
            )
        SELECT MD5(COALESCE(UPPER(TRIM(saleunit)),'N/A') ||COALESCE (UPPER(TRIM(customer_id)),'N/A') ||COALESCE (UPPER(TRIM(route_id)),'N/A') ||COALESCE (UPPER(TRIM(route_no)),'N/A')) AS hashkey,
            route_id,
            customer_id,
            route_no,
            saleunit,
            ship_to,
            contact_person,
            created_date,
            last_modified_date,
            file_upload_date,
            flag,
            filename,
            run_id,
            crt_dttm
FROM 
{% if target.name=='prod' %}
        thaitg_integration.itg_la_gt_route_detail
    {% else %}
        {{schema}}.thaitg_integration__itg_la_gt_route_detail
    {% endif %}
WHERE UPPER(TRIM(saleunit)) IN (SELECT DISTINCT UPPER(TRIM(saleunit))
                                FROM {{ ref('thawks_integration__wks_la_gt_route_detail') }} where filename = '{{filename}}')
AND   UPPER(flag) IN ('I','U');

                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}




