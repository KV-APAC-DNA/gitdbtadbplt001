{% macro build_wks_th_gt_route_detail_hashkey(filename) %}
    {% set tablename %}
    {% if target.name=='prod' %}
                    thawk_integration.wks_th_gt_route_detail_hashkey
                {% else %}
                    {{schema}}.thawks_integration__wks_th_gt_route_detail_hashkey
                {% endif %}	
    {% endset %}
    {% set query %}
    CREATE TABLE if not exists  {{tablename}}	(
        hashkey varchar(500),
        cntry_cd varchar(5),
        crncy_cd varchar(5),
        routeid varchar(50),
        customerid varchar(50),
        routeno varchar(50),
        saleunit varchar(50),
        ship_to varchar(50),
        contact_person varchar(100),
        created_date date,
        last_modified_date date,
        flag varchar(5),
        filename varchar(100),
        file_uploaded_date date,
        run_id varchar(50),
        crt_dttm timestamp without time zone
    );
    TRUNCATE TABLE {{tablename}};
    INSERT INTO {{tablename}}
    (
    hashkey,
    cntry_cd,
    crncy_cd,
    routeid,
    customerid,
    routeno,
    saleunit,
    ship_to,
    contact_person,
    created_date,
    last_modified_date,
    flag,
    filename,
    file_uploaded_date,
    run_id,
    crt_dttm
    )

    SELECT 
        MD5(COALESCE(UPPER(TRIM(saleunit)),'N/A') ||COALESCE (UPPER(TRIM(customerid)),'N/A') ||COALESCE (UPPER(TRIM(routeid)),'N/A') ||COALESCE (UPPER(TRIM(routeno)),'N/A')) AS hashkey,
       cntry_cd,
       crncy_cd,
       routeid,
       customerid,
       routeno,
       saleunit,
       ship_to,
       contact_person,
       created_date,
       last_modified_date,
       flag,
       filename,
       file_uploaded_date,
       run_id,
       crt_dttm
FROM
    {% if target.name=='prod' %}
            thaitg_integration.itg_th_gt_route_detail
        {% else %}
            {{schema}}.thaitg_integration__itg_th_gt_route_detail
        {% endif %}
    WHERE UPPER(TRIM(saleunit)) IN (SELECT DISTINCT UPPER(TRIM(saleunit))
                                    FROM {{ source('thasdl_raw', 'sdl_th_gt_route_detail') }} where filename = '{{filename}}'
                                    )
    AND   UPPER(flag) IN ('I','U');
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}
