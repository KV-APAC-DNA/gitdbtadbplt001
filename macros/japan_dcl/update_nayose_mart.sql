{% macro update_nayose_mart() %}
    {% if execute %}
    {% set query %}
    

    create temporary table {{ env_var('DBT_ENV_CORE_DB') }}.jpdclitg_integration.target_kokya as
    (
        SELECT 
            LPAD(C_DIPARENTUSRID, 10, '0') AS C_DIPARENTUSRID,
            LPAD(C_DICHILDUSRID, 10, '0') AS C_DICHILDUSRID
        FROM 
            {% if target.name=='prod' %}
                    jpdclitg_integration.c_tbmembunitrel
                {% else %}
                    {{schema}}.jpndclitg_integration__c_tbmembunitrel
            {% endif %}
        WHERE 
            DIELIMFLG = '0'
        ORDER BY 
            DSPREP,
            DSREN,
            C_DIPARENTUSRID
    );

    UPDATE {{source ('jpdcledw_integration', 'teikikeiyaku_data_mart_cl')}} td
    SET 
        C_DIUSRID = subquery.C_DIPARENTUSRID,
        BK_C_DIUSRID = subquery.C_DIPARENTUSRID,
        updated_date = current_timestamp(),
        updated_by = 'ETL_Batch'
    FROM {{ env_var('DBT_ENV_CORE_DB') }}.jpdclitg_integration.target_kokya subquery
    WHERE td.C_DIUSRID = subquery.C_DICHILDUSRID;

    {% endset %}
    {% set result = run_query(query).columns[0][0] %}
{% endif %}
{% endmacro %}