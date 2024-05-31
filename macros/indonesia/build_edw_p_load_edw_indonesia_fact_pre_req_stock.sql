{% macro build_edw_p_load_edw_indonesia_fact_pre_req_stock() %}
    {{ log("===============================================================================================") }}
    {{ log("Step1: Trying to fetch the distributor_cd,source_system, effective_from,effective_to from ITG table: idnitg_integration.itg_mds_id_dist_reporting_control_sellout_sales ") }}
    {{ log("===============================================================================================") }}
    {% set get_rows_query %}
        SELECT DISTRIBUTOR_CD,UPPER(TRIM(SOURCE_SYSTEM)),EFFECTIVE_FROM,EFFECTIVE_TO FROM {{ ref('idnitg_integration__itg_mds_id_dist_reporting_control_stock') }}
        ORDER BY DISTRIBUTOR_CD,EFFECTIVE_FROM;
    {% endset %}
    {{ log("Try to execute the query to fetch the distributor_cd,source_system, effective_from,effective_to ") }}
    {{ log("===============================================================================================") }}
        {% set get_rows_query_result = run_query(get_rows_query) %}
        {% if execute %}
                {% set results_list = get_rows_query_result.rows %}
            {% else %}
                {% set results_list = [] %}
            {% endif %}
    {{ log("Truncate table before load ") }}
    {{ log("===============================================================================================") }}
        {% set trunc_tbl_exec %}
        truncate table {% if target.name=='prod' %}
                    idnwks_integration.wks_itg_all_distributor_stock
                {% else %}
                    {{schema}}.idnwks_integration__wks_itg_all_distributor_stock
                {% endif %}
        {% endset %}
        {% do run_query(trunc_tbl_exec) %}
    {{ log("Try to execute the query to run loop ") }}
    {{ log("===============================================================================================") }}
        {% for i in results_list %}
            {% set sql_stmnt_loop %}
            CREATE OR REPLACE TEMPORARY TABLE WKS_MDS_ID_DIST_REPORTING_CONTROL_STOCK
        	AS
        			 SELECT DISTINCT DISTRIBUTOR_CD,
        			 	SOURCE_SYSTEM,
        			 	EFFECTIVE_FROM,
        			 	EFFECTIVE_TO
        			 FROM {{ ref('idnitg_integration__itg_mds_id_dist_reporting_control_stock') }}
        			 WHERE UPPER(TRIM(DISTRIBUTOR_CD)) = UPPER(TRIM('{{ i[0] }}'))
        			 AND   UPPER(TRIM(SOURCE_SYSTEM)) = UPPER(TRIM('{{ i[1] }}'))
        			 AND   EFFECTIVE_FROM = '{{ i[2] }}'
        			 AND EFFECTIVE_TO='{{ i[3] }}';
            {% if i[1] == 'SQL' %}
                INSERT INTO {% if target.name=='prod' %}
                    idnwks_integration.wks_itg_all_distributor_stock
                {% else %}
                    {{schema}}.idnwks_integration__wks_itg_all_distributor_stock
                {% endif %}
                (
                    STOCK_DT,
                    JJ_MNTH_ID,
                    JJ_WK,
                    DSTRBTR_GRP_CD,
                    JJ_SAP_DSTRBTR_ID,
                    JJ_SAP_PROD_ID,
                    TOT_STOCK,
                    STOCK_VAL,
                    SALEABLE_STOCK_QTY,
                    SALEABLE_STOCK_VALUE,
                    NON_SALEABLE_STOCK_QTY,
                    NON_SALEABLE_STOCK_VALUE
                    )
                    SELECT T1.STOCK_DT,
                        T1.JJ_MNTH_ID,
                        T1.JJ_WK,
                        T1.DSTRBTR_CD AS DSTRBTR_GRP_CD,
                        T1.JJ_SAP_DSTRBTR_ID,
                        T1.JJ_SAP_PROD_ID,
                        T1.TOT_STOCK,
                        T1.STOCK_VAL,
                        T1.SALEABLE_STOCK_QTY,
                        T1.SALEABLE_STOCK_VALUE,
                        T1.NON_SALEABLE_STOCK_QTY,
                        T1.NON_SALEABLE_STOCK_VALUE
                    FROM {{ ref('idnitg_integration__itg_stock_dist_map') }} T1,
                        WKS_MDS_ID_DIST_REPORTING_CONTROL_STOCK DIST_CNT,
                        {{ source('idnedw_integration','edw_time_dim') }} T2
                    WHERE TO_DATE(T1.STOCK_DT) = TO_DATE(T2.CAL_DATE)
                    AND   UPPER(TRIM(T1.DSTRBTR_CD)) = UPPER(TRIM(DIST_CNT.DISTRIBUTOR_CD))
                    AND   T2.JJ_MNTH_ID >= DIST_CNT.EFFECTIVE_FROM
                    AND   T2.JJ_MNTH_ID <= DIST_CNT.EFFECTIVE_TO;
                {% else %}
                    {{ log("Nothig to execute") }}
            {% endif %}
            {% endset %}
            {{ log("Execute WKS_ITG_ALL_DISTRIBUTOR_STOCK table load") }}
            {% do run_query(sql_stmnt_loop) %}
        {% endfor %}
        
{% endmacro %}