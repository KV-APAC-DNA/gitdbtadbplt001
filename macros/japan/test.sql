{% macro test() %}
{{ log("------------------------------Start Query-------------------------------------------------") }}
{% set flag_query = run_query('select SUBSTRING(current_timestamp(),6,2)||SUBSTRING(current_timestamp(),9,2) as daate') %}
{% set flag = flag_query.columns[0].values()[0] %}

{% if flag == '0229' %}

-- SQL script for flag 0
{% set year_query = run_query("select DATEADD(day,1,to_date(round(SUBSTRING(current_timestamp()::timestamp_ntz(9),1,4)-1) ||  '0228', 'YYYYMMDD')) as dt") %}
{% set ty_exec_start_dt_1 = year_query.columns[0].values()[0] %}
{% else %}
-- SQL script for flag 1
{% set year_query = run_query("select SUBSTRING(DATEADD(year,-1,DATEADD(day,1,SUBSTRING(current_timestamp(),1,10))),1,10) as dt") %}
{% set ty_exec_start_dt_1 = year_query.columns[0].values()[0] %}
{% endif %}
{{ log("------------------------------End Query-------------------------------------------------") }}
{% set t_start_query = run_query("SELECT SUBSTRING(MIN(MAIN.YMD_DT),1,10) as ty_tounen_start_dt FROM DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.MT_CLD	MAIN WHERE	EXISTS
(SELECT * FROM	DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.MT_CLD SUB 
WHERE MAIN.YEAR_445 = SUB.YEAR_445	AND	SUB.YMD_DT = to_date(current_timestamp()))") %}
{% set ty_tounen_start_dt_1 = t_start_query.columns[0].values()[0] %}

{{ log(ty_exec_start_dt_1) }}
{{ log("------------------------------create table-------------------------------------------------") }}
{{ log(ty_tounen_start_dt_1) }}
    {% set tablename %}
    {% if target.name=='prod' %}
                jpnwks_integration.wk_iv_logical_dly_temp
            {% else %}
                {{schema}}.jpnwks_integration__wk_iv_logical_dly_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    jpnwks_integration.wk_iv_logical_dly_temp
                {% else %}
                    {{schema}}.jpnwks_integration__wk_iv_logical_dly_temp
                {% endif %}
    (       		
            cstm_cd varchar(10),
            jan_cd varchar(18),
            data_type varchar(10),
            calday timestamp_ntz(9),
            pc number(5,0),
            qty number(20,10),
            net_prc number(20,3)

    );      
    {{ log("------------------------------first insert-------------------------------------------------") }}

        {#     insert into {% if target.name=='prod' %}
                     jpnwks_integration.wk_iv_logical_dly_temp
               {% else %}
                     {{schema}}.jpnwks_integration__wk_iv_logical_dly_temp
               {% endif %}
        #}
        --     (
        --     CALDAY,
        --     DATA_TYPE,
        --     CSTM_CD,
        --     JAN_CD,
        --     PC,
        --     QTY,
        --     NET_PRC
        --     )
        -- SELECT SID.CALDAY,
        --     'SELLIN',
        --     SID.CUSTOMER,
        --     ITM.JAN_CD,
        --     NVL2(nullif(MAX(ITM.PC), ''), MAX(ITM.PC), NULL),
        --     NVL(SUM(SID.JCP_QTY * - 1), 0),
        --     NVL(SUM(SID.AMOCCC * - 1), 0)
        -- FROM DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.DW_SI_SELL_IN_DLY SID
        -- INNER JOIN DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.EDI_ITEM_M ITM ON SID.MATERIAL = ITM.ITEM_CD
        -- WHERE to_date(SID.CALDAY) >= LEAST('{{ty_exec_start_dt_1}}', '{{ty_tounen_start_dt_1}}', to_date( (substring('{{ty_exec_start_dt_1}}', 1, 4) || '-01-01')))
        --     AND SID.MATERIAL != 'REBATE'
        --     AND SID.ACCOUNT IN  ('402000', '402098')
        -- GROUP BY SID.CALDAY,
        --     SID.CUSTOMER,
        --     ITM.JAN_CD
    {{ log("------------------------------least value calculation-------------------------------------------------") }}
    {% set get_least_query %}
        select LEAST('{{ty_exec_start_dt_1}}', '{{ty_tounen_start_dt_1}}', to_date( (substring('{{ty_exec_start_dt_1}}', 1, 4) || '-01-01')));
    {% endset %}
    {% set least_query = run_query(get_least_query) %}
      {% if execute %}
        {% set least = least_query.columns[0].values()[0] %}
    {% else %}
        {% set least = 0 %}
    {% endif %}
     {{ log("------------------------------least value-------------------------------------------------") }}
    {{ log(least) }}
     {{ log("------------------------------least value-------------------------------------------------") }}
    {{ log("----------------------------------------------------------------------------------------") }}
    {{ log("----------------------------------second insert---------------------------------------------") }}

    {#
        insert into {% if target.name=='prod' %}
                jpnwks_integration.wk_iv_logical_dly_temp
        {% else %}
                {{schema}}.jpnwks_integration__wk_iv_logical_dly_temp
        {% endif %}
    #}
        -- (
        --     CALDAY,
        --     DATA_TYPE,
        --     CSTM_CD,
        --     JAN_CD,
        --     PC,
        --     QTY,
        --     NET_PRC
        --     )
        -- SELECT SPF.SHP_DATE,
        --     'SELLOUT',
        --     SPF.JCP_SHP_TO_CD,
        --     SPF.ITEM_CD,
        --     NVL2(nullif(MAX(ITM.PC), ''), MAX(ITM.PC), NULL),
        --     NVL(SUM(SPF.QTY), 0),
        --     NVL(SUM(SPF.QTY * ITM.UNT_PRC), 0)
        -- FROM DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.DW_SO_SELL_OUT_DLY SPF
        -- INNER JOIN DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.EDI_ITEM_M ITM ON SPF.ITEM_CD = ITM.JAN_CD_SO
        -- GROUP BY SPF.SHP_DATE,
        --     SPF.JCP_SHP_TO_CD,
        --     SPF.ITEM_CD;
    {{ log("----------------------------------end of second insert---------------------------------------------") }}

     {{ log("----------------------------------------------------------------------------------------") }}

    {{ log("---------------------------------------ty_end_date calculation-----------------------------") }}
  {#      {% set t_end_query = run_query("") %}
        {% set ty_exec_end_dt = t_end_query.columns[0].values()[0] %}
    {{ log("---------------------------------------ty_end_date-----------------------------") }}
        {{ log(ty_exec_end_dt) }}#}

        {% set t_end_query %}
    SELECT to_date(MAX(CALDAY)) FROM 
        {% if target.name=='prod' %}
                jpnwks_integration.wk_iv_logical_dly_temp
        {% else %}
                {{schema}}.jpnwks_integration__wk_iv_logical_dly_temp
        {% endif %};    
        {% endset %}
    {% set t_end_query_result = run_query(t_end_query) %}
      {% if execute %}
        {% set ty_exec_end_dt = t_end_query_result.columns[0].values()[0] %}
    {% else %}
        {% set ty_exec_end_dt = 0 %}
    {% endif %}
     {{ log("------------------------------ty_end_date-------------------------------------------------") }}
    {{ log(ty_exec_end_dt) }}
     {{ log("----------------------------------------------------------------------------------------") }}
     {{ log("----------------------------------------------------------------------------------------") }}
     {{ log("-----------------------------------------third insert--------------------------------------") }}

 {# insert into {% if target.name=='prod' %}
                jpnwks_integration.wk_iv_logical_dly_temp
        {% else %}
                {{schema}}.jpnwks_integration__wk_iv_logical_dly_temp
        {% endif %}#}
--     (
-- 	CALDAY,
-- 	DATA_TYPE,
-- 	CSTM_CD,
-- 	JAN_CD,
-- 	PC,
-- 	QTY,
-- 	NET_PRC
-- 	)
-- SELECT CALDAY,
-- 	DATA_TYPE,
-- 	CSTM_CD,
-- 	JAN_CD,
-- 	PC,
-- 	QTY,
-- 	NET_PRC
-- FROM DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.DM_IV_LOGICAL_DLY
-- WHERE to_date(CALDAY) = to_date(DATEADD(day, - 1, '{{ty_exec_start_dt_1}}'))
-- 	AND DATA_TYPE = 'INVT';
     {{ log("-----------------------------------------end of third insert--------------------------------------") }}
     {{ log("----------------------------------------------------------------------------------------") }}
     {{ log("-----------------------------------------create table inventory_cursor--------------------------------------") }}

    {% set query2 %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    jpnwks_integration.inventory_cursor
                {% else %}
                    {{schema}}.jpnwks_integration__inventory_cursor
        {% endif %}
    (       		
            year_445 varchar(4),
            bgn_ymd_dt timestamp_ntz(9),
            end_ymd_dt timestamp_ntz(9),
            tounen_flg int,
            year_end_flg int

    );     
        truncate {% if target.name=='prod' %}
                jpnwks_integration.inventory_cursor
        {% else %}
                {{schema}}.jpnwks_integration__inventory_cursor
        {% endif %};
     {{ log("-----------------------------------------insert for inventory_cursor--------------------------------------") }}

       insert into {% if target.name=='prod' %}
                jpnwks_integration.inventory_cursor
        {% else %}
                {{schema}}.jpnwks_integration__inventory_cursor
        {% endif %} (
            year_445,
            bgn_ymd_dt,
            end_ymd_dt,
            tounen_flg,
            year_end_flg
            )
        SELECT CLD.YEAR_445,
            CLD.BGN_YMD_DT,
            CLD.END_YMD_DT,
            CASE 
                WHEN CLD.YEAR_445 = CLD.YEARR
                    THEN '1'
                ELSE '0'
                END TOUNEN_FLG,
            CASE 
                WHEN YE.YEARR IS NOT NULL
                    THEN '1'
                ELSE '0'
                END YEAR_END_FLG
        FROM (
            SELECT MAIN.YEAR_445 YEAR_445,
                MIN(MAIN.YEAR) YEARR,
                MIN(MAIN.YMD_DT) BGN_YMD_DT,
                MAX(MAIN.YMD_DT) END_YMD_DT
            FROM DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.MT_CLD MAIN
            INNER JOIN DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.MT_CLD SUB ON MAIN.YEAR_445 = SUB.YEAR_445
                AND TO_CHAR(SUB.YMD_DT, 'YYYYMMDD') >= '{{ty_exec_start_dt_1}}'
            GROUP BY MAIN.YEAR_445
            ) CLD
        LEFT OUTER JOIN (
            SELECT (TO_NUMBER(TO_CHAR(INVT_DT, 'YYYY'), '9999') + 1)::varchar YEARR
            FROM DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.DW_IV_YEAR_END
            GROUP BY  (TO_NUMBER(TO_CHAR(INVT_DT, 'YYYY'), '9999') + 1)::varchar
            ) YE ON CLD.YEAR_445 = YE.YEARR
        ORDER BY CLD.YEAR_445;

        {% endset %}
    {% do run_query(query2) %}

        {% set get_year_query %}
        select  year_445 from {% if target.name=='prod' %}
                jpnwks_integration.inventory_cursor
        {% else %}
                {{schema}}.jpnwks_integration__inventory_cursor
        {% endif %}
        ORDER BY year_445;
    {% endset %}
    {{ log("Try to execute the query to fetch year from inventory_cursor table ") }}
    {{ log("===============================================================================================") }}
        {% set get_rows_query_result = run_query(get_year_query) %}
        {% if execute %}
                {% set year_445 = get_rows_query_result.columns[0].values()[:]  %}
            {% else %}
                {% set year_445 = [] %}
            {% endif %}

    {{ log(year_445) }}
     {{ log("===============================================================================================") }}

       {% set get_col_values_query %}

        select  SUBSTRING(BGN_YMD_DT,1,10) as BGN_YMD_DT, SUBSTRING(END_YMD_DT,1,10) as END_YMD_DT, TOUNEN_FLG, YEAR_END_FLG 
        from {% if target.name=='prod' %}
                jpnwks_integration.inventory_cursor
        {% else %}
                {{schema}}.jpnwks_integration__inventory_cursor
        {% endif %} where year_445 in {{year_445}};

    {% endset %}

    {{ log("Try to execute the query to fetch values from inventory_cursor table ") }}
    {{ log("===============================================================================================") }}
        {% set get_values_query_result = run_query(get_col_values_query) %}
        {% if execute %}
                {% set BGN_YMD_DT = get_values_query_result.columns[0].values()%}  
               {% set END_YMD_DT = get_values_query_result.columns[1].values()%}
                {% set TOUNEN_FLG = get_values_query_result.columns[2].values()%}
                {% set YEAR_END_FLG = get_values_query_result.columns[3].values()%}
            {% else %}
                {% set BGN_YMD_DT = []%}
                 {% set END_YMD_DT = []%}
                {% set TOUNEN_FLG = []%}
                {% set YEAR_END_FLG = []%}
            {% endif %}

    {{ log(BGN_YMD_DT) }}
    {{ log(END_YMD_DT) }}
    {{ log(TOUNEN_FLG) }}
    {{ log(YEAR_END_FLG) }}
    {{ log("===============================================================================================") }}

 

  {% endset %}
    {% do run_query(query) %}


{% endmacro %}