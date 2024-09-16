{% macro build_dm_iv_logical_dly() %}
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
{% set t_start_query = run_query("SELECT SUBSTRING(MIN(MAIN.YMD_DT),1,10) as ty_tounen_start_dt FROM {{ env_var('DBT_ENV_CORE_DB') }}.JPNEDW_INTEGRATION.MT_CLD	MAIN WHERE	EXISTS
(SELECT * FROM	{{ env_var('DBT_ENV_CORE_DB') }}.JPNEDW_INTEGRATION.MT_CLD SUB 
WHERE MAIN.YEAR_445 = SUB.YEAR_445	AND	SUB.YMD_DT = to_date(current_timestamp()))") %}
{% set ty_tounen_start_dt_1 = t_start_query.columns[0].values()[0] %}

{{ log(ty_exec_start_dt_1) }}
{{ log("------------------------------create table-------------------------------------------------") }}
{{ log(ty_tounen_start_dt_1) }}
    {% set tablename %}
    {% if target.name=='prod' %}
                jpnwks_integration.wk_iv_logical_dly
            {% else %}
                {{schema}}.jpnwks_integration__wk_iv_logical_dly
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    jpnwks_integration.wk_iv_logical_dly
                {% else %}
                    {{schema}}.jpnwks_integration__wk_iv_logical_dly
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

    truncate {% if target.name=='prod' %}
                    jpnwks_integration.wk_iv_logical_dly
                {% else %}
                    {{schema}}.jpnwks_integration__wk_iv_logical_dly
                {% endif %};
    {{ log("------------------------------first insert-------------------------------------------------") }}

             insert into {% if target.name=='prod' %}
                     jpnwks_integration.wk_iv_logical_dly
               {% else %}
                     {{schema}}.jpnwks_integration__wk_iv_logical_dly
               {% endif %}
        
            (
            CALDAY,
            DATA_TYPE,
            CSTM_CD,
            JAN_CD,
            PC,
            QTY,
            NET_PRC
            )
        SELECT SID.CALDAY,
            'SELLIN',
            SID.CUSTOMER,
            ITM.JAN_CD,
            NVL2(nullif(MAX(ITM.PC), ''), MAX(ITM.PC), NULL),
            NVL(SUM(SID.JCP_QTY * - 1), 0),
            NVL(SUM(SID.AMOCCC * - 1), 0)
        FROM {{ env_var('DBT_ENV_CORE_DB') }}.JPNEDW_INTEGRATION.DW_SI_SELL_IN_DLY SID
        INNER JOIN {{ env_var('DBT_ENV_CORE_DB') }}.JPNEDW_INTEGRATION.EDI_ITEM_M ITM ON SID.MATERIAL = ITM.ITEM_CD
        WHERE to_date(SID.CALDAY) >= LEAST('{{ty_exec_start_dt_1}}', '{{ty_tounen_start_dt_1}}', to_date( (substring('{{ty_exec_start_dt_1}}', 1, 4) || '-01-01')))
            AND SID.MATERIAL != 'REBATE'
            AND SID.ACCOUNT IN  ('402000', '402098')
        GROUP BY SID.CALDAY,
            SID.CUSTOMER,
            ITM.JAN_CD;
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

    
        insert into {% if target.name=='prod' %}
                jpnwks_integration.wk_iv_logical_dly
        {% else %}
                {{schema}}.jpnwks_integration__wk_iv_logical_dly
        {% endif %}
    
        (
            CALDAY,
            DATA_TYPE,
            CSTM_CD,
            JAN_CD,
            PC,
            QTY,
            NET_PRC
            )
        SELECT SPF.SHP_DATE,
            'SELLOUT',
            SPF.JCP_SHP_TO_CD,
            SPF.ITEM_CD,
            NVL2(nullif(MAX(ITM.PC), ''), MAX(ITM.PC), NULL),
            NVL(SUM(SPF.QTY), 0),
            NVL(SUM(SPF.QTY * ITM.UNT_PRC), 0)
        FROM {{ env_var('DBT_ENV_CORE_DB') }}.JPNEDW_INTEGRATION.DW_SO_SELL_OUT_DLY SPF
        INNER JOIN {{ env_var('DBT_ENV_CORE_DB') }}.JPNEDW_INTEGRATION.EDI_ITEM_M ITM ON SPF.ITEM_CD = ITM.JAN_CD_SO
        GROUP BY SPF.SHP_DATE,
            SPF.JCP_SHP_TO_CD,
            SPF.ITEM_CD;
    {{ log("----------------------------------end of second insert---------------------------------------------") }}

     {{ log("----------------------------------------------------------------------------------------") }}

    {{ log("---------------------------------------ty_end_date calculation-----------------------------") }}
        
    {{ log("---------------------------------------ty_end_date-----------------------------") }}
        

        {% set t_end_query %}
    SELECT to_date(MAX(CALDAY)) FROM 
        {% if target.name=='prod' %}
                jpnwks_integration.wk_iv_logical_dly
        {% else %}
                {{schema}}.jpnwks_integration__wk_iv_logical_dly
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

  insert into {% if target.name=='prod' %}
                jpnwks_integration.wk_iv_logical_dly
        {% else %}
                {{schema}}.jpnwks_integration__wk_iv_logical_dly
        {% endif %}
                (
                CALDAY,
                DATA_TYPE,
                CSTM_CD,
                JAN_CD,
                PC,
                QTY,
                NET_PRC
                )
            SELECT CALDAY,
                DATA_TYPE,
                CSTM_CD,
                JAN_CD,
                PC,
                QTY,
                NET_PRC
            FROM {{ env_var('DBT_ENV_CORE_DB') }}.JPNEDW_INTEGRATION.DM_IV_LOGICAL_DLY
            WHERE to_date(CALDAY) = to_date(DATEADD(day, - 1, '{{ty_exec_start_dt_1}}'))
                AND DATA_TYPE = 'INVT';
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
            FROM {{ env_var('DBT_ENV_CORE_DB') }}.JPNEDW_INTEGRATION.MT_CLD MAIN
            INNER JOIN {{ env_var('DBT_ENV_CORE_DB') }}.JPNEDW_INTEGRATION.MT_CLD SUB ON MAIN.YEAR_445 = SUB.YEAR_445
                AND TO_CHAR(SUB.YMD_DT, 'YYYYMMDD') >= '{{ty_exec_start_dt_1}}'
            GROUP BY MAIN.YEAR_445
            ) CLD
        LEFT OUTER JOIN (
            SELECT (TO_NUMBER(TO_CHAR(INVT_DT, 'YYYY'), '9999') + 1)::varchar YEARR
            FROM {{ env_var('DBT_ENV_CORE_DB') }}.JPNEDW_INTEGRATION.DW_IV_YEAR_END
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
    {% set year_445 = [] %}
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
       {% set list_length = year_445 | length %}
      
        {% for i in range(0,list_length)%}
        {{ log("Iterate year = " ~ year_445[i]) }}
        {{ log(year_445[i]) }}
       

       {% set get_col_values_query %}

        select  SUBSTRING(BGN_YMD_DT,1,10) as BGN_YMD_DT, SUBSTRING(END_YMD_DT,1,10) as END_YMD_DT, TOUNEN_FLG, YEAR_END_FLG 
        from {% if target.name=='prod' %}
                jpnwks_integration.inventory_cursor
        {% else %}
                {{schema}}.jpnwks_integration__inventory_cursor
        {% endif %} where year_445 = '{{year_445[i]}}';

    {% endset %}

    {{ log("Try to execute the query to fetch values from inventory_cursor table ") }}
    {{ log("===============================================================================================") }}
        {% set BGN_YMD_DT = ''%}
                 {% set END_YMD_DT = ''%}
                {% set TOUNEN_FLG = ''%}
                {% set YEAR_END_FLG = ''%}
        
        {% set get_values_query_result = run_query(get_col_values_query) %}
        {% if execute %}
                {% set BGN_YMD_DT = get_values_query_result.columns[0].values()%}  
               {% set END_YMD_DT = get_values_query_result.columns[1].values() %}
                {% set TOUNEN_FLG = get_values_query_result.columns[2].values()%}
                {% set YEAR_END_FLG = get_values_query_result.columns[3].values()%}
            {% else %}
                {% set BGN_YMD_DT = ''%}
                 {% set END_YMD_DT = ''%}
                {% set TOUNEN_FLG = ''%}
                {% set YEAR_END_FLG = ''%}
            {% endif %}

    {{ log(BGN_YMD_DT) }}
    {{ log(END_YMD_DT) }}
    {{ log(TOUNEN_FLG) }}
    {{ log(YEAR_END_FLG) }}
    {{ log("===============================================================================================") }}
    
    {% if BGN_YMD_DT | format > ty_exec_start_dt_1 %}
    {% set a = '1'%}
    {% elif  BGN_YMD_DT | format < ty_exec_start_dt_1%}
    {% set a = '-1'%}
    {% else %}
    {% set a = '0'%}
    {% endif %}

    {{log(a)}}
    {% set BGN_YMD_DT_up = BGN_YMD_DT | join(', ')%}
    {% set get_ty_calday_query %}
        select to_date(DATEADD(DAY,1,to_date('{{BGN_YMD_DT_up}}')))    
    {% endset %}
    {% set get_ty_calday = run_query(get_ty_calday_query) %}
      {% if execute %}
        {% set ty_calday = get_ty_calday.columns[0].values()[0] %}
    {% else %}
        {% set ty_calday = 0 %}
    {% endif %}
    {{log(ty_calday)}}

    
    {% if a == -1 or YEAR_END_FLG | format == 0 %}
        {% if a == -1 or a | format == 1%}
            {% set d = ty_calday%}
        {% else %}
            {% set d = ''%}
        {% endif %}
        {{log(d)}}
    {% elif  a == 1  and YEAR_END_FLG | format == 1%}
        {% set get_d_last_period_dt_query %}
            select to_date((('{{year_445[i]}}')::number - 1)|| '-12-31')
        {% endset %}
        {% set get_d_last_period_dt = run_query(get_d_last_period_dt_query) %}
        {% if execute %}
            {% set d_last_period_dt = get_d_last_period_dt.columns[0].values()[0] %}
        {% else %}
            {% set d_last_period_dt = '0' %}
        {% endif %}
        {{log(d_last_period_dt)}}


        {% if '1' == TOUNEN_FLG[0] | format %}
                {% set z = '1'%}
                insert into {% if target.name=='prod' %}
                     jpnwks_integration.wk_iv_logical_dly
               {% else %}
                     {{schema}}.jpnwks_integration__wk_iv_logical_dly
               {% endif %}
					(
						CALDAY,
						DATA_TYPE,
						CSTM_CD,
						JAN_CD,
						PC,
						QTY,
						NET_PRC
					)
					SELECT
						'{{BGN_YMD_DT_up}}',
						'INVT_BGN',
						INVT.CSTM_CD,
						INVT.JAN_CD,
						INVT.PC,
						NVL(INVT.CS_QTY * ITM.PC, 0) + NVL(INVT.QTY, 0) + NVL(INOUT.QTY, 0),
						ITM.UNT_PRC * NVL(INVT.CS_QTY * INVT.PC, 0) + ITM.UNT_PRC * NVL(INVT.QTY, 0) + NVL(INOUT.NET_PRC, 0)
					FROM
					(
						SELECT
							INVT_DT,
							CSTM_CD,
							JAN_CD,
							MAX(PC)			PC,
							SUM(CS_QTY)		CS_QTY,
							SUM(QTY)		QTY
						FROM
							JPNEDW_INTEGRATION.DW_IV_YEAR_END
						WHERE
							to_date(INVT_DT) = '{{d_last_period_dt}}'
						GROUP BY
							INVT_DT,
							CSTM_CD,
							JAN_CD
					)	INVT
					LEFT OUTER JOIN
					(
						SELECT
							CSTM_CD,
							JAN_CD,
							SUM(CASE WHEN  DATA_TYPE ='SELLIN'
								THEN 1 ELSE -1 END * QTY) QTY,
							SUM(CASE WHEN  DATA_TYPE ='SELLIN' 
								THEN 1 ELSE -1 END * NET_PRC) NET_PRC
						FROM
							{% if target.name=='prod' %}
                                    jpnwks_integration.wk_iv_logical_dly
                            {% else %}
                                    {{schema}}.jpnwks_integration__wk_iv_logical_dly
                            {% endif %}
						WHERE
							to_date(CALDAY) > '{{d_last_period_dt}}'
						AND
							to_date(CALDAY) < '{{BGN_YMD_DT_up}}'
						AND
							DATA_TYPE IN ('SELLIN', 'SELLOUT')
						GROUP BY
							CSTM_CD,
							JAN_CD
					)	INOUT
					ON
						INVT.CSTM_CD = INOUT.CSTM_CD
					AND
						INVT.JAN_CD = INOUT.JAN_CD
					INNER JOIN
							{{ env_var('DBT_ENV_LOAD_DB') }}.jpnsdl_raw.EDI_ITEM_M	ITM
						ON
							INVT.JAN_CD = ITM.JAN_CD_SO	;
                    insert into {% if target.name=='prod' %}
                     jpnwks_integration.wk_iv_logical_dly
               {% else %}
                     {{schema}}.jpnwks_integration__wk_iv_logical_dly
               {% endif %}
				(
					CALDAY,
					DATA_TYPE,
					CSTM_CD,
					JAN_CD,
					PC,
					QTY,
					NET_PRC
				)
				SELECT
					NVL(INVT.CALDAY, INOUT.CALDAY),
					'INVT',
					NVL(INVT.CSTM_CD, INOUT.CSTM_CD),
					NVL(INVT.JAN_CD, INOUT.JAN_CD),
					NVL(INVT.PC, INOUT.PC),
					NVL(INVT.QTY, 0) + NVL(INOUT.QTY, 0),
					NVL(INVT.NET_PRC, 0) + NVL(INOUT.NET_PRC, 0)
				FROM
				(
					SELECT
						CALDAY,
						DATA_TYPE,
						CSTM_CD,
						JAN_CD,
						PC,
						QTY,
						NET_PRC
					FROM
						{% if target.name=='prod' %}
                            jpnwks_integration.wk_iv_logical_dly
                    {% else %}
                            {{schema}}.jpnwks_integration__wk_iv_logical_dly
                    {% endif %}
					WHERE
						to_date(CALDAY) ='{{BGN_YMD_DT_up}}'
					AND
						DATA_TYPE ='INVT_BGN'
				)	INVT
			FULL OUTER JOIN
				(
					SELECT
						CSTM_CD,
						JAN_CD,
						CALDAY,
						MAX(PC) PC,
						SUM(CASE WHEN  DATA_TYPE = 'SELLIN'
							THEN 1 ELSE -1 END * QTY) QTY,
						SUM(CASE WHEN  DATA_TYPE = 'SELLIN'
							THEN 1 ELSE -1 END * NET_PRC) NET_PRC
					FROM
						{% if target.name=='prod' %}
                     jpnwks_integration.wk_iv_logical_dly
                    {% else %}
                            {{schema}}.jpnwks_integration__wk_iv_logical_dly
                    {% endif %}
					WHERE
						to_date(CALDAY) = '{{BGN_YMD_DT_up}}'
					AND
						DATA_TYPE IN ('SELLIN', 'SELLOUT')
					GROUP BY
						CSTM_CD,
						JAN_CD,
						CALDAY
				)	INOUT
				ON
					INVT.CSTM_CD = INOUT.CSTM_CD
				AND
					INVT.JAN_CD = INOUT.JAN_CD
				AND
					INVT.CALDAY = INOUT.CALDAY;

                    
        {% elif  '1' != TOUNEN_FLG[0] | format  %}
                {% set z = '-1'%}

                insert into {% if target.name=='prod' %}
                     jpnwks_integration.wk_iv_logical_dly
               {% else %}
                     {{schema}}.jpnwks_integration__wk_iv_logical_dly
                {%endif%}
					(
						CALDAY,
						DATA_TYPE,
						CSTM_CD,
						JAN_CD,
						PC,
						QTY,
						NET_PRC
					)
		SELECT
						'{{BGN_YMD_DT_up}}',
						'INVT_BGN',
						INVT.CSTM_CD,
						INVT.JAN_CD,
						INVT.PC,
						NVL(INVT.CS_QTY * ITM.PC, 0) + NVL(INVT.QTY, 0) + NVL(INOUT.QTY, 0),
						ITM.UNT_PRC * NVL(INVT.CS_QTY * INVT.PC, 0) + ITM.UNT_PRC * NVL(INVT.QTY, 0) + NVL(INOUT.NET_PRC, 0)
					FROM
					(
						SELECT
							INVT_DT,
							CSTM_CD,
							JAN_CD,
							MAX(PC)			PC,
							SUM(CS_QTY)		CS_QTY,
							SUM(QTY)		QTY
						FROM
							JPNEDW_INTEGRATION.DW_IV_YEAR_END
						WHERE
							to_date(INVT_DT) = '{{d_last_period_dt}}'
						GROUP BY
							INVT_DT,
							CSTM_CD,
							JAN_CD
					)	INVT
					LEFT OUTER JOIN
					(
						SELECT
							CSTM_CD,
							JAN_CD,
							SUM(CASE WHEN  DATA_TYPE ='SELLIN'
								THEN 1 ELSE -1 END * QTY) QTY,
							SUM(CASE WHEN  DATA_TYPE ='SELLIN' 
								THEN 1 ELSE -1 END * NET_PRC) NET_PRC
						FROM
							{% if target.name=='prod' %}
                            jpnwks_integration.wk_iv_logical_dly
                            {% else %}
                                    {{schema}}.jpnwks_integration__wk_iv_logical_dly
                            {% endif %}
						WHERE
							to_date(CALDAY) > '{{d_last_period_dt}}'
						AND
							to_date(CALDAY) < '{{BGN_YMD_DT_up}}'
						AND
							DATA_TYPE IN ('SELLIN', 'SELLOUT')
						GROUP BY
							CSTM_CD,
							JAN_CD
					)	INOUT
					ON
						INVT.CSTM_CD = INOUT.CSTM_CD
					AND
						INVT.JAN_CD = INOUT.JAN_CD
					INNER JOIN
							JPNEDW_INTEGRATION.EDI_ITEM_M	ITM
						ON
							INVT.JAN_CD = ITM.JAN_CD_SO	;
        
        {% else %}
                {% set z = '0'%}
        {% endif %}
    
        {{log("z ="+z)}}

    {% set x = '-1'%}
    {% else %}
    {% set x = '0'%}
    {% endif %}
     {{log("x ="+x)}}
 
     {% if END_YMD_DT | format > ty_exec_end_dt | format %}
            {% set b = '1'%}
    {% elif  END_YMD_DT | format < ty_exec_end_dt | format %}
            {% set b = '-1'%}
    {% else %}
            {% set b = '0'%}
    {% endif %}
        {{log("b "+b)}}

    {% if b  == '1' %}
        {% set b1 = least%}
    {% elif b  == '-1'%}
        {% set b1 = least%}
    {% else %}
        {% set b1 = ''%}
    {% endif %}
        {{log("b1 "~b1)}}


    {{log("least "~least)}}
    {{log("calday "~ty_calday)}}
    {% if Least | format > ty_calday | format %}
            {% set c = '1'%}
    {% elif  Least | format < ty_calday | format %}
            {% set c = '-1'%}
    {% else %}
            {% set c = '0'%}
    {% endif %}
        {{log("c "+c)}}

     {% if c  == '1' %}
        {% set b = least%}
    {% elif c  == '-1'%}
        {% set b = least%}
    {% else %}
        {% set b = ''%}
    {% endif %}
        {{log("b updatted  "~b)}}

    insert into {% if target.name=='prod' %}
                     jpnwks_integration.wk_iv_logical_dly
               {% else %}
                     {{schema}}.jpnwks_integration__wk_iv_logical_dly
                {%endif%}
				(
					CALDAY,
					DATA_TYPE,
					CSTM_CD,
					JAN_CD,
					PC,
					QTY,
					NET_PRC
				)
				SELECT
					to_date(DATEADD(day,+1,to_date(NVL(INVT.CALDAY, INOUT.CALDAY)))),
					'INVT',
					NVL(INVT.CSTM_CD, INOUT.CSTM_CD),
					NVL(INVT.JAN_CD, INOUT.JAN_CD),
					NVL(INVT.PC, INOUT.PC),
					NVL(INVT.QTY, 0)::number + NVL(INOUT.QTY, 0)::number,
					NVL(INVT.NET_PRC, 0)::number + NVL(INOUT.NET_PRC, 0)::number
				FROM
				(
					SELECT
						CALDAY,
						DATA_TYPE,
						CSTM_CD,
						JAN_CD,
						PC,
						QTY,
						NET_PRC
					FROM
						 {% if target.name=='prod' %}
                     jpnwks_integration.wk_iv_logical_dly
                    {% else %}
                            {{schema}}.jpnwks_integration__wk_iv_logical_dly
                        {%endif%}
					WHERE
            to_date(CALDAY) = to_date(DATEADD(day,-1,'{{ty_calday}}'))
                    AND
						DATA_TYPE = 'INVT'
				)	INVT
				FULL OUTER JOIN
				(
					SELECT
						CSTM_CD,
						JAN_CD,
					 to_date(DATEADD(day,-1,CALDAY)) as CALDAY,
						MAX(PC) PC,
						SUM(CASE WHEN  DATA_TYPE = 'SELLIN'
							THEN 1 ELSE -1 END * QTY) QTY,
						SUM(CASE WHEN  DATA_TYPE = 'SELLIN'
							THEN 1 ELSE -1 END * NET_PRC) NET_PRC
					FROM
						 {% if target.name=='prod' %}
                     jpnwks_integration.wk_iv_logical_dly
               {% else %}
                     {{schema}}.jpnwks_integration__wk_iv_logical_dly
                {%endif%}
					WHERE
						to_date(CALDAY) =to_date('{{ty_calday}}')
					AND
						DATA_TYPE IN ('SELLIN','SELLOUT')
					GROUP BY
						CSTM_CD,
						JAN_CD,
						CALDAY
				)	INOUT
				ON
					INVT.CSTM_CD = INOUT.CSTM_CD
				AND
					INVT.JAN_CD = INOUT.JAN_CD
				AND
					INVT.CALDAY = INOUT.CALDAY;

    {% set get_ty_calday_up_query %}
        select to_date(DATEADD(DAY,1,to_date('{{ty_calday}}')))    
    {% endset %}
    {% set get_ty_calday_up = run_query(get_ty_calday_up_query) %}
      {% if execute %}
        {% set ty_calday_up = get_ty_calday_up.columns[0].values()[0] %}
    {% else %}
        {% set ty_calday_up = 0 %}
    {% endif %}
    {{log("ty_calday_up "~ ty_calday_up)}}

 {% endfor%}

    DELETE FROM {% if target.name=='prod' %}
                     jpnedw_integration.DM_IV_LOGICAL_DLY
                    {% else %}
                            {{schema}}.jpnedw_integration__DM_IV_LOGICAL_DLY
                        {%endif%}
		WHERE
		(
				to_date(CALDAY) >= to_date('{{ty_exec_start_dt_1}}') 
			AND
				DATA_TYPE IN ('INVT_BGN', 'INVT')
		)
		OR
		(
				to_date(CALDAY) >= LEAST('{{ty_tounen_start_dt_1}}', '{{ty_exec_start_dt_1}}')
			AND
				DATA_TYPE = 'SELLIN'
		)
		OR
			DATA_TYPE ='SELLOUT';


    INSERT INTO
			{% if target.name=='prod' %}
                     jpnedw_integration.DM_IV_LOGICAL_DLY
                    {% else %}
                            {{schema}}.jpnedw_integration__DM_IV_LOGICAL_DLY
            {%endif%}
		(
			CALDAY,
			DATA_TYPE,
			CSTM_CD,
			JAN_CD,
			PC,
			QTY,
			NET_PRC,
			UPDATE_DT
		)
		SELECT
			CALDAY,
			DATA_TYPE,
			CSTM_CD,
			JAN_CD,
			PC,
			QTY,
			NET_PRC,
			current_timestamp()::timestamp_ntz(9)
		FROM
			{% if target.name=='prod' %}
                     jpnwks_integration.wk_iv_logical_dly
               {% else %}
                     {{schema}}.jpnwks_integration__wk_iv_logical_dly
                {%endif%}
		WHERE
		(
				to_date(CALDAY) >= to_date('{{ty_exec_start_dt_1}}')
			AND
				DATA_TYPE IN ('INVT_BGN', 'INVT')
		)
		OR
		(
				to_date(CALDAY) >= LEAST( '{{ty_tounen_start_dt_1}}', '{{ty_exec_start_dt_1}}')
			AND
				DATA_TYPE = 'SELLIN'
		)
		OR
			DATA_TYPE = 'SELLOUT';
  {% endset %}
    {% do run_query(query) %}


{% endmacro %}