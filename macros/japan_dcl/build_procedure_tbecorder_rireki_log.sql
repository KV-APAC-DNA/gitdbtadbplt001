{% macro build_procedure_tbecorder_rireki_log() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                jpndcledw_integration.tbecorder_log_rireki_tmp
            {% else %}
                {{schema}}.jpndcledw_integration__tbecorder_rireki_log_tmp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    jpndcledw_integration.tbecorder_rireki_log
                {% else %}
                    {{schema}}.jpndcledw_integration__tbecorder_rireki_log
                {% endif %}
            (   
                DIORDERID NUMBER(38,0),
                T_KBN VARCHAR(1),
                INSERTED_DATE TIMESTAMP_NTZ(9),
                INSERTED_BY VARCHAR(100),
                UPDATED_DATE TIMESTAMP_NTZ(9),
                UPDATED_BY VARCHAR(100)        
            );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            jpndcledw_integration.tbecorder_rireki_log
        {% else %}
            {{schema}}.jpndcledw_integration__tbecorder_rireki_log
        {% endif %};
    {% endset %}
    {% set query %}
        UPDATE {{tablename}}
        SET T_KBN = 'T',
            updated_date = GETDATE(),
            updated_by = 'ETL_Batch'
        FROM (
            SELECT DIORDERHISTID,
                    DIORDERID
            FROM {{ref('jpndclitg_integration__c_tbecorderhistory_1')}} C_TBECORDERHISTORY
            WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                    AND C_DSTEMPOCODE IS NOT NULL
            ) LIST
        WHERE {{tablename}}.DIORDERID = LIST.DIORDERID
            AND {{tablename}}.DIORDERHISTID = LIST.DIORDERHISTID;

        INSERT INTO {{tablename}} (
            DIORDERHISTID,
            DIORDERID,
            T_KBN
            )
        SELECT LIST.DIORDERHISTID,
            LIST.DIORDERID,
            'T'
        FROM (
            SELECT DIORDERHISTID,
                    DIORDERID
            FROM {{ref('jpndclitg_integration__c_tbecorderhistory_1')}} C_TBECORDERHISTORY
            WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                    AND C_DSTEMPOCODE IS NOT NULL
            ) LIST
        LEFT JOIN {{tablename}} LOG ON LOG.DIORDERID = LIST.DIORDERID
            AND LOG.DIORDERHISTID = LIST.DIORDERHISTID
        WHERE LIST.DIORDERID IS NULL
            AND LIST.DIORDERHISTID IS NULL;

        UPDATE {{ref('jpndclitg_integration__c_tbecorderhistory_1')}}
        SET C_DSUKETSUKETELCOMPANYCD = 'TNP',
            updated_date = GETDATE(),
            updated_by = 'ETL_Batch'
        WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
            AND C_DSTEMPOCODE IS NOT NULL;

        UPDATE {{tablename}}
        SET T_KBN = 'W',
            updated_date = GETDATE(),
            updated_by = 'ETL_Batch'
        FROM (
            SELECT DIORDERHISTID,
                    DIORDERID
            FROM {{ref('jpndclitg_integration__c_tbecorderhistory_1')}} C_TBECORDERHISTORY
            WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                    AND dirouteid = '5'
            ) LIST
        WHERE {{tablename}}.DIORDERID = LIST.DIORDERID
            AND {{tablename}}.DIORDERHISTID = LIST.DIORDERHISTID;

        INSERT INTO {{tablename}} (
            DIORDERHISTID,
            DIORDERID,
            T_KBN
            )
        SELECT LIST.DIORDERHISTID,
            LIST.DIORDERID,
            'W'
        FROM (
            SELECT DIORDERHISTID,
                    DIORDERID
            FROM {{ref('jpndclitg_integration__c_tbecorderhistory_1')}} C_TBECORDERHISTORY
            WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                    AND dirouteid = '5'
            ) LIST
        LEFT JOIN {{tablename}} LOG ON LOG.DIORDERID = LIST.DIORDERID
            AND LOG.DIORDERHISTID = LIST.DIORDERHISTID
        WHERE LIST.DIORDERID IS NULL
            AND LIST.DIORDERHISTID IS NULL;

        UPDATE {{ref('jpndclitg_integration__c_tbecorderhistory_1')}}
        SET C_DSUKETSUKETELCOMPANYCD = 'WEB',
            updated_date = GETDATE(),
            updated_by = 'ETL_Batch'
        WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
            AND dirouteid = '5';

        UPDATE {{tablename}}
        SET T_KBN = 'F',
            updated_date = GETDATE(),
            updated_by = 'ETL_Batch'
        FROM (
            SELECT DIORDERHISTID,
                    DIORDERID
            FROM {{ref('jpndclitg_integration__c_tbecorderhistory_1')}} C_TBECORDERHISTORY
            WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                    AND dirouteid = '3'
            ) LIST
        WHERE {{tablename}}.DIORDERID = LIST.DIORDERID
            AND {{tablename}}.DIORDERHISTID = LIST.DIORDERHISTID;

        INSERT INTO {{tablename}} (
            DIORDERHISTID,
            DIORDERID,
            T_KBN
            )
        SELECT LIST.DIORDERHISTID,
            LIST.DIORDERID,
            'F'
        FROM (
            SELECT DIORDERHISTID,
                    DIORDERID
            FROM {{ref('jpndclitg_integration__c_tbecorderhistory_1')}} C_TBECORDERHISTORY
            WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                    AND dirouteid = '3'
            ) LIST
        LEFT JOIN {{tablename}} LOG ON LOG.DIORDERID = LIST.DIORDERID
            AND LOG.DIORDERHISTID = LIST.DIORDERHISTID
        WHERE LIST.DIORDERID IS NULL
            AND LIST.DIORDERHISTID IS NULL;

        UPDATE {{ref('jpndclitg_integration__c_tbecorderhistory_1')}}
        SET C_DSUKETSUKETELCOMPANYCD = 'FAX',
            updated_date = GETDATE(),
            updated_by = 'ETL_Batch'
        WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
            AND dirouteid = '3';

        UPDATE {{tablename}}
        SET T_KBN = 'M',
            updated_date = GETDATE(),
            updated_by = 'ETL_Batch'
        FROM (
            SELECT DIORDERHISTID,
                    DIORDERID
            FROM {{ref('jpndclitg_integration__c_tbecorderhistory_1')}} C_TBECORDERHISTORY
            WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                    AND dirouteid = '4'
            ) LIST
        WHERE {{tablename}}.DIORDERID = LIST.DIORDERID
            AND {{tablename}}.DIORDERHISTID = LIST.DIORDERHISTID;

        INSERT INTO {{tablename}} (
            DIORDERHISTID,
            DIORDERID,
            T_KBN
            )
        SELECT LIST.DIORDERHISTID,
            LIST.DIORDERID,
            'M'
        FROM (
            SELECT DIORDERHISTID,
                    DIORDERID
            FROM {{ref('jpndclitg_integration__c_tbecorderhistory_1')}} C_TBECORDERHISTORY
            WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                    AND dirouteid = '4'
            ) LIST
        LEFT JOIN {{tablename}} LOG ON LOG.DIORDERID = LIST.DIORDERID
            AND LOG.DIORDERHISTID = LIST.DIORDERHISTID
        WHERE LIST.DIORDERID IS NULL
            AND LIST.DIORDERHISTID IS NULL;

        UPDATE {{ref('jpndclitg_integration__c_tbecorderhistory_1')}}
        SET C_DSUKETSUKETELCOMPANYCD = 'MAL',
            updated_date = GETDATE(),
            updated_by = 'ETL_Batch'
        WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
            AND dirouteid = '4';

        UPDATE {{tablename}}
        SET T_KBN = 'S',
            updated_date = GETDATE(),
            updated_by = 'ETL_Batch'
        FROM (
            SELECT DIORDERHISTID,
                    DIORDERID
            FROM {{ref('jpndclitg_integration__c_tbecorderhistory_1')}} C_TBECORDERHISTORY
            WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                    AND dirouteid = '6'
            ) LIST
        WHERE {{tablename}}.DIORDERID = LIST.DIORDERID
            AND {{tablename}}.DIORDERHISTID = LIST.DIORDERHISTID;

                               
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}