{{
    config(
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "{{build_procedure_tbecorder_rireki_log()}}",
        post_hook = "
                    UPDATE {{ref('jpndclitg_integration__c_tbecorderhistory_1')}}
                    SET C_DSUKETSUKETELCOMPANYCD = 'SHN',
                        updated_date = GETDATE(),
                        updated_by = 'ETL_Batch'
                    WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                        AND dirouteid = '6';"
               )
}}
with C_TBECORDERHISTORY
as
(
    select * from {{ref('jpndclitg_integration__c_tbecorderhistory_1')}}
),
TBECORDER_RIREKI_LOG as
(
    select * from {{source('jpdcledw_integration','tbecorder_rireki_log_tmp')}}
),
final as
(
    SELECT LIST.DIORDERHISTID::NUMBER(38,0) AS DIORDERHISTID,
        LIST.DIORDERID::NUMBER(38,0) AS DIORDERID,
        'S'::VARCHAR(1) AS T_KBN ,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS INSERTED_DATE,
        NULL::VARCHAR(100) AS INSERTED_BY,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS UPDATED_DATE,
        NULL::VARCHAR(100) AS UPDATED_BY
    FROM (
        SELECT DIORDERHISTID,
                DIORDERID
        FROM C_TBECORDERHISTORY
        WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                AND dirouteid = '6'
        ) LIST
    LEFT JOIN TBECORDER_RIREKI_LOG LOG ON LOG.DIORDERID = LIST.DIORDERID
        AND LOG.DIORDERHISTID = LIST.DIORDERHISTID
    WHERE LIST.DIORDERID IS NULL
        AND LIST.DIORDERHISTID IS NULL
)
select * from final


