{{
    config(
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "{{build_procedure_tbecorder_log()}}",
        post_hook =                  
                    "UPDATE {{ref('jpndclitg_integration__tbecorder_1')}}
                    SET C_DSUKETSUKETELCOMPANYCD = 'SHN',
                        updated_date = GETDATE(),
                        updated_by = 'ETL_Batch'
                    WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                        AND dirouteid = '6';"
                    )
}}
--ADD POST HOOK TO UPDATE TBECORDER
with
TBECORDER as
(
    select * from {{ref('jpndclitg_integration__tbecorder_1')}}
),
tbecorder_log_tmp as
(
    select * from {{source('jpdcledw_integration','tbecorder_log_tmp')}}
),
final as
(
    SELECT LIST.DIORDERID,
      'S' AS T_KBN,
      NULL AS INSERTED_DATE,
      NULL AS INSERTED_BY,
      NULL AS UPDATED_DATE,
      NULL AS UPDATED_BY
    FROM (
        SELECT DIORDERID
        FROM TBECORDER
        WHERE C_DSUKETSUKETELCOMPANYCD = 'DCL'
                AND dirouteid = '6'
        ) LIST
    LEFT JOIN tbecorder_log_tmp LOG ON LOG.DIORDERID = LIST.DIORDERID
    WHERE LIST.DIORDERID IS NULL
)
select * from final
