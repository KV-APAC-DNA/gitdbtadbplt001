{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
        pre_hook = ["{% if is_incremental() %}
                    DELETE FROM {{this}} WHERE 1 = 1 AND KOKYANO IN (
                                SELECT C_DICHILDUSRID
                                FROM DEV_DNA_CORE.nnaras01_workspace.jpndcledw_integration__temp_nayose1);
                                {% endif %}",
            
                "{% if is_incremental() %}
                        DELETE FROM DEV_DNA_CORE.nnaras01_workspace.jpndcledw_integration__temp_nayose1 WHERE SEQ IN (
                        SELECT SEQ
                        FROM (
                            SELECT A.SEQ,
                                    A.YM,
                                    A.KOKYANO,
                                    A.RANK,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY A.YM,
                                        A.KOKYANO ORDER BY B.HYOJIJUN
                                        ) AS SELECTED
                            FROM DEV_DNA_CORE.nnaras01_workspace.jpndcledw_integration__temp_nayose1 A
                            INNER JOIN DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.STAGE_MSt B ON A.RANK = B.RANK
                            ) T
                        WHERE 1 = 1
                            AND T.SELECTED <> 1
                    );
                        {% endif %}"
                    ]
    )
}}
with TEMP_NAYOSE as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TEMP_NAYOSE
),
transformed as (
SELECT YM,
      KOKYANO,
      RANK,
      SYSDATE() as insertdate,
      SYSDATE() as updatedate
FROM TEMP_NAYOSE TEMP_NAYOSE
),
final as (
select
ym::varchar(9) as ym,
kokyano::varchar(15) as kokyano,
rank::varchar(150) as rank,
insertdate::timestamp_ntz(9) as insertdate,
updatedate::timestamp_ntz(9) as updatedate,
current_timestamp()::timestamp_ntz(9) as INSERTED_DATE,
null::varchar(100) as inserted_by,
current_timestamp()::timestamp_ntz(9) as UPDATED_DATE,
null::varchar(100) as updated_by
from transformed
)
select * from final
