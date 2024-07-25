{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = ["{% if is_incremental() %}
                    DELETE FROM {{this}} WHERE 1 = 1 AND KOKYANO IN (
                                SELECT C_DICHILDUSRID
                                FROM EDW_SCHEMA.TEMP_NAYOSE TEMP_NAYOSE);
                                {% endif %}",
            
            "{% if is_incremental() %}
                    DELETE FROM EDW_SCHEMA.TEMP_NAYOSE WHERE SEQ IN (
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
                        FROM EDW_SCHEMA.TEMP_NAYOSE A
                        INNER JOIN EDW_SCHEMA.STAGE_MST B ON A.RANK = B.RANK
                        ) T
                    WHERE 1 = 1
                        AND T.SELECTED <> 1
                );
                    {% endif %}"
                    ]
    )
}}