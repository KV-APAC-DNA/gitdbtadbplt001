{{
    config(
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = ["{{build_wk_birthday_cincit80_nyuhenkin()}}",
                    "{% if is_incremental() %}
                    UPDATE {{this}} CIT80_NYUHENKIN
                    SET SKY_KIN  = NYUHEN.SKY_KIN,
                        SOUSAI_KSSAI_KIN  = NYUHEN.SOUSAI_KSSAI_KIN,
                        KAIS_KSKM_KIN  = NYUHEN.KAIS_KSKM_KIN,
                        JIKAI_SKY_KURKOSI_KIN  = NYUHEN.JIKAI_SKY_KURKOSI_KIN,
                        MI_KAIS_KIN  = NYUHEN.MI_KAIS_KIN,
                        NYUHENKANFLG  = NYUHEN.NYUHENKANFLG,
                        updated_date = GETDATE(),
                        updated_by = 'ETL_Batch'
                        FROM
                        (SELECT
                            SAIKEN.HASSEI_MT_DEN_NO as SAL_NO,
                            SUM(KAISYU.SKY_KIN) as SKY_KIN,
                            SUM(KAISYU.SOUSAI_KSSAI_KIN) as SOUSAI_KSSAI_KIN,
                            SUM(KAISYU.KAIS_KSKM_KIN) as KAIS_KSKM_KIN,
                            SUM(KAISYU.JIKAI_SKY_KURKOSI_KIN) as JIKAI_SKY_KURKOSI_KIN,
                            SUM(KAISYU.MI_KAIS_KIN) as MI_KAIS_KIN,
                            CASE WHEN SUM(KAISYU.SKY_KIN)= SUM(KAISYU.KAIS_KSKM_KIN)THEN 1 ELSE 0 END as NYUHENKANFLG
                        FROM
                            {{ref('jpndcledw_integration__hanyo_attr')}} KAISHA
                        INNER JOIN
                            {{ source('jpdclitg_integration','aartnar') }} SAIKEN
                        ON
                            SAIKEN.KAISHA_CD = KAISHA.ATTR1
                        INNER JOIN
                            {{ source('jpdclitg_integration','aartbkaisytiar') }} KAISYU
                        ON
                            KAISYU.KAISHA_CD = KAISHA.ATTR1 AND
                            SAIKEN.AR_NAIBU_NO = KAISYU.AR_NAIBU_NO
                        WHERE
                            KAISHA.KBNMEI = 'KAISYA' AND
                            SAIKEN.KOUSHIN_DATE >= (SELECT
                                                        CAST( TO_CHAR( CONVERT_TIMEZONE('Asia/Tokyo',current_timestamp()),'YYYYMMDD') AS NUMERIC) + (select distinct(CAST(ATTR1 AS NUMERIC))
                                                    FROM {{ref('jpndcledw_integration__hanyo_attr')}} HANYO_ATTR
                                                    WHERE HANYO_ATTR.KBNMEI = 'DAILYFROM'))
                                                    AND
                            (SAIKEN.HASSEI_MT_DEN_SHUBT_KBN = '107' OR SAIKEN.TORI_SHUR_CD = 'Z302')
                        GROUP BY
                            SAIKEN.HASSEI_MT_DEN_NO
                        ORDER BY
                            SAIKEN.HASSEI_MT_DEN_NO) NYUHEN
                        WHERE
                            CIT80_NYUHENKIN.SAL_NO = NYUHEN.SAL_NO;
                        {% endif %}"
                        ]
    )
}}

with HANYO_ATTR
as
(
    select * from {{ref('jpndcledw_integration__hanyo_attr')}}
),
AARTNAR as
(
    select * from {{ source('jpdclitg_integration','aartnar') }}
),
AARTBKAISYTIAR as
(
    select * from {{ source('jpdclitg_integration','aartbkaisytiar') }}
),
datefrom as
(
    SELECT
        CAST( TO_CHAR(CONVERT_TIMEZONE('Asia/Tokyo',current_timestamp()),'YYYYMMDD') AS NUMERIC) + (select distinct(CAST(ATTR1 AS NUMERIC))
    FROM HANYO_ATTR
    WHERE HANYO_ATTR.KBNMEI = 'DAILYFROM')
),
NYUHEN as
(    SELECT
        SAIKEN.HASSEI_MT_DEN_NO as SAL_NO,
        SUM(KAISYU.SKY_KIN) as SKY_KIN,
        SUM(KAISYU.SOUSAI_KSSAI_KIN) as SOUSAI_KSSAI_KIN,
        SUM(KAISYU.KAIS_KSKM_KIN) as KAIS_KSKM_KIN,
        SUM(KAISYU.JIKAI_SKY_KURKOSI_KIN) as JIKAI_SKY_KURKOSI_KIN,
        SUM(KAISYU.MI_KAIS_KIN) as MI_KAIS_KIN,
        CASE WHEN SUM(KAISYU.SKY_KIN)= SUM(KAISYU.KAIS_KSKM_KIN)THEN 1 ELSE 0 END as NYUHENKANFLG
    FROM HANYO_ATTR KAISHA
    INNER JOIN AARTNAR SAIKEN
    ON SAIKEN.KAISHA_CD = KAISHA.ATTR1
    INNER JOIN AARTBKAISYTIAR KAISYU
    ON KAISYU.KAISHA_CD = KAISHA.ATTR1 AND
        SAIKEN.AR_NAIBU_NO = KAISYU.AR_NAIBU_NO
    WHERE
        KAISHA.KBNMEI = 'KAISYA' AND
        SAIKEN.KOUSHIN_DATE >= (SELECT * FROM datefrom) AND
        (SAIKEN.HASSEI_MT_DEN_SHUBT_KBN = '107' OR SAIKEN.TORI_SHUR_CD = 'Z302')
    GROUP BY
        SAIKEN.HASSEI_MT_DEN_NO
    ORDER BY
        SAIKEN.HASSEI_MT_DEN_NO

),
final as
(SELECT
        NYUHEN.SAL_NO::VARCHAR(18) as SAL_NO,
        NYUHEN.SKY_KIN::NUMBER(38,0) as SKY_KIN,
        NYUHEN.SOUSAI_KSSAI_KIN::NUMBER(38,0) as SOUSAI_KSSAI_KIN,
        NYUHEN.KAIS_KSKM_KIN::NUMBER(38,0) as KAIS_KSKM_KIN,
        NYUHEN.JIKAI_SKY_KURKOSI_KIN::NUMBER(38,0)  as JIKAI_SKY_KURKOSI_KIN,
        NYUHEN.MI_KAIS_KIN::NUMBER(38,0) as MI_KAIS_KIN,
        NYUHEN.NYUHENKANFLG::NUMBER(38,0) as NYUHENKANFLG,
        NULL::TIMESTAMP_NTZ(9) AS INSERTDATE,
        NULL::TIMESTAMP_NTZ(9) AS UPDATEDATE,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS INSERTED_DATE,
        NULL::VARCHAR(100) AS INSERTED_BY,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS UPDATED_DATE,
        NULL::VARCHAR(100) AS UPDATED_BY,

    FROM NYUHEN
    LEFT JOIN {{this}} CIT80
    ON CIT80.SAL_NO = NYUHEN.SAL_NO
    WHERE CIT80.SAL_NO  IS NULL
)

select * from final