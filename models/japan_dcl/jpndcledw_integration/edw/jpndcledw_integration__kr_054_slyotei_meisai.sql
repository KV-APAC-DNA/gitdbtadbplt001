{% if build_month_end_job_models()  %}
WITH KR_054_ALLADM
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kr_054_alladm')}}
    ),
KR_054_SFUYO_MEISAI
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__kr_054_sfuyo_meisai') }}
    ),
TBUSRPRAM
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__tbusrpram')}}
    ),
FINAL
AS (
    SELECT ADM.DIECUSRID::NUMBER(38,0) AS DIECUSRID,
        SUBSTRING(ADM.C_DSPOINTLIMITDATE, 1, 6)::VARCHAR(9) AS YOTEI_YM,
        SUBSTRING(ADM.C_DSPOINTLIMITDATE, 1, 4)::VARCHAR(6) AS YOTEI_YY,
        SUBSTRING(ADM.C_DSPOINTLIMITDATE, 5, 2)::VARCHAR(3) AS YOTEI_MM,
        WK.POINT_YM::VARCHAR(9) AS POINT_YM,
        WK.POINT_YY::VARCHAR(6) AS POINT_YY,
        WK.POINT_MM::VARCHAR(3) AS POINT_MM,
        ADM.DIREMNANTPOINT::NUMBER(38,0) AS YPOINT,
        ADM.C_DIPOINTISSUEID::NUMBER(38,0) AS YPOINT_ID
    FROM KR_054_ALLADM ADM
    INNER JOIN KR_054_SFUYO_MEISAI WK ON ADM.DIECUSRID = WK.DIECUSRID
        AND ADM.C_DIPOINTISSUEID = WK.POINT_ID
    WHERE 1 = 1
        AND ADM.DIELIMFLG = '0'
        AND ADM.DIVALIDFLG = '1'
        AND ADM.DIPOINTCODE = '1'
        AND ADM.DIREMNANTPOINT <> 0
        AND SUBSTRING(ADM.C_DSPOINTLIMITDATE, 1, 4) >= '2020'
        AND WK.DIECUSRID IN (
            SELECT USR.DIUSRID
            FROM TBUSRPRAM USR
            WHERE 1 = 1
                AND USR.DIELIMFLG = '0'
                AND USR.DISECESSIONFLG = '0'
                AND USR.DSDAT93 = '通常ユーザ'
            )
    )
SELECT *
FROM FINAL
{% else %}
    select * from {{this}}
{% endif %}