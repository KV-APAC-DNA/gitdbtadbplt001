{% if build_month_end_job_models()  %}
WITH KR_054_CAL_V
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kr_054_cal_v')}} 
    ),
KR_054_SLYOTEI_MEISAI
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kr_054_slyotei_meisai')}}
    ),
T1
AS (
    SELECT '103_失効予定P'::VARCHAR(30) AS YOTEI_LABEL,
        MEI.YOTEI_YM::VARCHAR(9) AS YOTEI_YM,
        MEI.YOTEI_YY::VARCHAR(6) AS YOTEI_YY,
        MEI.YOTEI_MM::VARCHAR(3) AS YOTEI_MM,
        V.YYMM::VARCHAR(9) AS POINT_YM,
        SUBSTRING(V.YYMM, 1, 4)::VARCHAR(6) AS POINT_YY,
        SUBSTRING(V.YYMM, 5, 2)::VARCHAR(3) AS POINT_MM,
        SUM(NVL(MEI.YPOINT, 0))::NUMBER(38,0) AS YPOINT
    FROM KR_054_CAL_V V
    LEFT JOIN KR_054_SLYOTEI_MEISAI MEI ON V.YYMM = MEI.POINT_YM
    GROUP BY YOTEI_LABEL,
        YOTEI_YM,
        YOTEI_YY,
        YOTEI_MM,
        V.YYMM,
        SUBSTRING(V.YYMM, 1, 4),
        SUBSTRING(V.YYMM, 5, 2)
    ),
FINAL
AS (
    SELECT *
    FROM T1
    ORDER BY 1,
        5,
        2
    )
SELECT *
FROM FINAL
{% else %}
    select * from {{this}}
{% endif %}