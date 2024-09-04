{% if build_month_end_job_models()  %}
WITH kr_054_sfuyo_meisai
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__kr_054_sfuyo_meisai') }}
    ),
kr_054_cal_v
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__kr_054_cal_v') }}
    ),
transformed
AS (
    SELECT '01_付与P' AS COL1,
        V.YYMM AS POINT_YM,
        SUBSTRING(V.YYMM, 1, 4) AS POINT_YY,
        SUBSTRING(V.YYMM, 5, 2) AS POINT_MM,
        SUM(NVL(MEI.POINT, 0)) AS POINT_SUM
    FROM KR_054_CAL_V V
    LEFT JOIN KR_054_SFUYO_MEISAI MEI ON V.YYMM = MEI.POINT_YM
    GROUP BY COL1,
        V.YYMM,
        SUBSTRING(V.YYMM, 1, 4),
        SUBSTRING(V.YYMM, 5, 2)
    ),
final
AS (
    SELECT col1::VARCHAR(30) AS fuyo_label,
        point_ym::VARCHAR(9) AS point_ym,
        point_yy::VARCHAR(6) AS point_yy,
        point_mm::VARCHAR(3) AS point_mm,
        point_sum::number(38, 0) AS point
    FROM transformed
    )
SELECT *
FROM final
{% else %}
    select * from {{this}}
{% endif %}