{% if build_month_end_job_models()  %}
WITH WK_D22686_POINT_CALC_F_HIST_WK
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__wk_d22686_point_calc_f_hist_wk')}} 
    ),
KR_NEWKAIMA_CALC_HIST
AS (
    SELECT *
    FROM {{source('jpdcledw_integration', 'kr_newkaima_calc_hist')}}  
    ),
tbecordermeisai
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__tbecordermeisai')}}
    ),
final
AS (
    SELECT DISTINCT WRK.ORD_DIORDERID::NUMBER(38,0) AS "受注id",
        WRK.KES_C_DIKESAIID::NUMBER(38,0) AS "決済id",
        CAST(LTRIM({{encryption_1('CAST(WRK.ORD_DIECUSRID AS VARCHAR)')}}, '0') AS BIGINT)::NUMBER(38,0) AS "顧客id",
        CAST(LTRIM({{encryption_1('CAST((LPAD(WRK.ORD_DIECUSRID, 10, 0)) AS VARCHAR)')}}, '0') AS BIGINT)::NUMBER(38,0) AS "顧客no",
        WRK.ORD_C_DSORDERREFERDATE::VARCHAR(16) AS "受注基準日",
        (WRK.KES_DISEIKYUPRC - WRK.KES_DIHAISOPRC - WRK.KES_C_DICOLLECTPRC)::NUMBER(38,0) AS "基準金額"
        --STARTADD_20220425
        ,
        (CASE 
            WHEN B.DIROUTEID IN (1, 2, 3, 4, 5, 6, 10, 11)
                THEN 1
            ELSE NULL
            END)::NUMBER(38,0) AS "tsu-han",
        (CASE 
            WHEN B.DIROUTEID IN (5, 6)
                THEN 1
            ELSE NULL
            END)::NUMBER(38,0) AS "WEB",
        (CASE 
            WHEN B.DIROUTEID IN (1, 2, 3, 4, 10, 11)
                THEN 1
            ELSE NULL
            END)::NUMBER(38,0) AS "CALL",
        (CASE 
            WHEN B.DIROUTEID IN (7, 8, 9)
                THEN 1
            ELSE NULL
            END)::NUMBER(38,0) AS "STORE"
    --ENDADD_20220425
    FROM WK_D22686_POINT_CALC_F_HIST_WK WRK
    LEFT JOIN KR_NEWKAIMA_CALC_HIST HIS ON HIS.KES_C_DIKESAIID = WRK.KES_C_DIKESAIID
    --STARTADD_20220425
    LEFT JOIN tbecordermeisai B ON WRK.KES_C_DIKESAIID = B.C_DIKESAIID
    --ENDADD_20220425
    WHERE 1 = 1
        AND WRK.ORD_DICANCEL = 0
        AND WRK.ORD_DIELIMFLG = 0
        AND WRK.KES_DIELIMFLG = 0
        AND WRK.KES_DISHUKKASTS IN ('1040', '1060')
        AND WRK.ORD_DIECUSRID <> '0'
        AND HIS.KES_C_DIKESAIID IS NULL
    )
SELECT *
FROM final
{% else %}
    select * from {{this}}
{% endif %}