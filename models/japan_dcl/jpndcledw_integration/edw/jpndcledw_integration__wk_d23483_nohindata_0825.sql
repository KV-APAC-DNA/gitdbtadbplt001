WITH WK_D22686_POINT_CALC_F_HIST_WK
AS (
    SELECT *
    FROM JPDCLEDW_INTEGRATION.WK_D22686_POINT_CALC_F_HIST_WK
    ),
KR_NEWKAIMA_CALC_HIST
AS (
    SELECT *
    FROM JPDCLEDW_INTEGRATION.KR_NEWKAIMA_CALC_HIST
    ),
tbecordermeisai
AS (
    SELECT *
    FROM JPDCLITG_INTEGRATION.tbecordermeisai
    ),
final
AS (
    SELECT DISTINCT WRK.ORD_DIORDERID::NUMBER(38,0) AS "受注ID",
        WRK.KES_C_DIKESAIID::NUMBER(38,0) AS "決済ID",
        CAST(LTRIM({{encryption_1('CAST(WRK.ORD_DIECUSRID AS VARCHAR)')}}, '0') AS BIGINT)::NUMBER(38,0) AS "顧客ID",
        CAST(LTRIM({{encryption_1('CAST((LPAD(WRK.ORD_DIECUSRID, 10, 0)) AS VARCHAR)')}}, '0') AS BIGINT)::NUMBER(38,0) AS "顧客NO",
        WRK.ORD_C_DSORDERREFERDATE::VARCHAR(16) AS "受注基準日",
        (WRK.KES_DISEIKYUPRC - WRK.KES_DIHAISOPRC - WRK.KES_C_DICOLLECTPRC)::NUMBER(38,0) AS "基準金額"
        --STARTADD_20220425
        ,
        (CASE 
            WHEN B.DIROUTEID IN (1, 2, 3, 4, 5, 6, 10, 11)
                THEN 1
            ELSE NULL
            END)::NUMBER(38,0) AS "Tsu-han",
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
            END)::NUMBER(38,0) AS "Store"
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
