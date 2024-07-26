{{
    config(
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "{{build_wk_birthday_viewcolumn_job()}}"
    )
}}

WITH WK_BIRTHDAY_CILINE_JOB
AS
(
    SELECT * FROM SNAPJPDCLEDW_INTEGRATION.WK_BIRTHDAY_CILINE_JOB
),
TBUSRPRAM AS
(
    SELECT * FROM SNAPJPDCLITG_INTEGRATION.TBUSRPRAM
),
FINAL AS
( 
    SELECT WK.diUsrid::NUMBER(10,0) AS diUsrid
            ,(CASE
                -- WK.diOrderCntで大小判定が出来るということは、180日以内に通販・店舗で買い物をしているということ
                WHEN WK.diBuyCnt > WK.diStoreBuyCnt AND UP.dsdat60 = '希望' THEN '通販'
                WHEN WK.diBuyCnt > WK.diStoreBuyCnt AND UP.dsdat60 = '拒否' AND UP.dsdat61 = '拒否' THEN '通販'
                WHEN WK.diBuyCnt > WK.diStoreBuyCnt AND UP.dsdat60 = '拒否' AND UP.dsdat61 = '希望' THEN '店舗'
                WHEN WK.diBuyCnt < WK.diStoreBuyCnt AND UP.dsdat61 = '希望' THEN '店舗'
                WHEN WK.diBuyCnt < WK.diStoreBuyCnt AND UP.dsdat61 = '拒否' AND UP.dsdat60 = '拒否' THEN '店舗'
                WHEN WK.diBuyCnt < WK.diStoreBuyCnt AND UP.dsdat61 = '拒否' AND UP.dsdat60 = '希望' THEN '通販'
                -- diBuyCnt=diStoreBuyCntということはどちらでも買い物をしているということ
                WHEN WK.diBuyCnt = WK.diStoreBuyCnt AND WK.dsLatestDate >= WK.dsStoreLatestDate AND UP.dsdat60 = '希望' THEN '通販'
                WHEN WK.diBuyCnt = WK.diStoreBuyCnt AND WK.dsLatestDate >= WK.dsStoreLatestDate AND UP.dsdat60 = '拒否' AND UP.dsdat61 = '拒否' THEN '通販'
                WHEN WK.diBuyCnt = WK.diStoreBuyCnt AND WK.dsLatestDate >= WK.dsStoreLatestDate AND UP.dsdat60 = '拒否' AND UP.dsdat61 = '希望' THEN '店舗'
                WHEN WK.diBuyCnt = WK.diStoreBuyCnt AND WK.dsLatestDate <  WK.dsStoreLatestDate AND UP.dsdat61 = '希望' THEN '店舗'
                WHEN WK.diBuyCnt = WK.diStoreBuyCnt AND WK.dsLatestDate <  WK.dsStoreLatestDate AND UP.dsdat61 = '拒否' AND UP.dsdat60 = '拒否' THEN '店舗'
                WHEN WK.diBuyCnt = WK.diStoreBuyCnt AND WK.dsLatestDate <  WK.dsStoreLatestDate AND UP.dsdat61 = '拒否' AND UP.dsdat60 = '希望' THEN '通販'
                -- 想定外の場合空白をセット
                ELSE ''
                END)::VARCHAR(6) as dsUsrKbn
            ,(CASE
                WHEN WK.dsLatestDate >= WK.dsStoreLatestDate THEN WK.dsLatestDate
                WHEN WK.dsStoreLatestDate <> '00000000' THEN WK.dsStoreLatestDate
                END)::VARCHAR(36) as dsShukkaDate
            ,''::VARCHAR(2) AS JUDGEKBN
            ,''::VARCHAR(3) AS DIMONTH
        FROM WK_BIRTHDAY_CILINE_JOB WK
        INNER JOIN TBUSRPRAM UP
            ON UP.diUsrid = WK.diUsrid
        WHERE NOT EXISTS (SELECT 'X'
                            FROM {{this}}
                            WHERE DIUSRID = UP.DIUSRID)
)
SELECT * FROM FINAL