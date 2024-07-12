WITH wk_kpi_06_04
AS (
    SELECT *
    FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.WK_KPI_06_04
    ),
ct1
AS (
    SELECT '通販' AS channel_name,
        'A-15' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."ユニーク契約者数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = 'ＤＴＣ'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '01_有効契約者数'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '01_有効契約者数'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    
    UNION ALL
    
    SELECT '通販' AS channel_name,
        'A-16' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."総契約件数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = 'ＤＴＣ'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '03_新規'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct2
AS (
    SELECT '通販' AS channel_name,
        'A-16-C' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."総契約件数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = 'ＤＴＣ'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '04_解約'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct3
AS (
    SELECT '通販' AS channel_name,
        'A-17' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."総契約件数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = 'ＤＴＣ'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '01_有効契約者数'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '01_有効契約者数'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct4
AS (
    SELECT '通販' AS channel_name,
        'A-18' AS channel_id,
        kpi01.yymm,
        (kpi01."総契約件数" - kpi02."総契約件数") AS "ユニーク契約者数"
    FROM (
        (
            SELECT wk_kpi_06_04.yymm,
                sum(wk_kpi_06_04."総契約件数") AS "総契約件数"
            FROM wk_kpi_06_04
            WHERE (
                    (
                        ((wk_kpi_06_04."販路")::TEXT = 'ＤＴＣ'::TEXT)
                        AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                        )
                    AND ((wk_kpi_06_04."小区分")::TEXT = '03_新規'::TEXT)
                    )
            GROUP BY wk_kpi_06_04.yymm
            ) kpi01 JOIN (
            SELECT wk_kpi_06_04.yymm,
                sum(wk_kpi_06_04."総契約件数") AS "総契約件数"
            FROM wk_kpi_06_04
            WHERE (
                    (
                        ((wk_kpi_06_04."販路")::TEXT = 'ＤＴＣ'::TEXT)
                        AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                        )
                    AND ((wk_kpi_06_04."小区分")::TEXT = '04_解約'::TEXT)
                    )
            GROUP BY wk_kpi_06_04.yymm
            ) kpi02 ON (((kpi01.yymm)::TEXT = (kpi02.yymm)::TEXT))
        )
    ),
ct5
AS (
    SELECT 'DTC' AS channel_name,
        'A-15' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."ユニーク契約者数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = '通販'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '01_有効契約者数'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '01_有効契約者数'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct6
AS (
    SELECT 'DTC' AS channel_name,
        'A-16' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."総契約件数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = '通販'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '03_新規'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct7
AS (
    SELECT 'DTC' AS channel_name,
        'A-16-C' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."総契約件数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = '通販'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '04_解約'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct8
AS (
    SELECT 'DTC' AS channel_name,
        'A-17' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."総契約件数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = '通販'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '01_有効契約者数'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '01_有効契約者数'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct9
AS (
    SELECT 'DTC' AS channel_name,
        'A-18' AS channel_id,
        kpi01.yymm,
        (kpi01."総契約件数" - kpi02."総契約件数") AS "ユニーク契約者数"
    FROM (
        (
            SELECT wk_kpi_06_04.yymm,
                sum(wk_kpi_06_04."総契約件数") AS "総契約件数"
            FROM wk_kpi_06_04
            WHERE (
                    (
                        ((wk_kpi_06_04."販路")::TEXT = '通販'::TEXT)
                        AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                        )
                    AND ((wk_kpi_06_04."小区分")::TEXT = '03_新規'::TEXT)
                    )
            GROUP BY wk_kpi_06_04.yymm
            ) kpi01 JOIN (
            SELECT wk_kpi_06_04.yymm,
                sum(wk_kpi_06_04."総契約件数") AS "総契約件数"
            FROM wk_kpi_06_04
            WHERE (
                    (
                        ((wk_kpi_06_04."販路")::TEXT = '通販'::TEXT)
                        AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                        )
                    AND ((wk_kpi_06_04."小区分")::TEXT = '04_解約'::TEXT)
                    )
            GROUP BY wk_kpi_06_04.yymm
            ) kpi02 ON (((kpi01.yymm)::TEXT = (kpi02.yymm)::TEXT))
        )
    ),
ct10
AS (
    SELECT 'WEB' AS channel_name,
        'A-15' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."ユニーク契約者数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = 'ＷＥＢ'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '01_有効契約者数'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '01_有効契約者数'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct11
AS (
    SELECT 'WEB' AS channel_name,
        'A-16' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."総契約件数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = 'ＷＥＢ'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '03_新規'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct12
AS (
    SELECT 'WEB' AS channel_name,
        'A-16-C' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."総契約件数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = 'ＷＥＢ'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '04_解約'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct13
AS (
    SELECT 'WEB' AS channel_name,
        'A-17' AS channel_id,
        wk_kpi_06_04.yymm,
        sum(wk_kpi_06_04."総契約件数") AS "ユニーク契約者数"
    FROM wk_kpi_06_04
    WHERE (
            (
                ((wk_kpi_06_04."販路")::TEXT = 'ＷＥＢ'::TEXT)
                AND ((wk_kpi_06_04."大区分")::TEXT = '01_有効契約者数'::TEXT)
                )
            AND ((wk_kpi_06_04."小区分")::TEXT = '01_有効契約者数'::TEXT)
            )
    GROUP BY wk_kpi_06_04.yymm
    ),
ct14
AS (
    SELECT 'WEB' AS channel_name,
        'A-18' AS channel_id,
        kpi01.yymm,
        (kpi01."総契約件数" - kpi02."総契約件数") AS "ユニーク契約者数"
    FROM (
        (
            SELECT wk_kpi_06_04.yymm,
                sum(wk_kpi_06_04."総契約件数") AS "総契約件数"
            FROM wk_kpi_06_04
            WHERE (
                    (
                        ((wk_kpi_06_04."販路")::TEXT = 'ＷＥＢ'::TEXT)
                        AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                        )
                    AND ((wk_kpi_06_04."小区分")::TEXT = '03_新規'::TEXT)
                    )
            GROUP BY wk_kpi_06_04.yymm
            ) kpi01 JOIN (
            SELECT wk_kpi_06_04.yymm,
                sum(wk_kpi_06_04."総契約件数") AS "総契約件数"
            FROM wk_kpi_06_04
            WHERE (
                    (
                        ((wk_kpi_06_04."販路")::TEXT = 'ＷＥＢ'::TEXT)
                        AND ((wk_kpi_06_04."大区分")::TEXT = '02_内訳'::TEXT)
                        )
                    AND ((wk_kpi_06_04."小区分")::TEXT = '04_解約'::TEXT)
                    )
            GROUP BY wk_kpi_06_04.yymm
            ) kpi02 ON (((kpi01.yymm)::TEXT = (kpi02.yymm)::TEXT))
        )
    ),
final
AS (
    SELECT *
    FROM ct1
    
    UNION ALL
    
    SELECT *
    FROM ct2
    
    UNION ALL
    
    SELECT *
    FROM ct3
    
    UNION ALL
    
    SELECT *
    FROM ct4
    
    UNION ALL
    
    SELECT *
    FROM ct5
    
    UNION ALL
    
    SELECT *
    FROM ct6
    
    UNION ALL
    
    SELECT *
    FROM ct6
    
    UNION ALL
    
    SELECT *
    FROM ct8
    
    UNION ALL
    
    SELECT *
    FROM ct9
    
    UNION ALL
    
    SELECT *
    FROM ct10
    
    UNION ALL
    
    SELECT *
    FROM ct11
    
    UNION ALL
    
    SELECT *
    FROM ct12
    
    UNION ALL
    
    SELECT *
    FROM ct13
    
    UNION ALL
    
    SELECT *
    FROM ct14
    )
SELECT *
FROM final