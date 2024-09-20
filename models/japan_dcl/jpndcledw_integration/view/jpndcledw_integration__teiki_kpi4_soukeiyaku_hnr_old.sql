WITH c_tbecregularcoursemst
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__c_tbecregularcoursemst') }}
    ),
c_tbecregularmeisai
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__c_tbecregularmeisai') }}
    ),
teikikeiyaku_data_mart_hnr_old
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__teikikeiyaku_data_mart_hnr_old') }}
    ),
tbecordermeisai
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__tbecordermeisai') }}
    ),
tbecitem
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__tbecitem') }}
    ),
union1
AS (
    SELECT '01_有効契約者数' AS "大区分",
        '01_有効契約者数' AS "小区分",
        teiki.c_dsdeleveryym AS "年月",
        teiki.dirouteid AS "販路",
        count(DISTINCT teiki.c_diregularcontractid) AS "総契約件数",
        sum(CASE 
                WHEN ((teiki.c_dsordercreatekbn)::TEXT = (2)::TEXT)
                    THEN keiyaku_sum.keiyaku_sum
                ELSE (0)::DOUBLE PRECISION
                END) AS "総契約金額",
        count(DISTINCT teiki.c_diusrid) AS "ユニーク契約者数"
    FROM (
        teikikeiyaku_data_mart_hnr_old teiki LEFT JOIN (
            SELECT tei.c_dsregularmeisaiid,
                tei.c_dsdeleveryym,
                sum(CASE 
                        WHEN (tei.diitemsalesprc = 0)
                            THEN 1
                        ELSE 1
                        END) AS keiyaku_item_cnt,
                sum(CASE 
                        WHEN (tei.diitemsalesprc = 0)
                            THEN 0
                        ELSE 1
                        END) AS jiseki_item_cnt,
                sum(nvl2(round(((((mei.ditotalprc * mei.diitemnum) / 108) * 100))::DOUBLE PRECISION), round(((((mei.ditotalprc * mei.diitemnum) / 108) * 100))::DOUBLE PRECISION), (0)::DOUBLE PRECISION)) AS jiseki_sum,
                sum(CASE 
                        WHEN ((itm.dsoption046)::TEXT = '利用しない'::TEXT)
                            THEN (
                                    CASE 
                                        WHEN ((mst.c_dsregularcoursenameryaku)::TEXT = '毎月'::TEXT)
                                            THEN round((((itm.diitemsalesprc / 100))::NUMERIC * ((100)::NUMERIC - (itm.dsoption047)::NUMERIC)))
                                        WHEN ((mst.c_dsregularcoursenameryaku)::TEXT = '２ヵ月'::TEXT)
                                            THEN round((((itm.diitemsalesprc / 100))::NUMERIC * ((100)::NUMERIC - (itm.dsoption048)::NUMERIC)))
                                        WHEN ((mst.c_dsregularcoursenameryaku)::TEXT = '３ヵ月'::TEXT)
                                            THEN round((((itm.diitemsalesprc / 100))::NUMERIC * ((100)::NUMERIC - (itm.dsoption049)::NUMERIC)))
                                        ELSE (0)::NUMERIC
                                        END
                                    )::DOUBLE PRECISION
                        WHEN ((itm.dsoption046)::TEXT = '利用する'::TEXT)
                            THEN round((((itm.diitemsalesprc / 100) * (100 - mst.didiscrate)))::DOUBLE PRECISION)
                        ELSE (0)::DOUBLE PRECISION
                        END) AS keiyaku_sum
            FROM (
                (
                    (
                        (
                            teikikeiyaku_data_mart_hnr_old tei LEFT JOIN tbecordermeisai mei ON ((mei.dimeisaiid = tei.dimeisaiid))
                            ) LEFT JOIN c_tbecregularmeisai rcm ON ((rcm.c_dsregularmeisaiid = tei.c_dsregularmeisaiid))
                        ) LEFT JOIN tbecitem itm ON (
                            (
                                (rcm.diid = itm.diid)
                                AND ((itm.dielimflg)::TEXT = (0)::TEXT)
                                )
                            )
                    ) LEFT JOIN c_tbecregularcoursemst mst ON ((mst.c_diregularcourseid = rcm.c_diregularcourseid))
                )
            GROUP BY tei.c_dsregularmeisaiid,
                tei.c_dsdeleveryym
            ) keiyaku_sum ON (
                (
                    (keiyaku_sum.c_dsregularmeisaiid = teiki.c_dsregularmeisaiid)
                    AND ((keiyaku_sum.c_dsdeleveryym)::TEXT = (teiki.c_dsdeleveryym)::TEXT)
                    )
                )
        )
    WHERE (
            ((teiki.c_dsdeleveryym)::TEXT >= (201702)::TEXT)
            AND (
                (
                    (
                        (teiki.contract_kbn = '継続'::TEXT)
                        OR (teiki.contract_kbn = '新規'::TEXT)
                        )
                    OR (teiki.contract_kbn = 'スポット'::TEXT)
                    )
                OR (teiki.contract_kbn = '休止'::TEXT)
                )
            )
    GROUP BY teiki.c_dsdeleveryym,
        teiki.dirouteid
    ),
union2
AS (
    SELECT '02_内訳' AS "大区分",
        (
            CASE 
                WHEN (teiki.contract_kbn = '継続'::TEXT)
                    THEN '01_継続'::TEXT
                WHEN (teiki.contract_kbn = 'スポット'::TEXT)
                    THEN '02_スポット'::TEXT
                WHEN (teiki.contract_kbn = '新規'::TEXT)
                    THEN '03_新規'::TEXT
                WHEN (teiki.contract_kbn = '解約'::TEXT)
                    THEN '04_解約'::TEXT
                WHEN (teiki.contract_kbn = '休止'::TEXT)
                    THEN '05_休止'::TEXT
                WHEN (teiki.contract_kbn = '解約以降'::TEXT)
                    THEN '06_解約以降'::TEXT
                WHEN (teiki.contract_kbn = '初回お届け前'::TEXT)
                    THEN '07_初回お届け前'::TEXT
                ELSE 'その他'::TEXT
                END
            )::CHARACTER VARYING AS "小区分",
        teiki.c_dsdeleveryym AS "年月",
        teiki.dirouteid AS "販路",
        count(DISTINCT teiki.c_diregularcontractid) AS "総契約件数",
        sum(CASE 
                WHEN ((teiki.c_dsordercreatekbn)::TEXT = (2)::TEXT)
                    THEN keiyaku_sum.keiyaku_sum
                ELSE (0)::DOUBLE PRECISION
                END) AS "総契約金額",
        count(DISTINCT teiki.c_diusrid) AS "ユニーク契約者数"
    FROM (
        teikikeiyaku_data_mart_hnr_old teiki LEFT JOIN (
            SELECT tei.c_dsregularmeisaiid,
                tei.c_dsdeleveryym,
                sum(CASE 
                        WHEN (tei.diitemsalesprc = 0)
                            THEN 1
                        ELSE 1
                        END) AS keiyaku_item_cnt,
                sum(CASE 
                        WHEN (tei.diitemsalesprc = 0)
                            THEN 0
                        ELSE 1
                        END) AS jiseki_item_cnt,
                sum(nvl2(round(((((mei.ditotalprc * mei.diitemnum) / 108) * 100))::DOUBLE PRECISION), round(((((mei.ditotalprc * mei.diitemnum) / 108) * 100))::DOUBLE PRECISION), (0)::DOUBLE PRECISION)) AS jiseki_sum,
                sum(CASE 
                        WHEN ((itm.dsoption046)::TEXT = '利用しない'::TEXT)
                            THEN (
                                    CASE 
                                        WHEN ((mst.c_dsregularcoursenameryaku)::TEXT = '毎月'::TEXT)
                                            THEN round((((itm.diitemsalesprc / 100))::NUMERIC * ((100)::NUMERIC - (itm.dsoption047)::NUMERIC)))
                                        WHEN ((mst.c_dsregularcoursenameryaku)::TEXT = '２ヵ月'::TEXT)
                                            THEN round((((itm.diitemsalesprc / 100))::NUMERIC * ((100)::NUMERIC - (itm.dsoption048)::NUMERIC)))
                                        WHEN ((mst.c_dsregularcoursenameryaku)::TEXT = '３ヵ月'::TEXT)
                                            THEN round((((itm.diitemsalesprc / 100))::NUMERIC * ((100)::NUMERIC - (itm.dsoption049)::NUMERIC)))
                                        ELSE (0)::NUMERIC
                                        END
                                    )::DOUBLE PRECISION
                        WHEN ((itm.dsoption046)::TEXT = '利用する'::TEXT)
                            THEN round((((itm.diitemsalesprc / 100) * (100 - mst.didiscrate)))::DOUBLE PRECISION)
                        ELSE (0)::DOUBLE PRECISION
                        END) AS keiyaku_sum
            FROM (
                (
                    (
                        (
                            teikikeiyaku_data_mart_hnr_old tei LEFT JOIN tbecordermeisai mei ON ((mei.dimeisaiid = tei.dimeisaiid))
                            ) LEFT JOIN c_tbecregularmeisai rcm ON ((rcm.c_dsregularmeisaiid = tei.c_dsregularmeisaiid))
                        ) LEFT JOIN tbecitem itm ON (
                            (
                                (rcm.diid = itm.diid)
                                AND ((itm.dielimflg)::TEXT = (0)::TEXT)
                                )
                            )
                    ) LEFT JOIN c_tbecregularcoursemst mst ON ((mst.c_diregularcourseid = rcm.c_diregularcourseid))
                )
            GROUP BY tei.c_dsregularmeisaiid,
                tei.c_dsdeleveryym
            ) keiyaku_sum ON (
                (
                    (keiyaku_sum.c_dsregularmeisaiid = teiki.c_dsregularmeisaiid)
                    AND ((keiyaku_sum.c_dsdeleveryym)::TEXT = (teiki.c_dsdeleveryym)::TEXT)
                    )
                )
        )
    WHERE ((teiki.c_dsdeleveryym)::TEXT >= (201702)::TEXT)
    GROUP BY teiki.c_dsdeleveryym,
        teiki.contract_kbn,
        teiki.dirouteid
    ORDER BY 1,
        2,
        3,
        4
    ),
final
AS (
    SELECT *
    FROM union1
    
    UNION ALL
    
    SELECT *
    FROM union2
    )
SELECT *
FROM final
