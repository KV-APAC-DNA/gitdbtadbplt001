WITH
WK_BIRTHDAY_CINEXTUSRP_JOB AS
(
    SELECT * FROM {{ ref('jpndcledw_integration__wk_birthday_cinextusrp_job') }}
),
WK_BIRTHDAY_CINEXTBIO_JOB AS
(
    SELECT * FROM  {{ ref('jpndcledw_integration__wk_birthday_cinextbio_job') }}
),
WK_BIRTHDAY_CINEXTSTORE_JOB AS
(
    SELECT * FROM {{ ref('jpndcledw_integration__wk_birthday_cinextstore_job') }}
),
WK_BIRTHDAY_VIEWCOLUMN_JOB AS
(
    SELECT * FROM {{ ref('jpndcledw_integration__wk_birthday_viewcolumn_job') }}
),
T1 AS
(
        SELECT "会員基本情報".diusrid AS diusrid,
           CASE
               -- diOrderCntで大小判定が出来るということは、180日以内に"通販"・"店舗"で買い物をしているということ
               WHEN NVL("通販".diOrderCnt,0) > NVL("店舗".diOrderCnt,0) AND "会員基本情報".dsdat60 = '希望' THEN '"通販"'
               WHEN NVL("通販".diOrderCnt,0) > NVL("店舗".diOrderCnt,0) AND "会員基本情報".dsdat60 = '拒否' AND "会員基本情報".dsdat61 = '拒否' THEN '"通販"'
               WHEN NVL("通販".diOrderCnt,0) > NVL("店舗".diOrderCnt,0) AND "会員基本情報".dsdat60 = '拒否' AND "会員基本情報".dsdat61 = '希望' THEN '"店舗"'
               WHEN NVL("通販".diOrderCnt,0) < NVL("店舗".diOrderCnt,0) AND "会員基本情報".dsdat61 = '希望' THEN '"店舗"'
               WHEN NVL("通販".diOrderCnt,0) < NVL("店舗".diOrderCnt,0) AND "会員基本情報".dsdat61 = '拒否' AND "会員基本情報".dsdat60 = '拒否' THEN '"店舗"'
               WHEN NVL("通販".diOrderCnt,0) < NVL("店舗".diOrderCnt,0) AND "会員基本情報".dsdat61 = '拒否' AND "会員基本情報".dsdat60 = '希望' THEN '"通販"'
               -- "通販".diOrderCnt <> 0 で"通販".diOrderCnt="店舗".diOrderCntということは180日以内にどちらでも買い物をしているということ
               WHEN NVL("通販".diOrderCnt,0) <> 0 AND NVL("通販".diOrderCnt,0) = NVL("店舗".diOrderCnt,0) AND "通販".c_dsShukkaDate >= "店舗".c_dsShukkaDate AND "会員基本情報".dsdat60 = '希望' THEN '"通販"'
               WHEN NVL("通販".diOrderCnt,0) <> 0 AND NVL("通販".diOrderCnt,0) = NVL("店舗".diOrderCnt,0) AND "通販".c_dsShukkaDate >= "店舗".c_dsShukkaDate AND "会員基本情報".dsdat60 = '拒否' AND "会員基本情報".dsdat61 = '拒否' THEN '"通販"'
               WHEN NVL("通販".diOrderCnt,0) <> 0 AND NVL("通販".diOrderCnt,0) = NVL("店舗".diOrderCnt,0) AND "通販".c_dsShukkaDate >= "店舗".c_dsShukkaDate AND "会員基本情報".dsdat60 = '拒否' AND "会員基本情報".dsdat61 = '希望' THEN '"店舗"'
               WHEN NVL("通販".diOrderCnt,0) <> 0 AND NVL("通販".diOrderCnt,0) = NVL("店舗".diOrderCnt,0) AND "通販".c_dsShukkaDate <  "店舗".c_dsShukkaDate AND "会員基本情報".dsdat61 = '希望' THEN '"店舗"'
               WHEN NVL("通販".diOrderCnt,0) <> 0 AND NVL("通販".diOrderCnt,0) = NVL("店舗".diOrderCnt,0) AND "通販".c_dsShukkaDate <  "店舗".c_dsShukkaDate AND "会員基本情報".dsdat61 = '拒否' AND "会員基本情報".dsdat60 = '拒否' THEN '"店舗"'
               WHEN NVL("通販".diOrderCnt,0) <> 0 AND NVL("通販".diOrderCnt,0) = NVL("店舗".diOrderCnt,0) AND "通販".c_dsShukkaDate <  "店舗".c_dsShukkaDate AND "会員基本情報".dsdat61 = '拒否' AND "会員基本情報".dsdat60 = '希望' THEN '"通販"'
               -- "通販".diOrderCnt = 0 で"通販".diOrderCnt="店舗".diOrderCntということはどちらでも買い物をしていないため、3年以内に買い物をした直近の販売経路から"顧客区分"を判断
               WHEN NVL("通販".diOrderCnt,0) = 0 AND
                    NVL("店舗".diOrderCnt,0) = 0 AND
                    "会員基本情報".dsdat60 = '希望' AND
                    "会員基本情報".dsdat61 = '希望' AND
                   (SUBSTRING("会員基本情報".ShukkaDT_RouteID, 10, 1) <> '7' AND
                    SUBSTRING("会員基本情報".ShukkaDT_RouteID, 10, 1) <> '8' AND
                    SUBSTRING("会員基本情報".ShukkaDT_RouteID, 10, 1) <> '9'
                   ) THEN '"通販"'
               WHEN NVL("通販".diOrderCnt,0) = 0 AND
                    NVL("店舗".diOrderCnt,0) = 0 AND
                    "会員基本情報".dsdat60 = '希望' AND
                    "会員基本情報".dsdat61 = '希望' AND
                   (SUBSTRING("会員基本情報".ShukkaDT_RouteID, 10, 1) = '7' OR
                    SUBSTRING("会員基本情報".ShukkaDT_RouteID, 10, 1) = '8' OR
                    SUBSTRING("会員基本情報".ShukkaDT_RouteID, 10, 1) = '9'
                   ) THEN '"店舗"'
               -- 3年以内に買い物をしており、一方が拒否の場合、他方は希望のとき、他方に"顧客区分"をセット。
               WHEN NVL("通販".diOrderCnt,0) = 0 AND
                    NVL("店舗".diOrderCnt,0) = 0 AND
                    "会員基本情報".dsdat60 = '希望' AND
                    "会員基本情報".dsdat61 = '拒否'
                    THEN '"通販"'
               WHEN NVL("通販".diOrderCnt,0) = 0 AND
                    NVL("店舗".diOrderCnt,0) = 0 AND
                    "会員基本情報".dsdat60 = '拒否' AND
                    "会員基本情報".dsdat61 = '希望'
                    THEN '"店舗"'
               -- 両方とも拒否の場合は"通販"とみなす
               WHEN NVL("通販".diOrderCnt,0) = 0 AND
                    NVL("店舗".diOrderCnt,0) = 0 AND
                    "会員基本情報".dsdat60 = '拒否' AND
                    "会員基本情報".dsdat61 = '拒否'
                    THEN '"通販"'
               -- 想定外の場合空白をセット
               ELSE ''
            END HANRO,
           CASE
               -- "通販".c_dsShukkaDateが'00000000'ではないということは180日以内に"通販"で買い物したc_dsShukkaDateをセット
               WHEN NVL("通販".c_dsShukkaDate,'00000000') <> '00000000' AND NVL("通販".c_dsShukkaDate,'00000000') >= NVL("店舗".c_dsShukkaDate,'00000000') THEN "通販".c_dsShukkaDate
               -- "店舗".c_dsShukkaDateが'00000000'ではないということは180日以内に"店舗"で買い物したc_dsShukkaDateをセット
               WHEN NVL("店舗".c_dsShukkaDate,'00000000') <> '00000000' THEN "店舗".c_dsShukkaDate
               -- 上記以外は3年間にどちらかで買い物をしたc_dsShukkaDateをセットする
                ELSE SUBSTRING("会員基本情報".ShukkaDT_RouteID, 1, 8)
            END SHUKADATE
      FROM WK_BIRTHDAY_CINEXTUSRP_JOB "会員基本情報"
      LEFT OUTER JOIN WK_BIRTHDAY_CINEXTBIO_JOB "通販"
        ON "会員基本情報".diusrid = "通販".diusrid
      LEFT OUTER JOIN WK_BIRTHDAY_CINEXTSTORE_JOB "店舗"
        ON "会員基本情報".diusrid = "店舗".diusrid
     WHERE "会員基本情報".disecessionflg = '0'
       AND "会員基本情報".dielimflg = '0'
),
WK AS
(
        SELECT diUsrid AS DIUSRID
            ,dsUsrKbn AS HANRO
            ,dsShukkaDate AS SHUKADATE
        FROM WK_BIRTHDAY_VIEWCOLUMN_JOB
),
T2
AS
(
    SELECT DIUSRID
        ,SUBSTRING(MAX(SHUKADATE||'_'||HANRO),REGEXP_INSTR(MAX(SHUKADATE||'_'||HANRO), '_')+1, 
        LENGTH(MAX(SHUKADATE||'_'||HANRO))) AS HANRO
        ,MAX(SHUKADATE) AS SHUKADATE FROM
    (
        SELECT * FROM T1
        UNION
        SELECT * FROM WK
    )
    GROUP BY DIUSRID
),
FINAL AS
(
    SELECT DIUSRID::NUMBER(10,0) AS DIUSRID,
        HANRO::VARCHAR(15) AS HANRO,
        SHUKADATE::VARCHAR(12) AS SHUKADATE
    FROM T2
)
SELECT * FROM FINAL