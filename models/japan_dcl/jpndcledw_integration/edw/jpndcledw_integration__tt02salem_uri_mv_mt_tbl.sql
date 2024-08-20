WITH c_tbecordermeisaihistory
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__c_tbecordermeisaihistory') }}
  ),
c_tbeckesaihistory
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__c_tbeckesaihistory') }}
  ),
tbecorder
AS (
  SELECT *
  FROM  {{ ref('jpndclitg_integration__tbecorder') }}
  ),
tbecorderhist_qv
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__tbecorderhist_qv') }}
  ),
c_tbecorderhistory
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__c_tbecorderhistory') }}
  ),
transformed
AS (
  SELECT
        'U' || CAST((tbEcOrderMeisai.c_dikesaiid) as VARCHAR)		AS	SALENO
        ,NVL(tbEcOrderMeisai.DIMEISAIID,0)				AS	GYONO
        ,'商品'											AS	MEISAIKBN
        ,tbEcOrderMeisai.dsItemID						AS	ITEMCODE
        ,tbEcOrderMeisai.diItemNum						AS	SURYO
        ,decode(tbEcOrderMeisai.diItemNum,0,0,(nvl(tbEcOrderMeisai.c_diitemtotalprc,0) - nvl(tbEcOrderMeisai.c_didiscountmeisai,0)) / nvl(tbEcOrderMeisai.diItemNum,1))	AS	TANKA				--割引後税込単価：(小計(税込）- 割引額) / 数量
        ,decode(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,NVL(tbEcOrderMeisai.c_diitemtotalprc,0) - NVL(tbEcOrderMeisai.c_didiscountmeisai,0))			AS	KINGAKU
        ,decode(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,ceil(((nvl(tbEcOrderMeisai.c_diitemtotalprc,0) - nvl(tbEcOrderMeisai.c_didiscountmeisai,0)) / ((100+tbecorder.DITAXRATE)/100))) )				AS	MEISAINUKIKINGAKU
        ,decode(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,NVL(tbEcOrderMeisai.c_didiscountrate,0))			AS	WARIRITU
        ,NVL(tbEcOrderMeisai.diTotalPrc,0)									AS	WARIMAEKOMITANKA		--割引前税込単価：単価(税込)
        ,decode(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,NVL((tbEcOrderMeisai.c_diitemtotalprc - (tbEcOrderMeisai.diItemTax * nvl(tbEcOrderMeisai.diItemNum,1))),0))					AS	WARIMAENUKIKINGAKU
        ,decode(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,NVL(tbEcOrderMeisai.c_diitemtotalprc,0))				AS	WARIMAEKOMIKINGAKU
        ,CAST((tbEcOrderMeisai.c_dikesaiid) as VARCHAR)	AS	DISPSALENO
        ,tbEcOrderMeisai.c_dikesaiid			AS	KESAIID
        --,tbEcOrderMeisai.rowid					AS	MEISAIROWID
        ,null						AS	KESAIROWID
        ,null						AS	ORDERROWID
        ,tbecorder.DIORDERCODE					AS	DIORDERCODE --受注番号
    --決済情報に存在する受注明細情報のみ
    FROM 
        (
            SELECT
                MEISAI.DIORDERHISTID
                ,MEISAI.DIMEISAIID
                ,MEISAI.DIORDERID
                ,MEISAI.DISETID
                ,MEISAI.DIID
                ,MEISAI.DSITEMID
                ,MEISAI.DITOTALPRC
                ,MEISAI.DIITEMTAX
                ,MEISAI.DIITEMNUM
                ,MEISAI.C_DIITEMTOTALPRC
                ,MEISAI.C_DIDISCOUNTMEISAI
                ,NVL(MEISAI.C_DIKESAIID,KESAI.C_DIKESAIID) AS C_DIKESAIID
                ,MEISAI.C_DSPOINTITEMFLG
                ,MEISAI.DICANCEL
                ,MEISAI.C_DINOSHINSHOITEMPRC
                ,MEISAI.C_DIDISCOUNTRATE
                ,MEISAI.DIELIMFLG
            FROM
                c_tbecordermeisaihistory MEISAI
            LEFT OUTER JOIN
            (
                SELECT
                    DIORDERID
                    ,MIN(c_dikesaiid) AS c_dikesaiid
                FROM
                    c_tbeckesaihistory
                GROUP BY
                    DIORDERID
            ) KESAI
            ON
                MEISAI.DIORDERID = KESAI.DIORDERID
            LEFT OUTER JOIN
                tbecorder HEADER
            ON
                HEADER.DIORDERID = MEISAI.DIORDERID
            JOIN
                tbecorderhist_qv TBECORDERHIST_QV
            ON
                TBECORDERHIST_QV.DIORDERHISTID = MEISAI.DIORDERHISTID --受注変更履歴.履歴ID = 受注明細情報履歴.履歴ID
            AND
                TBECORDERHIST_QV.DIORDERID = MEISAI.DIORDERID --受注変更履歴.受注内部ID = 受注明細情報履歴.受注内部ID
        )tbEcOrderMeisai --受注明細情報
        , c_tbeckesaihistory c_tbEcKesai --決済情報
        , c_tbecorderhistory tbecorder --受注情報
        , tbecorderhist_qv TBECORDERHIST_QV --受注変更履歴QV
    WHERE
        TBECORDERHIST_QV.DIORDERHISTID = tbEcOrder.DIORDERHISTID --受注変更履歴.履歴ID = 受注情報履歴.履歴ID
    AND
        TBECORDERHIST_QV.DIORDERID = tbEcOrder.DIORDERID --受注変更履歴.受注内部ID = 受注情報履歴.受注内部ID
    AND
        tbEcOrderMeisai.DIORDERID = tbecorder.DIORDERID 
    AND
        tbEcOrderMeisai.DIORDERHISTID = tbecorder.DIORDERHISTID 
    AND
        tbEcOrderMeisai.c_dikesaiid = c_tbEcKesai.c_dikesaiid  --受注明細情報．決済ID = 決済情報．決済ID
    AND
        tbEcOrderMeisai.DIORDERHISTID = c_tbEcKesai.DIORDERHISTID  --受注明細情報．決済ID = 決済情報．決済ID
    AND
        c_tbEcKesai.diCancel = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
    AND
        tbEcOrderMeisai.DISETID = tbEcOrderMeisai.diID	--セット品の子供を除く
    AND
        tbEcOrderMeisai.c_dsPointItemFlg = '0'			--ポイント交換対象外
    --削除フラグ対応
    AND
        tbecorder.dielimflg = '0'
    AND
        c_tbEcKesai.dielimflg = '0'
    AND
        tbEcOrderMeisai.dielimflg = '0'
    --ADD BGN 20171006 HIROBE 受注明細 出荷対象外対応
    AND
        tbEcOrderMeisai.dicancel = '0'
    --ADD END 20171006 HIROBE 受注明細 出荷対象外対応COMMENT ON MATERIALIZED VIEW CI_DWH_MAIN.TT02SALEM_URI_MV_MT IS 'shot table for shot CI_DWH_MAIN.TT02SALEM_URI_MV_MT'
),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        gyono::number(18,0) as gyono,
        meisaikbn::varchar(6) as meisaikbn,
        itemcode::varchar(45) as itemcode,
        suryo::number(10,0) as suryo,
        tanka::number(18,0) as tanka,
        kingaku::number(18,0) as kingaku,
        meisainukikingaku::number(18,0) as meisainukikingaku,
        wariritu::number(18,0) as wariritu,
        warimaekomitanka::number(18,0) as warimaekomitanka,
        warimaenukikingaku::number(18,0) as warimaenukikingaku,
        warimaekomikingaku::number(18,0) as warimaekomikingaku,
        dispsaleno::varchar(60) as dispsaleno,
        kesaiid::number(18,0) as kesaiid,
        kesairowid::varchar(1) as kesairowid,
        orderrowid::varchar(1) as orderrowid,
        diordercode::varchar(18) as diordercode
  FROM transformed
)
SELECT *
FROM final