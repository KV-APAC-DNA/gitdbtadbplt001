WITH c_tbeckesaihistory
AS (
  SELECT *
  FROM dev_dna_core.jpdclitg_integration.c_tbeckesaihistory
  ),
c_tbecorderhistory
AS (
  SELECT *
  FROM dev_dna_core.jpdclitg_integration.c_tbecorderhistory
  ),
c_tbecusercard
AS (
  SELECT *
  FROM dev_dna_core.jpdclitg_integration.c_tbecusercard
  ),
tbpromotion
AS (
  SELECT *
  FROM dev_dna_core.jpdclitg_integration.tbpromotion
  ),
hanyo_attr
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.hanyo_attr
  ),
tbecorderhist_qv
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.tbecorderhist_qv
  ),
c_tbeckesai
AS (
  SELECT *
  FROM dev_dna_core.jpdclitg_integration.c_tbeckesai
  ),
tbecorder
AS (
  SELECT *
  FROM dev_dna_core.jpdclitg_integration.tbecorder
  ),
keirokbn
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.keirokbn
  ),
transformed
AS (
  SELECT
        cast(('U' || c_tbEcKesai.c_dikesaiid) as varchar)	AS	SALENO
        ,tbEcOrder.c_dsorderkbn	AS	JUCHKBN
        ,cast((TO_CHAR(tbEcOrder.dsOrderDt, 'YYYYMM')) as numeric)	AS	JUCHYM
        ,cast((TO_CHAR(tbEcOrder.dsOrderDt, 'YYYYMMDD')) as numeric)	AS	JUCHDATE
        ,NVL(LPAD(tbEcOrder.diEcUsrID,10,'0'),'0000000000')	AS	KOKYANO
        ,NVL(HANYO_ATTR.ATTR2,'その他')						AS	HANROCODE
        --2018/3/30 ADD 小原 変更0261 START
        ,CASE WHEN tbEcOrder.C_DSUKETSUKETELCOMPANYCD = 'WEB'
        AND  WEBKEIRO.KEIRO             IS NOT NULL THEN
            NVL(WEBKEIRO.ATTR3,'不明')
        ELSE NVL(HANYO_ATTR.ATTR3,'不明')
        END                                                AS  SYOHANROBUNNAME
        ,CASE WHEN tbEcOrder.C_DSUKETSUKETELCOMPANYCD = 'WEB'
        AND  WEBKEIRO.KEIRO             IS NOT NULL THEN
            NVL(WEBKEIRO.ATTR4,'不明')
        ELSE NVL(HANYO_ATTR.ATTR4,'不明')
        END                                                AS  CHUHANROBUNNAME
        ,CASE WHEN tbEcOrder.C_DSUKETSUKETELCOMPANYCD = 'WEB'
        AND  WEBKEIRO.KEIRO             IS NOT NULL THEN
            NVL(WEBKEIRO.ATTR5,'不明')
        ELSE NVL(HANYO_ATTR.ATTR5,'不明')
        END                                                AS  DAIHANROBUNNAME
        --2018/3/30 ADD 小原 変更0261 END
        ,NVL(tbPromotion.dspromcode,'00000')					AS	MEDIACODE
        ,c_tbEcKesai.dskessaihoho	AS	KESSAIKBN
        ,NVL(c_tbEcKesai.diHaisoPrc,'0')					AS	SORYO
        ,ceil(NVL(c_tbEcKesai.diseikyuprc,'0')*(tbEcOrder.diTaxRate/(100 + tbEcOrder.diTaxRate))	)	AS	TAX
        ,NVL(c_tbEcKesai.diseikyuprc,'0')				AS	SOGOKEI
        ,NVL(cast((c_tbEcUserCard.c_dsCardCompanyId) as varchar),'0000000')	AS	CARDCORPCODE
        ,'000'	AS	HENREASONCODE
        ,c_tbEcKesai.diCancel	AS	CANCELFLG
        ,NVL(cast((TO_CHAR(c_tbEcKesai.dsprep,'YYYYMMDD')) as numeric),'0')	AS	INSERTDATE
        ,NVL(cast((TO_CHAR(c_tbEcKesai.dsprep,'HH24MISS')) as numeric),'0')	AS	INSERTTIME
        ,NVL(cast((tbEcKesai.diprepusr) as varchar),'000000')		AS	INSERTID
        ,NVL(cast((TO_CHAR(c_tbEcKesai.dsren,'YYYYMMDD')) as numeric),'0')		AS	UPDATEDATE
        ,NVL(cast((TO_CHAR(c_tbEcKesai.dsren,'HH24MISS')) as numeric),'0')	AS	UPDATETIME
        ,NVL(c_tbEcKesai.dsTodokeZip,' ')				AS	ZIPCODE
        ,NVL(c_tbEcKesai.dsTodokePref,' ')				AS	TODOFUKENCODE
        ,NVL((c_tbEcKesai.C_DIKAKUTOKUYOTEIPOINT),'0')	AS	HAPPENPOINT
        ,-1 * NVL(c_tbEcKesai.diUsePoint,'0')			AS	RIYOPOINT
        ,NVL(tbEcKesai.diShukkaSts,'0')					AS	SHUKKASTS --決済情報
        ,DECODE(tbEcOrder.dirouteid, 7, '03', 9, '04', 8, '05', '01')		AS	TORIKEIKBN
        ,NVL(tbEcOrder.c_dstempocode,'00000')					AS	TENPOCODE
        ,NVL(cast((TO_CHAR(tbEcKesai.dsUriageDt, 'YYYYMM')) as numeric),0)		AS	SHUKAYM --決済情報
        ,NVL(cast((TO_CHAR(tbEcKesai.dsUriageDt, 'YYYYMMDD')) as numeric),0)		AS	SHUKADATE --決済情報
        ,NVL(tbEcOrder.c_diClassID,0)						AS	RANK
        ,cast((c_tbEcKesai.c_dikesaiid)	as varchar)			AS	DISPSALENO
        ,c_tbEcKesai.c_dikesaiid	AS	KESAIID
        ,tbEcOrder.DIORDERCODE	AS	ORDERCODE
        ,' '	AS	HENREASONNAME
        ,NVL(tbEcOrder.c_diuketsukeusrid,0)					AS	UKETSUKEUSRID
        ,NVL(tbEcOrder_now.C_DSUKETSUKETELCOMPANYCD,'000')				AS	UKETSUKETELCOMPANYCD
        ,NVL(tbEcOrder.dirouteid,0)						AS	SMKEIROID
        ,tbEcOrder.diPromId		AS DIPROMID
        ,NVL(C_TBECKESAI.c_dicollectPrc,0)						AS 	DICOLLECTPRC --代引き手数料
        ,NVL(C_TBECKESAI.c_ditoujitsuhaisoPrc,0)		AS DITOUJITSUHAISOPRC	 --当日配送手数料
        ,NVL(C_TBECKESAI.c_diDiscountAll,0)			AS DIDISCOUNTALL 	 --値引き
        ,-1 * cast((NVL(C_TBECKESAI.C_DIEXCHANGEPOINT,0)) as numeric)	AS	POINT_Exchange --交換商品ポイント
        --2017/11/27 ADD 柳本 変更0195 START
        ,tbEcOrder_now.C_DILASTUPDUSRID AS LASTUPDUSRID
        --2017/11/27 ADD 柳本 変更0195 END
        ,null as KESAIROWID
        ,null as ORDERROWID
        ,null as USERCARDROWID
        ,null as PROMOROWID
        ,null as HANYOROWID
        --BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
        --DnA側でデータマートを作成するため廃止
        --,TO_NUMBER(GREATEST(
        --	 TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.c_tbEcKesai.DSREN,CI_NEXT.c_tbEcKesai.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.tbEcOrder.DSREN,CI_NEXT.tbEcOrder.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.c_tbEcUserCard.DSREN,CI_NEXT.c_tbEcUserCard.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(TO_DATE(NVL(CI_NEXT.tbPromotion.DSREN,CI_NEXT.tbPromotion.DSPREP),'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(HANYO_ATTR.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(NVL(TBECORDERHIST_QV.DSREN,TBECORDERHIST_QV.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.tbEcKesai.DSREN,CI_NEXT.tbEcKesai.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.tbEcOrder_now.DSREN,CI_NEXT.tbEcOrder_now.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,NVL(WEBKEIRO.JOIN_REC_UPDDATE,0)
        --)) AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
        --END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
  FROM c_tbeckesaihistory c_tbEcKesai --決済情報履歴
        , c_tbecorderhistory tbEcOrder --受注情報履歴
        , c_tbecusercard c_tbEcUserCard --クレジットカード情報(顧客）
        , tbpromotion tbPromotion --プロモーションマスタ
        , hanyo_attr HANYO_ATTR --汎用属性（通信経路）
        , tbecorderhist_qv TBECORDERHIST_QV --受注変更履歴QV
        , c_tbeckesai tbEcKesai --決済情報
        , tbecorder tbEcOrder_now --受注情報
        --2018/3/30 ADD 小原 変更0261 START
        ,(
        SELECT KEIROKBN.DIORDERID
                ,KEIROKBN.KEIRO
                ,HANYO_WEB.ATTR1
                ,HANYO_WEB.ATTR3
                ,HANYO_WEB.ATTR4
                ,HANYO_WEB.ATTR5
        --BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
        --DnA側でデータマートを作成するため廃止
        --      ,TO_NUMBER(GREATEST(
        --             TO_NUMBER(NVL(TO_CHAR(CI_DWH_MAIN.KEIROKBN.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
        --           , TO_NUMBER(NVL(TO_CHAR(HANYO_WEB.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
        --       )) AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
        --END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
        FROM   keirokbn
        INNER JOIN hanyo_attr HANYO_WEB
        ON HANYO_WEB.ATTR1     = 'WEB' || '-' || KEIROKBN.KEIRO
        AND HANYO_WEB.KBNMEI = 'TUUSHINKEIRO'
        )WEBKEIRO
        --2018/3/30 ADD 小原 変更0261 END
  WHERE c_tbEcKesai.diorderid = tbEcOrder.diOrderID --決済情報履歴．受注内部ID = 受注情報履歴．受注内部ID
        AND c_tbEcKesai.DIORDERHISTID = tbEcOrder.DIORDERHISTID --決済情報履歴．履歴ID = 受注情報履歴．履歴ID
        AND tbEcOrder.diOrderID = tbEcOrder_now.diOrderID --受注情報履歴．受注内部ID = 受注情報．受注内部ID
        AND c_tbEcKesai.c_dicardid = c_tbEcUserCard.c_dicardid(+) --決済情報履歴．クレジットカード内部ID = クレジットカード情報(顧客）．クレジットカード内部ID
        AND tbEcOrder.diPromId = tbPromotion.dipromid(+) --受注情報履歴．プロモーションID = プロモーションマスタ．プロモーションID
        AND HANYO_ATTR.ATTR1(+) = tbEcOrder.C_DSUKETSUKETELCOMPANYCD --汎用属性（通信経路）.属性1（テレマ会社コード） = 受注情報履歴．受付担当者テレマ会社コード
        AND HANYO_ATTR.KBNMEI(+) = 'TUUSHINKEIRO'
        AND TBECORDERHIST_QV.DIORDERID = tbEcOrder.DIORDERID --受注変更履歴.受注内部ID = 受注情報履歴.受注内部ID
        AND TBECORDERHIST_QV.DIORDERHISTID = tbEcOrder.DIORDERHISTID --受注変更履歴.履歴ID = 受注情報履歴.履歴ID
        AND c_tbEcKesai.diCancel = '0' --決済情報履歴.キャンセルフラグ = '0'(0:有効)
        AND c_tbEcKesai.DIELIMFLG = '0' --決済情報履歴.論理削除フラグ = '0'(0:有効)
        AND tbEcOrder.DIELIMFLG = '0' --受注情報履歴.論理削除フラグ = '0'(0:有効)
        and c_tbEcKesai.C_DIKESAIID = tbEcKesai.C_DIKESAIID --決済情報履歴.決済ID = 決済情報.決済ID
        AND tbEcKesai.diCancel = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
        AND tbEcKesai.DIELIMFLG = '0' --決済情報.論理削除フラグ = '0'(0:有効)
        AND tbEcOrder_now.DIELIMFLG = '0' --受注情報.論理削除フラグ = '0'(0:有効)
        --通信経路区分と結合
        AND tbEcOrder_now.DIORDERID = WEBKEIRO.DIORDERID(+)
  ),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        juchkbn::varchar(1) as juchkbn,
        juchym::number(18,0) as juchym,
        juchdate::number(18,0) as juchdate,
        kokyano::varchar(30) as kokyano,
        hanrocode::varchar(60) as hanrocode,
        syohanrobunname::varchar(60) as syohanrobunname,
        chuhanrobunname::varchar(60) as chuhanrobunname,
        daihanrobunname::varchar(60) as daihanrobunname,
        mediacode::varchar(8) as mediacode,
        kessaikbn::varchar(3) as kessaikbn,
        soryo::number(18,0) as soryo,
        tax::number(18,0) as tax,
        sogokei::number(18,0) as sogokei,
        cardcorpcode::varchar(60) as cardcorpcode,
        henreasoncode::varchar(5) as henreasoncode,
        cancelflg::varchar(15) as cancelflg,
        insertdate::number(18,0) as insertdate,
        inserttime::number(18,0) as inserttime,
        insertid::varchar(60) as insertid,
        updatedate::number(18,0) as updatedate,
        updatetime::number(18,0) as updatetime,
        zipcode::varchar(15) as zipcode,
        todofukencode::varchar(15) as todofukencode,
        happenpoint::number(18,0) as happenpoint,
        riyopoint::number(18,0) as riyopoint,
        shukkasts::varchar(6) as shukkasts,
        torikeikbn::varchar(3) as torikeikbn,
        tenpocode::varchar(8) as tenpocode,
        shukaym::number(18,0) as shukaym,
        shukadate::number(18,0) as shukadate,
        rank::number(18,0) as rank,
        dispsaleno::varchar(60) as dispsaleno,
        kesaiid::number(10,0) as kesaiid,
        ordercode::varchar(18) as ordercode,
        henreasonname::varchar(2) as henreasonname,
        uketsukeusrid::number(18,0) as uketsukeusrid,
        uketsuketelcompanycd::varchar(5) as uketsuketelcompanycd,
        smkeiroid::number(18,0) as smkeiroid,
        dipromid::number(10,0) as dipromid,
        dicollectprc::number(18,0) as dicollectprc,
        ditoujitsuhaisoprc::number(18,0) as ditoujitsuhaisoprc,
        didiscountall::number(18,0) as didiscountall,
        point_exchange::number(18,0) as point_exchange,
        lastupdusrid::number(10,0) as lastupdusrid,
        kesairowid::varchar(1) as kesairowid,
        orderrowid::varchar(1) as orderrowid,
        usercardrowid::varchar(1) as usercardrowid,
        promorowid::varchar(1) as promorowid,
        hanyorowid::varchar(1) as hanyorowid
  FROM transformed
)
SELECT *
FROM final