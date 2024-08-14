WITH c_tbecinquirekesai
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__c_tbecinquirekesai') }}
  ),
c_tbecinquire
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__c_tbecinquire') }}
  ),
c_tbeckesai
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__c_tbeckesai') }}
  ),
tbecorder
AS (
  SELECT *
  FROM  {{ ref('jpndclitg_integration__tbecorder') }}
  ),
c_tbecusercard
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__c_tbecusercard') }}
  ),
tt01_henpin_riyu
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__tt01_henpin_riyu') }}
  ),
tbechenpinriyu
AS (
  SELECT *
  FROM  {{ ref('jpndclitg_integration__tbechenpinriyu') }}
  ),
tbpromotion
AS (
  SELECT *
  FROM  {{ ref('jpndclitg_integration__tbpromotion') }}
  ),
hanyo_attr
AS (
  SELECT *
  FROM  {{ ref('jpndcledw_integration__hanyo_attr') }}
  ),
tt02salem_add1_exp_hen_mt
AS (
  SELECT *
  FROM  {{ ref('jpndcledw_integration__tt02salem_add1_exp_hen_mt') }}
  ),
keirokbn
AS (
  SELECT *
  FROM {{ source('jpdcledw_integration','keirokbn') }}
  ),
transformed
AS (
  SELECT
        CAST(('H' || c_tbEcInquireKesai.c_diinquirekesaiid)	as VARCHAR)		AS	SALENO
        ,DECODE(tbEcOrder.c_dsorderkbn,'0','90','1','91','2','92','90')		AS	JUCHKBN
        ,CAST((TO_CHAR(tbEcOrder.dsOrderDt, 'YYYYMM')) as NUMERIC)		AS	JUCHYM
        ,CAST((TO_CHAR(tbEcOrder.dsOrderDt, 'YYYYMMDD')) as NUMERIC)			AS	JUCHDATE
        ,NVL(LPAD(c_tbEcKesai.diEcUsrID,10,'0'),'0000000000')	AS	KOKYANO
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
        ,c_tbEcKesai.dskessaihoho						AS	KESSAIKBN
        ,(NVL(c_tbEcInquireKesai.dihaisoprc,'0')) *-1						AS	SORYO
        --BGN-MOD 20180423 HIGASHI ***課題203 返品金額の修正***
        ,ceil(NVL(c_tbEcInquireKesai.DIDIFFTOTALPRC,'0')*(c_tbEcInquireKesai.diTaxRate/(100 + c_tbEcInquireKesai.diTaxRate)) *-1	)					AS	TAX
        ,NVL(c_tbEcInquireKesai.DIDIFFTOTALPRC,'0') *-1				AS	SOGOKEI
        --,ceil(NVL(c_tbEcInquire.DIDIFFTOTALPRC,0)*(c_tbEcInquire.diTaxRate/(100 + c_tbEcInquire.diTaxRate)) *-1	)					AS	TAX
        --,NVL(c_tbEcInquire.DIDIFFTOTALPRC,0) *-1				AS	SOGOKEI
        --END-MOD 20180423 HIGASHI ***課題203 返品金額の修正***
        ,NVL(cast((c_tbEcUserCard.c_dsCardCompanyId) as varchar),'0000000')	AS	CARDCORPCODE
        ,NVL(cast((c_tbEcInquireMeisai.dihenpinriyuid) as varchar),'000')			AS	HENREASONCODE
        ,c_tbEcKesai.diCancel							AS	CANCELFLG
        ,NVL(cast((TO_CHAR(c_tbEcInquireKesai.dsPrep,'YYYYMMDD')) as numeric),0)	AS	INSERTDATE
        ,NVL(cast((TO_CHAR(c_tbEcInquireKesai.dsPrep,'HH24MISS')) as numeric),0)	AS	INSERTTIME
        ,NVL(cast((c_tbEcInquireKesai.diPrepUsr) as varchar),'000000')	AS	INSERTID
        ,NVL(cast((TO_CHAR(c_tbEcInquireKesai.dsRen,'YYYYMMDD')) as numeric),0)		AS	UPDATEDATE
        ,NVL(cast((TO_CHAR(c_tbEcInquireKesai.dsRen,'HH24MISS')) as numeric),0)		AS	UPDATETIME
        ,NVL(c_tbEcInquireKesai.dsTodokeZip,' ')				AS	ZIPCODE
        ,NVL(c_tbEcInquireKesai.dsTodokePref,' ')				AS	TODOFUKENCODE
        ,0	AS	HAPPENPOINT
        ,(cast((NVL(c_tbEcInquireKesai.c_diHenkyakuYoteiPoint,0)) as numeric) - NVL(TT02SALEM_ADD1_EXP_HEN_MT.MEISAINUKIKINGAKU,'0')) 			AS	RIYOPOINT
        ,NVL(c_tbEcKesai.diShukkaSts,'0')					AS	SHUKKASTS
        ,DECODE(tbEcOrder.dirouteid, 7, '03', 9, '04', 8, '05', '01')		AS	TORIKEIKBN
        ,NVL(tbEcOrder.c_dstempocode,'00000')					AS	TENPOCODE
        ,NVL(cast((TO_CHAR(c_tbEcKesai.dsUriageDt, 'YYYYMM')) as numeric),'0')		AS	SHUKAYM
        ,NVL(cast((TO_CHAR(c_tbEcKesai.dsUriageDt, 'YYYYMMDD')) as numeric),'0')		AS	SHUKADATE
        ,NVL(tbEcOrder.c_diClassID,'0')						AS	RANK
        ,cast((c_tbEcInquireKesai.c_dikesaiid) as varchar)				AS	DISPSALENO
        ,c_tbEcInquireKesai.c_dikesaiid						AS	KESAIID
        ,tbEcOrder.DIORDERCODE							AS	ORDERCODE
        ,NVL(tbEcHenpinRiyu.dshenpinriyu,' ')					AS	HENREASONNAME
        ,NVL(tbEcOrder.c_diuketsukeusrid,'0')					AS	UKETSUKEUSRID
        ,NVL(tbEcOrder.C_DSUKETSUKETELCOMPANYCD,'000')				AS	UKETSUKETELCOMPANYCD
        ,NVL(tbEcOrder.dirouteid,'0')						AS	SMKEIROID
        ,tbEcOrder.diPromId		AS DIPROMID
        ,0						AS 	DICOLLECTPRC --代引き手数料
        ,0						AS DITOUJITSUHAISOPRC	 --当日配送手数料
        ,0						AS DIDISCOUNTALL 	 --値引き
        ,NVL(TT02SALEM_ADD1_EXP_HEN_MT.MEISAINUKIKINGAKU,'0') 						AS	POINT_Exchange --交換商品返品時返却ポイント
        ,NVL(C_TBECINQUIRE.C_DSHENPINSTS,'0')			AS HENPINSTS 	 --返品ステータス
        --2017/11/27 ADD 柳本 変更0195 START
        ,tbEcOrder.C_DILASTUPDUSRID AS LASTUPDUSRID
        --2017/11/27 ADD 柳本 変更0195 END
        ,null AS INQKESAIROWID
        ,null AS INQROWID
        ,null AS KESAIROWID
        ,null AS ORDERROWID
        ,null AS UCROWID
        ,null AS INQMEISAIROWID
        ,null AS HENRIYUROWID
        ,null AS PROMOROWID
        ,null AS HANYOROWID
        --BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
        --DnA側でデータマートを作成するため廃止
        --,TO_NUMBER(GREATEST(
        --	 TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.c_tbEcInquireKesai.DSREN,CI_NEXT.c_tbEcInquireKesai.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.c_tbEcInquire.DSREN,CI_NEXT.c_tbEcInquire.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.c_tbEcKesai.DSREN,CI_NEXT.c_tbEcKesai.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.tbEcOrder.DSREN,CI_NEXT.tbEcOrder.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.c_tbEcUserCard.DSREN,CI_NEXT.c_tbEcUserCard.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(c_tbEcInquireMeisai.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.tbEcHenpinRiyu.DSREN,CI_NEXT.tbEcHenpinRiyu.DSPREP),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(TO_DATE(NVL(CI_NEXT.tbPromotion.DSREN,CI_NEXT.tbPromotion.DSPREP),'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDDHH24MISS'),0))
        --	,TO_NUMBER(NVL(TO_CHAR(HANYO_ATTR.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
        --	,NVL(CI_DWH_MAIN.TT02SALEM_ADD1_EXP_HEN_MT.JOIN_REC_UPDDATE,0)
        --	,NVL(WEBKEIRO.JOIN_REC_UPDDATE,0)
        --)) AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
        --END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
        FROM c_tbecinquirekesai --返品交換依頼決済情報
        ,c_tbecinquire --返品交換依頼情報
        ,c_tbeckesai --決済情報
        ,tbecorder --受注情報
        ,c_tbecusercard --クレジットカード情報(顧客）
        ,tt01_henpin_riyu c_tbEcInquireMeisai --返品交換依頼明細情報
        ,tbechenpinriyu --返品理由
        ,tbpromotion --プロモーションマスタ
        ,hanyo_attr --汎用属性（通信経路）
        ,tt02salem_add1_exp_hen_mt	--ポイント交換商品サマリ
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
        --       ,TO_NUMBER(GREATEST(
        --              TO_NUMBER(NVL(TO_CHAR(CI_DWH_MAIN.KEIROKBN.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
        --            , TO_NUMBER(NVL(TO_CHAR(HANYO_WEB.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
        --        )) AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
        --END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
        FROM   keirokbn
        INNER JOIN hanyo_attr HANYO_WEB
        ON HANYO_WEB.ATTR1     = 'WEB' || '-' || KEIROKBN.KEIRO
        AND HANYO_WEB.KBNMEI = 'TUUSHINKEIRO'
        )WEBKEIRO
        --2018/3/30 ADD 小原 変更0261 END
        WHERE c_tbEcInquireKesai.diinquireid = c_tbEcInquire.diinquireid --返品交換依頼決済情報.返品交換依頼ID = 返品交換依頼情報.返品交換依頼ID
        AND c_tbEcInquireKesai.c_dikesaiid = c_tbEcKesai.c_dikesaiid --返品交換依頼決済情報.決済ID = 決済情報.決済ID
        AND c_tbEcInquireKesai.diorderid = tbEcOrder.diOrderID --返品交換依頼決済情報.受注内部ID = 受注情報.受注内部ID
        AND c_tbEcKesai.c_dicardid = c_tbEcUserCard.c_dicardid(+) --決済情報.クレジットカード内部ID = クレジットカード情報(顧客）.クレジットカード内部ID
        AND c_tbEcInquire.diinquireid = c_tbEcInquireMeisai.diinquireid --返品交換依頼情報.返品交換依頼ID = TT01返品理由.返品交換依頼ID
        AND c_tbEcInquireKesai.c_diinquirekesaiid = c_tbEcInquireMeisai.diinquirekesaiid --返品交換依頼決済情報.返品交換依頼決済ID = TT01返品理由.返品交換依頼決済ID
        AND c_tbEcInquireMeisai.dihenpinriyuid = tbEcHenpinRiyu.dihenpinriyuid(+) --返品交換依頼情報.返品理由ID = 返品理由.返品理由ID
        AND c_tbEcKesai.diCancel = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
        AND tbEcHenpinRiyu.dielimflg(+) = '0'--返品理由.削除FLG = 0：未削除
        AND tbEcOrder.diPromId = tbPromotion.dipromid(+) --受注情報．プロモーションID = プロモーションマスタ．プロモーションID
        AND tbEcOrder.C_DSUKETSUKETELCOMPANYCD = HANYO_ATTR.ATTR1(+) --受注情報．受付担当者テレマ会社コード = 汎用属性（通信経路）.属性1（テレマ会社コード）
        AND HANYO_ATTR.KBNMEI(+) = 'TUUSHINKEIRO'
        --BGN MOD 20190221 takano ***課題0029***
        --AND c_tbEcInquireKesai.c_dikesaiid = TT02SALEM_ADD1_EXP_HEN_MT.KESAIID(+) --返品交換依頼決済情報.決済ID = ポイント交換商品サマリ.決済ID
        AND cast(('H' || c_tbEcInquireKesai.c_diinquirekesaiid) as varchar) = TT02SALEM_ADD1_EXP_HEN_MT.SALENO(+)       --'H'||返品交換依頼決済情報.返品交換依頼決済ID = ポイント交換商品サマリ.売上NO
        --END MOD 20190221 takano ***課題0029***
        AND c_tbEcInquireKesai.DIELIMFLG = '0'
        AND c_tbEcInquire.DIELIMFLG = '0'
        AND c_tbEcKesai.DIELIMFLG = '0'
        AND tbEcOrder.DIELIMFLG = '0'
        AND NVL(C_TBECINQUIRE.C_DSHENPINSTS,'0') IN ('3010','5020')
        --2018/3/30 ADD 小原 変更0261 START
        --通信経路区分と結合
        AND tbEcOrder.DIORDERID = WEBKEIRO.DIORDERID(+)
  ),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        juchkbn::varchar(3) as juchkbn,
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
        henreasoncode::varchar(60) as henreasoncode,
        cancelflg::varchar(15) as cancelflg,
        insertdate::number(18,0) as insertdate,
        inserttime::number(18,0) as inserttime,
        insertid::varchar(30) as insertid,
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
        henreasonname::varchar(48) as henreasonname,
        uketsukeusrid::number(18,0) as uketsukeusrid,
        uketsuketelcompanycd::varchar(5) as uketsuketelcompanycd,
        smkeiroid::number(18,0) as smkeiroid,
        dipromid::number(10,0) as dipromid,
        dicollectprc::number(18,0) as dicollectprc,
        ditoujitsuhaisoprc::number(18,0) as ditoujitsuhaisoprc,
        didiscountall::number(18,0) as didiscountall,
        point_exchange::number(18,0) as point_exchange,
        henpinsts::varchar(6) as henpinsts,
        lastupdusrid::number(10,0) as lastupdusrid,
        inqkesairowid::varchar(1) as inqkesairowid,
        inqrowid::varchar(1) as inqrowid,
        kesairowid::varchar(1) as kesairowid,
        orderrowid::varchar(1) as orderrowid,
        ucrowid::varchar(1) as ucrowid,
        inqmeisairowid::varchar(1) as inqmeisairowid,
        henriyurowid::varchar(1) as henriyurowid,
        promorowid::varchar(1) as promorowid,
        hanyorowid::varchar(1) as hanyorowid
FROM transformed
)
SELECT *
FROM final