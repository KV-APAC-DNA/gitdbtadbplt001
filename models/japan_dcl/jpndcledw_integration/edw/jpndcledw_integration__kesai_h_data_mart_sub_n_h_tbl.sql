with c_tbecinquirekesai as (
    select * from {{ ref('jpndclitg_integration__c_tbecinquirekesai') }}
),
c_tbecinquire as (
    select * from {{ ref('jpndclitg_integration__c_tbecinquire') }}
),
c_tbeckesai as (
    select * from {{ ref('jpndclitg_integration__c_tbeckesai') }}
),
tbecorder as (
    select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
c_tbecusercard as (
    select * from {{ ref('jpndclitg_integration__c_tbecusercard') }}
),
tbechenpinriyu as (
    select * from {{ ref('jpndclitg_integration__tbechenpinriyu') }}
),
tbpromotion as (
    select * from {{ ref('jpndclitg_integration__tbpromotion') }}
),
hanyo_attr as(
    select * from {{ ref('jpndcledw_integration__hanyo_attr') }}
),
tt01_henpin_riyu as(
    select * from {{ ref('jpndcledw_integration__tt01_henpin_riyu') }}
),
KESAI_M_DATA_MART_SUB_N_H_NOTP as(
    select * from {{ ref('jpndcledw_integration__kesai_m_data_mart_sub_n_h_notp') }}
),
KEIROKBN as(
    select * from {{ source('jpdcledw_integration', 'keirokbn') }}
),
kesaiid_dummy as(
    select * from {{ ref('jpndcledw_integration__kesaiid_dummy') }}
),
transformed as(
	SELECT CAST(('H' || C_TBECINQUIREKESAI.C_DIINQUIREKESAIID) AS VARCHAR) AS SALENO,
	DECODE(TBECORDER.C_DSORDERKBN, '0', '90', '1', '91', '2', '92', '90') AS JUCHKBN,
	CAST((TO_CHAR(TBECORDER.DSORDERDT, 'YYYYMM')) AS INTEGER) AS JUCHYM,
	CAST((TO_CHAR(TBECORDER.DSORDERDT, 'YYYYMMDD')) AS INTEGER) AS JUCHDATE,
	NVL(LPAD(C_TBECKESAI.DIECUSRID, 10, '0'), '0000000000') AS KOKYANO,
	NVL(HANYO_ATTR.ATTR2, 'その他') AS HANROCODE,
	CASE 
		WHEN TBECORDER.C_DSUKETSUKETELCOMPANYCD = 'WEB'
			AND WEBKEIRO.KEIRO IS NOT NULL
			THEN NVL(WEBKEIRO.ATTR3, '不明')
		ELSE NVL(HANYO_ATTR.ATTR3, '不明')
		END AS SYOHANROBUNNAME,
	CASE 
		WHEN TBECORDER.C_DSUKETSUKETELCOMPANYCD = 'WEB'
			AND WEBKEIRO.KEIRO IS NOT NULL
			THEN NVL(WEBKEIRO.ATTR4, '不明')
		ELSE NVL(HANYO_ATTR.ATTR4, '不明')
		END AS CHUHANROBUNNAME,
	CASE 
		WHEN TBECORDER.C_DSUKETSUKETELCOMPANYCD = 'WEB'
			AND WEBKEIRO.KEIRO IS NOT NULL
			THEN NVL(WEBKEIRO.ATTR5, '不明')
		ELSE NVL(HANYO_ATTR.ATTR5, '不明')
		END AS DAIHANROBUNNAME,
	NVL(TBPROMOTION.DSPROMCODE, '00000') AS MEDIACODE,
	C_TBECKESAI.DSKESSAIHOHO AS KESSAIKBN,
	(NVL(C_TBECINQUIREKESAI.DIHAISOPRC, '0')) * - 1 AS SORYO,
	CEIL(NVL(C_TBECINQUIREKESAI.DIDIFFTOTALPRC, '0') * (C_TBECINQUIREKESAI.DITAXRATE / (1.0 * (100 + C_TBECINQUIREKESAI.DITAXRATE))) * - 1) AS TAX,
	NVL(C_TBECINQUIREKESAI.DIDIFFTOTALPRC, '0') * - 1 AS SOGOKEI,
	NVL(CAST((C_TBECUSERCARD.C_DSCARDCOMPANYID) AS VARCHAR), '0000000') AS CARDCORPCODE,
	NVL(CAST((HENPIN_RIYU.DIHENPINRIYUID) AS VARCHAR), '000') AS HENREASONCODE,
	C_TBECKESAI.DICANCEL AS CANCELFLG,
	NVL(CAST((TO_CHAR(C_TBECINQUIREKESAI.DSPREP, 'YYYYMMDD')) AS INTEGER), '0') AS INSERTDATE,
	NVL(CAST((TO_CHAR(C_TBECINQUIREKESAI.DSPREP, 'HH24MISS')) AS INTEGER), '0') AS INSERTTIME,
	NVL(CAST((C_TBECINQUIREKESAI.DIPREPUSR) AS VARCHAR), '000000') AS INSERTID,
	NVL(CAST((TO_CHAR(C_TBECINQUIREKESAI.DSREN, 'YYYYMMDD')) AS INTEGER), '0') AS UPDATEDATE,
	NVL(CAST((TO_CHAR(C_TBECINQUIREKESAI.DSREN, 'HH24MISS')) AS INTEGER), '0') AS UPDATETIME,
	NVL(C_TBECINQUIREKESAI.DSTODOKEZIP, ' ') AS ZIPCODE,
	NVL(C_TBECINQUIREKESAI.DSTODOKEPREF, ' ') AS TODOFUKENCODE,
	0 AS HAPPENPOINT,
	(CAST((NVL(C_TBECINQUIREKESAI.C_DIHENKYAKUYOTEIPOINT, '0')) AS INTEGER) - NVL(NOTP.MEISAINUKIKINGAKU, '0')) AS RIYOPOINT,
	NVL(C_TBECKESAI.DISHUKKASTS, '0') AS SHUKKASTS,
	DECODE(TBECORDER.DIROUTEID, 7, '03', 9, '04', 8, '05', '01') AS TORIKEIKBN,
	NVL(TBECORDER.C_DSTEMPOCODE, '00000') AS TENPOCODE,
	NVL(CAST((TO_CHAR(C_TBECKESAI.DSURIAGEDT, 'YYYYMM')) AS INTEGER), '0') AS SHUKAYM,
	NVL(CAST((TO_CHAR(C_TBECKESAI.DSURIAGEDT, 'YYYYMMDD')) AS INTEGER), '0') AS SHUKADATE,
	NVL(TBECORDER.C_DICLASSID, '0') AS RANK
	--BGN MOD 20190325 takano ***課題0027***
	--     , TO_CHAR(C_TBECINQUIREKESAI.C_DIKESAIID)                         AS DISPSALENO
	--     , C_TBECINQUIREKESAI.C_DIKESAIID                                  AS KESAIID
	,
	NVL(DUM.C_DIKESAIID_AFTER, CAST(C_TBECINQUIREKESAI.C_DIKESAIID AS VARCHAR)) AS DISPSALENO,
	NVL(DUM.C_DIKESAIID_AFTER, CAST(C_TBECINQUIREKESAI.C_DIKESAIID AS VARCHAR)) AS KESAIID
	--END MOD 20190325 takano ***課題0027***
	,
	TBECORDER.DIORDERCODE AS ORDERCODE,
	TBECORDER.DIORDERID AS DIORDERID,
	NVL(TBECHENPINRIYU.DSHENPINRIYU, ' ') AS HENREASONNAME,
	NVL(TBECORDER.C_DIUKETSUKEUSRID, '0') AS UKETSUKEUSRID,
	NVL(TBECORDER.C_DSUKETSUKETELCOMPANYCD, '000') AS UKETSUKETELCOMPANYCD,
	NVL(TBECORDER.DIROUTEID, '0') AS SMKEIROID,
	TBECORDER.DIPROMID AS DIPROMID,
	0 AS DICOLLECTPRC, --代引き手数料
	0 AS DITOUJITSUHAISOPRC, --当日配送手数料
	0 AS DIDISCOUNTALL, --値引き
	0 AS C_DIDISCOUNTPRC, --割引
	NVL(NOTP.MEISAINUKIKINGAKU, '0') AS POINT_EXCHANGE, --交換商品返品時返却ポイント
	NVL(C_TBECINQUIRE.C_DSHENPINSTS, '0') AS HENPINSTS, --返品ステータス
	TBECORDER.C_DILASTUPDUSRID AS LASTUPDUSRID,
	C_TBECKESAI.DIVOUCHERCODE AS DIVOUCHERCODE, --送り状NO
	TBECORDER.DITAXRATE AS DITAXRATE, --消費税率
	C_TBECKESAI.DISEIKYUREMAIN AS DISEIKYUREMAIN ,--請求残合計
	C_TBECKESAI.DINYUKINSTS AS DINYUKINSTS, --入金ステータス
	C_TBECKESAI.DICARDNYUKINSTS AS DICARDNYUKINSTS, --入金照合ステータス
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--, TO_NUMBER(GREATEST(
	--     TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.C_TBECINQUIREKESAI.DSREN,CI_NEXT.C_TBECINQUIREKESAI.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.C_TBECINQUIRE.DSREN,CI_NEXT.C_TBECINQUIRE.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.C_TBECKESAI.DSREN,CI_NEXT.C_TBECKESAI.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.TBECORDER.DSREN,CI_NEXT.TBECORDER.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.C_TBECUSERCARD.DSREN,CI_NEXT.C_TBECUSERCARD.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(CI_NEXT.TBECHENPINRIYU.DSREN,CI_NEXT.TBECHENPINRIYU.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(TO_DATE(NVL(CI_NEXT.TBPROMOTION.DSREN,CI_NEXT.TBPROMOTION.DSPREP),'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(HENPIN_RIYU.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(CI_DWH_MAIN.HANYO_ATTR.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
	--   , NVL(NOTP.JOIN_REC_UPDDATE,0)
	--   , NVL(WEBKEIRO.JOIN_REC_UPDDATE_SUB,0)
	--   , NVL(DUM.JOIN_REC_UPDDATE,0)
	--))                                                                 AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--BGN-ADD 20200309 D.KITANO ***変更20211(決済データマートに倉庫IDを付与する)****
	
	C_TBECKESAI.DISOKOID AS DISOKOID, --倉庫ID
	C_TBECKESAI.DIHAISOKEITAI AS DIHAISOKEITAI --配送形態
	--END-ADD 20200309 D.KITANO ***変更20211(決済データマートに倉庫IDを付与する)****
FROM 
    C_TBECINQUIREKESAI, --返品交換依頼決済情報
	C_TBECINQUIRE, --返品交換依頼情報
	C_TBECKESAI, --決済情報
	TBECORDER, --受注情報
	C_TBECUSERCARD, --クレジットカード情報(顧客）
	TT01_HENPIN_RIYU HENPIN_RIYU, --返品交換依頼明細情報
	TBECHENPINRIYU, --返品理由
	TBPROMOTION, --プロモーションマスタ
	HANYO_ATTR, --汎用属性（通信経路）
	(
		SELECT SALENO,
			SUM(MEISAINUKIKINGAKU) MEISAINUKIKINGAKU
		--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		--DnA側でデータマートを作成するため廃止
		--      , MAX(JOIN_REC_UPDDATE) JOIN_REC_UPDDATE
		--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		FROM KESAI_M_DATA_MART_SUB_N_H_NOTP
		WHERE MAKER = 2
		GROUP BY SALENO
		) NOTP --ポイント交換商品サマリ
	,
	(
		SELECT KEIROKBN.DIORDERID,
			KEIROKBN.KEIRO,
			HANYO_WEB.ATTR1,
			HANYO_WEB.ATTR3,
			HANYO_WEB.ATTR4,
			HANYO_WEB.ATTR5
		--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		--DnA側でデータマートを作成するため廃止
		--       , TO_NUMBER(GREATEST(
		--                TO_NUMBER(NVL(TO_CHAR(CI_DWH_MAIN.KEIROKBN.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
		--              , TO_NUMBER(NVL(TO_CHAR(HANYO_WEB.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
		--          )) AS JOIN_REC_UPDDATE_SUB     --結合レコード更新日時(JJ連携の差分抽出に使用)
		--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		FROM KEIROKBN
		INNER JOIN HANYO_ATTR HANYO_WEB ON HANYO_WEB.ATTR1 = 'WEB' || '-' || KEIROKBN.KEIRO
			AND HANYO_WEB.KBNMEI = 'TUUSHINKEIRO'
		) WEBKEIRO
	--BGN ADD 20190325 takano ***課題0027***
	,
	KESAIID_DUMMY DUM
--END ADD 20190325 takano ***課題0027***
WHERE C_TBECINQUIREKESAI.DIINQUIREID = C_TBECINQUIRE.DIINQUIREID --返品交換依頼決済情報.返品交換依頼ID = 返品交換依頼情報.返品交換依頼ID
	AND C_TBECINQUIREKESAI.C_DIKESAIID = C_TBECKESAI.C_DIKESAIID --返品交換依頼決済情報.決済ID = 決済情報.決済ID
	AND C_TBECINQUIREKESAI.DIORDERID = TBECORDER.DIORDERID --返品交換依頼決済情報.受注内部ID = 受注情報.受注内部ID
	AND C_TBECKESAI.C_DICARDID = C_TBECUSERCARD.C_DICARDID(+) --決済情報.クレジットカード内部ID = クレジットカード情報(顧客）.クレジットカード内部ID
	AND C_TBECINQUIRE.DIINQUIREID = HENPIN_RIYU.DIINQUIREID --返品交換依頼情報.返品交換依頼ID = TT01返品理由.返品交換依頼ID
	AND C_TBECINQUIREKESAI.C_DIINQUIREKESAIID = HENPIN_RIYU.DIINQUIREKESAIID --返品交換依頼決済情報.返品交換依頼決済ID = TT01返品理由.返品交換依頼決済ID
	AND HENPIN_RIYU.DIHENPINRIYUID = TBECHENPINRIYU.DIHENPINRIYUID(+) --TT01返品理由.返品理由ID = 返品理由.返品理由ID
	AND C_TBECKESAI.DICANCEL = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
	AND TBECHENPINRIYU.DIELIMFLG(+) = '0' --返品理由.削除FLG = 0：未削除
	AND TBECORDER.DIPROMID = TBPROMOTION.DIPROMID(+) --受注情報．プロモーションID = プロモーションマスタ．プロモーションID
	AND TBECORDER.C_DSUKETSUKETELCOMPANYCD = HANYO_ATTR.ATTR1(+) --受注情報．受付担当者テレマ会社コード = 汎用属性（通信経路）.属性1（テレマ会社コード）
	AND HANYO_ATTR.KBNMEI(+) = 'TUUSHINKEIRO'
	AND CAST(('H' || C_TBECINQUIREKESAI.C_DIINQUIREKESAIID) AS VARCHAR) = NOTP.SALENO(+) --返品交換依頼決済情報.決済ID = ポイント交換商品サマリ.決済ID
	AND C_TBECINQUIREKESAI.DIELIMFLG = '0'
	AND C_TBECINQUIRE.DIELIMFLG = '0'
	AND C_TBECKESAI.DIELIMFLG = '0'
	AND TBECORDER.DIELIMFLG = '0'
	AND NVL(C_TBECINQUIRE.C_DSHENPINSTS, '0') IN ('3010', '5020')
	AND TBECORDER.DIORDERID = WEBKEIRO.DIORDERID(+)
	--BGN ADD 20190325 takano ***課題0027***
	AND C_TBECINQUIREKESAI.DIINQUIREID = DUM.DIINQUIREID(+)
	AND C_TBECINQUIREKESAI.C_DIINQUIREKESAIID = DUM.C_DIINQUIREKESAIID(+)

	
	
),

final as(
    select 
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
        cancelflg::varchar(2) as cancelflg,
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
        dispsaleno::varchar(62) as dispsaleno,
        kesaiid::varchar(62) as kesaiid,
        ordercode::varchar(18) as ordercode,
        diorderid::number(10,0) as diorderid,
        henreasonname::varchar(48) as henreasonname,
        uketsukeusrid::number(18,0) as uketsukeusrid,
        uketsuketelcompanycd::varchar(5) as uketsuketelcompanycd,
        smkeiroid::number(18,0) as smkeiroid,
        dipromid::number(10,0) as dipromid,
        dicollectprc::number(18,0) as dicollectprc,
        ditoujitsuhaisoprc::number(18,0) as ditoujitsuhaisoprc,
        didiscountall::number(18,0) as didiscountall,
        c_didiscountprc::number(18,0) as c_didiscountprc,
        point_exchange::number(18,0) as point_exchange,
        henpinsts::varchar(6) as henpinsts,
        lastupdusrid::number(10,0) as lastupdusrid,
        divouchercode::varchar(768) as divouchercode,
        ditaxrate::number(3,0) as ditaxrate,
        diseikyuremain::number(10,0) as diseikyuremain,
        dinyukinsts::varchar(2) as dinyukinsts,
        dicardnyukinsts::varchar(2) as dicardnyukinsts,
        disokoid::number(10,0) as disokoid,
        dihaisokeitai::number(10,0) as dihaisokeitai
    from transformed
)
select * from final

