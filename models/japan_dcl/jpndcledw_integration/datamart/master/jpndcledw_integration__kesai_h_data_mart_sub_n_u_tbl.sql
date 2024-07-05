with c_tbeckesaihistory as(
    select * from {{ ref('jpndclitg_integration__c_tbeckesaihistory') }}
),
c_tbecorderhistory as(
    select * from {{ ref('jpndclitg_integration__c_tbecorderhistory') }}
),
c_tbecusercard as(
    select * from {{ ref('jpndclitg_integration__c_tbecusercard') }}
),
tbpromotion as(
    select * from {{ ref('jpndclitg_integration__tbpromotion') }}
),
c_tbeckesai as(
    select * from {{ ref('jpndclitg_integration__c_tbeckesai') }}
),
tbecorder as(
    select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
KEIROKBN as(
    select * from {{ source('jpndcledw_integration', 'keirokbn') }}
),
kesai_h_data_mart_sub_n_rirek as(
    select * from {{ source('jpndcledw_integration', 'kesai_h_data_mart_sub_n_rirek') }}
),
hanyo_attr as(
    select * from {{ source('jpndcledw_integration', 'hanyo_attr') }}
),
kesaiid_dummy as(
    select * from {{ ref('jpndcledw_integration__kesaiid_dummy') }}
),
union1 as(
SELECT CAST(('U' || C_TBECKESAI.C_DIKESAIID) AS VARCHAR) AS SALENO,
	TBECORDER.C_DSORDERKBN AS JUCHKBN,
	CAST(TO_CHAR(TBECORDER.DSORDERDT, 'YYYYMM') AS INTEGER) AS JUCHYM,
	CAST(TO_CHAR(TBECORDER.DSORDERDT, 'YYYYMMDD') AS INTEGER) AS JUCHDATE,
	NVL(LPAD(TBECORDER.DIECUSRID, 10, '0'), '0000000000') AS KOKYANO,
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
	NVL(C_TBECKESAI.DIHAISOPRC, 0) AS SORYO,
	CEIL(NVL(C_TBECKESAI.DISEIKYUPRC, 0) * (TBECORDER.DITAXRATE / (1.0 * (100 + TBECORDER.DITAXRATE)))) AS TAX,
	NVL(C_TBECKESAI.DISEIKYUPRC, 0) AS SOGOKEI,
	NVL(CAST(C_TBECUSERCARD.C_DSCARDCOMPANYID AS VARCHAR), '0000000') AS CARDCORPCODE,
	'000' AS HENREASONCODE,
	C_TBECKESAI.DICANCEL AS CANCELFLG,
	NVL(CAST(TO_CHAR(C_TBECKESAI.DSPREP, 'YYYYMMDD') AS INTEGER), 0) AS INSERTDATE,
	NVL(CAST(TO_CHAR(C_TBECKESAI.DSPREP, 'HH24MISS') AS INTEGER), 0) AS INSERTTIME,
	NVL(CAST(TBECKESAI.DIPREPUSR AS VARCHAR), '000000') AS INSERTID,
	NVL(CAST(TO_CHAR(C_TBECKESAI.DSREN, 'YYYYMMDD') AS INTEGER), 0) AS UPDATEDATE,
	NVL(CAST(TO_CHAR(C_TBECKESAI.DSREN, 'HH24MISS') AS INTEGER), 0) AS UPDATETIME,
	NVL(C_TBECKESAI.DSTODOKEZIP, ' ') AS ZIPCODE,
	NVL(C_TBECKESAI.DSTODOKEPREF, ' ') AS TODOFUKENCODE,
	NVL((C_TBECKESAI.C_DIKAKUTOKUYOTEIPOINT), 0) AS HAPPENPOINT,
	- 1 * NVL(C_TBECKESAI.DIUSEPOINT, 0) AS RIYOPOINT,
	NVL(TBECKESAI.DISHUKKASTS, '0') AS SHUKKASTS --決済情報.出荷ステータス
	,
	DECODE(TBECORDER.DIROUTEID, 7, '03', 9, '04', 8, '05', '01') AS TORIKEIKBN,
	NVL(TBECORDER.C_DSTEMPOCODE, '00000') AS TENPOCODE,
	NVL(CAST(TO_CHAR(TBECKESAI.DSURIAGEDT, 'YYYYMM') AS INTEGER), 0) AS SHUKAYM --決済情報.出荷月
	,
	NVL(CAST(TO_CHAR(TBECKESAI.DSURIAGEDT, 'YYYYMMDD') AS INTEGER), 0) AS SHUKADATE --決済情報.出荷日付
	,
	NVL(TBECORDER.C_DICLASSID, 0) AS RANK,
	CAST(C_TBECKESAI.C_DIKESAIID AS VARCHAR) AS DISPSALENO --TO_CHAR(決済ID)
	--BGN MOD 20190325 takano ***課題0027***
	--     , C_TBECKESAI.C_DIKESAIID                                      AS KESAIID
	,
	CAST(C_TBECKESAI.C_DIKESAIID AS VARCHAR) AS KESAIID
	--END MOD 20190325 takano ***課題0027***
	,
	TBECORDER.DIORDERCODE AS ORDERCODE,
	' ' AS HENREASONNAME,
	NVL(TBECORDER.C_DIUKETSUKEUSRID, 0) AS UKETSUKEUSRID,
	NVL(TBECORDER_NOW.C_DSUKETSUKETELCOMPANYCD, '000') AS UKETSUKETELCOMPANYCD,
	NVL(TBECORDER.DIROUTEID, 0) AS SMKEIROID,
	TBECORDER.DIPROMID AS DIPROMID,
	NVL(C_TBECKESAI.C_DICOLLECTPRC, 0) AS DICOLLECTPRC, --代引き手数料
	NVL(C_TBECKESAI.C_DITOUJITSUHAISOPRC, 0) AS DITOUJITSUHAISOPRC ,--当日配送手数料
	NVL(C_TBECKESAI.C_DIDISCOUNTALL, 0) AS DIDISCOUNTALL, --値引き
	NVL(C_TBECKESAI.C_DIDISCOUNTPRC, 0) AS C_DIDISCOUNTPRC, --割引
	- 1 * CAST((NVL(C_TBECKESAI.C_DIEXCHANGEPOINT, 0)) AS INTEGER) AS POINT_EXCHANGE, --交換商品ポイント
	TBECORDER_NOW.C_DILASTUPDUSRID AS LASTUPDUSRID,
	TBECKESAI.DIVOUCHERCODE AS DIVOUCHERCODE, --送り状NO
	TBECORDER.DITAXRATE AS DITAXRATE, --消費税率
	TBECKESAI.DISEIKYUREMAIN AS DISEIKYUREMAIN, --請求残合計
	TBECKESAI.DINYUKINSTS AS DINYUKINSTS, --入金ステータス
	TBECKESAI.DICARDNYUKINSTS AS DICARDNYUKINSTS, --入金照合ステータス
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--, TO_NUMBER(GREATEST(
	--     TO_NUMBER(NVL(TO_CHAR(NVL(C_TBECKESAI.DSREN,C_TBECKESAI.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(TBECORDER.DSREN,TBECORDER.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(C_TBECUSERCARD.DSREN,C_TBECUSERCARD.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(TO_DATE(NVL(CI_NEXT.TBPROMOTION.DSREN,CI_NEXT.TBPROMOTION.DSPREP),'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(HANYO_ATTR.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
	--   , NVL(RIREK.JOIN_REC_UPDDATE,0)
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(TBECKESAI.DSREN,TBECKESAI.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(TBECORDER_NOW.DSREN,TBECORDER_NOW.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , NVL(WEBKEIRO.JOIN_REC_UPDDATE_SUB,0)
	-- ))                                                            AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--BGN-ADD 20200309 D.KITANO ***変更20211(決済データマートに倉庫IDを付与する)****
	TBECKESAI.DISOKOID AS DISOKOID, --倉庫ID
	TBECKESAI.DIHAISOKEITAI AS DIHAISOKEITAI --配送形態
	--END-ADD 20200309 D.KITANO ***変更20211(決済データマートに倉庫IDを付与する)****
FROM 
    C_TBECKESAIHISTORY C_TBECKESAI, --決済情報履歴
	C_TBECORDERHISTORY TBECORDER, --受注情報履歴
	C_TBECUSERCARD C_TBECUSERCARD, --クレジットカード情報(顧客）
	TBPROMOTION TBPROMOTION, --プロモーションマスタ
	HANYO_ATTR HANYO_ATTR, --汎用属性（通信経路）
	KESAI_H_DATA_MART_SUB_N_RIREK RIREK, --受注変更履歴QV
	C_TBECKESAI TBECKESAI, --決済情報
	TBECORDER TBECORDER_NOW, --受注情報
	(
		SELECT KEIROKBN.DIORDERID,
			KEIROKBN.KEIRO,
			HANYO_WEB.ATTR1,
			HANYO_WEB.ATTR3,
			HANYO_WEB.ATTR4,
			HANYO_WEB.ATTR5
		--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		--DnA側でデータマートを作成するため廃止
		--      , TO_NUMBER(GREATEST(
		--            TO_NUMBER(NVL(TO_CHAR(KEIROKBN.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
		--          , TO_NUMBER(NVL(TO_CHAR(HANYO_WEB.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
		--        )) AS JOIN_REC_UPDDATE_SUB     --結合レコード更新日時(JJ連携の差分抽出に使用)
		--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		FROM KEIROKBN
		INNER JOIN HANYO_ATTR HANYO_WEB ON HANYO_WEB.ATTR1 = 'WEB' || '-' || KEIROKBN.KEIRO
			AND HANYO_WEB.KBNMEI = 'TUUSHINKEIRO'
		) WEBKEIRO
WHERE C_TBECKESAI.DIORDERID = TBECORDER.DIORDERID --決済情報履歴．受注内部ID = 受注情報履歴．受注内部ID
	AND C_TBECKESAI.DIORDERHISTID = TBECORDER.DIORDERHISTID --決済情報履歴．履歴ID = 受注情報履歴．履歴ID
	AND TBECORDER.DIORDERID = TBECORDER_NOW.DIORDERID --受注情報履歴．受注内部ID = 受注情報．受注内部ID
	AND C_TBECKESAI.C_DICARDID = C_TBECUSERCARD.C_DICARDID(+) --決済情報履歴．クレジットカード内部ID = クレジットカード情報(顧客）．クレジットカード内部ID
	AND TBECORDER.DIPROMID = TBPROMOTION.DIPROMID(+) --受注情報履歴．プロモーションID = プロモーションマスタ．プロモーションID
	AND HANYO_ATTR.ATTR1(+) = TBECORDER.C_DSUKETSUKETELCOMPANYCD --汎用属性（通信経路）.属性1（テレマ会社コード） = 受注情報履歴．受付担当者テレマ会社コード
	AND HANYO_ATTR.KBNMEI(+) = 'TUUSHINKEIRO'
	AND RIREK.DIORDERID = TBECORDER.DIORDERID --受注変更履歴.受注内部ID = 受注情報履歴.受注内部ID
	AND RIREK.DIORDERHISTID = TBECORDER.DIORDERHISTID --受注変更履歴.履歴ID = 受注情報履歴.履歴ID
	AND C_TBECKESAI.DICANCEL = '0' --決済情報履歴.キャンセルフラグ = '0'(0:有効)
	AND C_TBECKESAI.DIELIMFLG = '0' --決済情報履歴.論理削除フラグ = '0'(0:有効)
	AND TBECORDER.DIELIMFLG = '0' --受注情報履歴.論理削除フラグ = '0'(0:有効)
	AND C_TBECKESAI.C_DIKESAIID = TBECKESAI.C_DIKESAIID --決済情報履歴.決済ID = 決済情報.決済ID
	AND TBECKESAI.DICANCEL = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
	AND TBECKESAI.DIELIMFLG = '0' --決済情報.論理削除フラグ = '0'(0:有効)
	AND TBECORDER_NOW.DIELIMFLG = '0' --受注情報.論理削除フラグ = '0'(0:有効)
	AND TBECORDER_NOW.DIORDERID = WEBKEIRO.DIORDERID(+)
--BGN ADD 20190325 takano ***課題0027***
--ダミーレコード行作成
--金額、ポイントは全て0
),

union2 as(
SELECT CAST(('U' || DUM.C_DIKESAIID_AFTER) AS VARCHAR) AS SALENO,
	TBECORDER.C_DSORDERKBN AS JUCHKBN,
	CAST((TO_CHAR(TBECORDER.DSORDERDT, 'YYYYMM')) AS INTEGER) AS JUCHYM,
	CAST((TO_CHAR(TBECORDER.DSORDERDT, 'YYYYMMDD')) AS INTEGER) AS JUCHDATE,
	NVL(LPAD(TBECORDER.DIECUSRID, 10, '0'), '0000000000') AS KOKYANO,
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
	0 AS SORYO,
	0 AS TAX,
	0 AS SOGOKEI,
	NVL(CAST(C_TBECUSERCARD.C_DSCARDCOMPANYID AS VARCHAR), '0000000') AS CARDCORPCODE,
	'000' AS HENREASONCODE,
	C_TBECKESAI.DICANCEL AS CANCELFLG,
	NVL(CAST(TO_CHAR(C_TBECKESAI.DSPREP, 'YYYYMMDD') AS INTEGER), 0) AS INSERTDATE,
	NVL(CAST(TO_CHAR(C_TBECKESAI.DSPREP, 'HH24MISS') AS INTEGER), 0) AS INSERTTIME,
	NVL(CAST(TBECKESAI.DIPREPUSR AS VARCHAR), '000000') AS INSERTID,
	NVL(CAST(TO_CHAR(C_TBECKESAI.DSREN, 'YYYYMMDD') AS INTEGER), 0) AS UPDATEDATE,
	NVL(CAST(TO_CHAR(C_TBECKESAI.DSREN, 'HH24MISS') AS INTEGER), 0) AS UPDATETIME,
	NVL(C_TBECKESAI.DSTODOKEZIP, ' ') AS ZIPCODE,
	NVL(C_TBECKESAI.DSTODOKEPREF, ' ') AS TODOFUKENCODE,
	0 AS HAPPENPOINT,
	0 AS RIYOPOINT,
	NVL(TBECKESAI.DISHUKKASTS, '0') AS SHUKKAST, --決済情報.出荷ステータス
	DECODE(TBECORDER.DIROUTEID, 7, '03', 9, '04', 8, '05', '01') AS TORIKEIKBN,
	NVL(TBECORDER.C_DSTEMPOCODE, '00000') AS TENPOCODE,
	NVL(CAST(TO_CHAR(TBECKESAI.DSURIAGEDT, 'YYYYMM') AS INTEGER), 0) AS SHUKAYM, --決済情報.出荷月
	NVL(CAST(TO_CHAR(TBECKESAI.DSURIAGEDT, 'YYYYMMDD') AS INTEGER), 0) AS SHUKADATE, --決済情報.出荷日付
	NVL(TBECORDER.C_DICLASSID, 0) AS RANK,
	DUM.C_DIKESAIID_AFTER AS DISPSALENO, --TO_CHAR(決済ID)
	DUM.C_DIKESAIID_AFTER AS KESAIID,
	TBECORDER.DIORDERCODE AS ORDERCODE,
	' ' AS HENREASONNAME,
	NVL(TBECORDER.C_DIUKETSUKEUSRID, 0) AS UKETSUKEUSRID,
	NVL(TBECORDER_NOW.C_DSUKETSUKETELCOMPANYCD, '000') AS UKETSUKETELCOMPANYCD,
	NVL(TBECORDER.DIROUTEID, 0) AS SMKEIROID,
	TBECORDER.DIPROMID AS DIPROMID,
	0 AS DICOLLECTPRC ,--代引き手数料
	0 AS DITOUJITSUHAISOPRC, --当日配送手数料
	0 AS DIDISCOUNTALL, --値引き
	0 AS C_DIDISCOUNTPRC, --割引
	0 AS POINT_EXCHANGE, --交換商品ポイント
	TBECORDER_NOW.C_DILASTUPDUSRID AS LASTUPDUSRID,
	TBECKESAI.DIVOUCHERCODE AS DIVOUCHERCODE, --送り状NO
	TBECORDER.DITAXRATE AS DITAXRATE, --消費税率
	0 AS DISEIKYUREMAIN, --請求残合計
	TBECKESAI.DINYUKINSTS AS DINYUKINSTS, --入金ステータス
	TBECKESAI.DICARDNYUKINSTS AS DICARDNYUKINSTS, --入金照合ステータス
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--, TO_NUMBER(GREATEST(
	--     TO_NUMBER(NVL(TO_CHAR(NVL(C_TBECKESAI.DSREN,C_TBECKESAI.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(TBECORDER.DSREN,TBECORDER.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(C_TBECUSERCARD.DSREN,C_TBECUSERCARD.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(TO_DATE(NVL(CI_NEXT.TBPROMOTION.DSREN,CI_NEXT.TBPROMOTION.DSPREP),'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(HANYO_ATTR.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
	--   , NVL(RIREK.JOIN_REC_UPDDATE,0)
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(TBECKESAI.DSREN,TBECKESAI.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , TO_NUMBER(NVL(TO_CHAR(NVL(TBECORDER_NOW.DSREN,TBECORDER_NOW.DSPREP),'YYYYMMDDHH24MISS'),0))
	--   , NVL(WEBKEIRO.JOIN_REC_UPDDATE_SUB,0)
	--   , NVL(DUM.JOIN_REC_UPDDATE,0)
	-- ))                                                            AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--BGN-ADD 20200309 D.KITANO ***変更20211(決済データマートに倉庫IDを付与する)****
	
	TBECKESAI.DISOKOID AS DISOKOID, --倉庫ID
	TBECKESAI.DIHAISOKEITAI AS DIHAISOKEITAI --配送形態
	--END-ADD 20200309 D.KITANO ***変更20211(決済データマートに倉庫IDを付与する)****
FROM 
    C_TBECKESAIHISTORY C_TBECKESAI, --決済情報履歴
	C_TBECORDERHISTORY TBECORDER, --受注情報履歴
	C_TBECUSERCARD C_TBECUSERCARD, --クレジットカード情報(顧客）
	TBPROMOTION TBPROMOTION, --プロモーションマスタ
	HANYO_ATTR HANYO_ATTR, --汎用属性（通信経路）
	KESAI_H_DATA_MART_SUB_N_RIREK RIREK, --受注変更履歴QV
	C_TBECKESAI TBECKESAI, --決済情報
	TBECORDER TBECORDER_NOW, --受注情報
	(
		SELECT KEIROKBN.DIORDERID,
			KEIROKBN.KEIRO,
			HANYO_WEB.ATTR1,
			HANYO_WEB.ATTR3,
			HANYO_WEB.ATTR4,
			HANYO_WEB.ATTR5
		--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		--DnA側でデータマートを作成するため廃止
		--      , TO_NUMBER(GREATEST(
		--            TO_NUMBER(NVL(TO_CHAR(CI_DWH_MAIN.KEIROKBN.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
		--          , TO_NUMBER(NVL(TO_CHAR(HANYO_WEB.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
		--        )) AS JOIN_REC_UPDDATE_SUB     --結合レコード更新日時(JJ連携の差分抽出に使用)
		--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		FROM KEIROKBN
		INNER JOIN HANYO_ATTR HANYO_WEB ON HANYO_WEB.ATTR1 = 'WEB' || '-' || KEIROKBN.KEIRO
			AND HANYO_WEB.KBNMEI = 'TUUSHINKEIRO'
		) WEBKEIRO
	--BGN-UPD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	,
	(
		SELECT DISTINCT C_DIKESAIID_BEFORE,
			C_DIKESAIID_AFTER
		FROM kesaiid_dummy
		) DUM
--DnA側でデータマートを作成するため廃止
--, (SELECT C_DIKESAIID_BEFORE
--         ,C_DIKESAIID_AFTER
--         ,MAX(JOIN_REC_UPDDATE) AS JOIN_REC_UPDDATE
--     FROM CI_DWH_MAIN.KESAIID_DUMMY
--     GROUP BY C_DIKESAIID_BEFORE,C_DIKESAIID_AFTER)DUM
--END-UPD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
WHERE C_TBECKESAI.DIORDERID = TBECORDER.DIORDERID --決済情報履歴．受注内部ID = 受注情報履歴．受注内部ID
	AND C_TBECKESAI.DIORDERHISTID = TBECORDER.DIORDERHISTID --決済情報履歴．履歴ID = 受注情報履歴．履歴ID
	AND TBECORDER.DIORDERID = TBECORDER_NOW.DIORDERID --受注情報履歴．受注内部ID = 受注情報．受注内部ID
	AND C_TBECKESAI.C_DICARDID = C_TBECUSERCARD.C_DICARDID(+) --決済情報履歴．クレジットカード内部ID = クレジットカード情報(顧客）．クレジットカード内部ID
	AND TBECORDER.DIPROMID = TBPROMOTION.DIPROMID(+) --受注情報履歴．プロモーションID = プロモーションマスタ．プロモーションID
	AND HANYO_ATTR.ATTR1(+) = TBECORDER.C_DSUKETSUKETELCOMPANYCD --汎用属性（通信経路）.属性1（テレマ会社コード） = 受注情報履歴．受付担当者テレマ会社コード
	AND HANYO_ATTR.KBNMEI(+) = 'TUUSHINKEIRO'
	AND RIREK.DIORDERID = TBECORDER.DIORDERID --受注変更履歴.受注内部ID = 受注情報履歴.受注内部ID
	AND RIREK.DIORDERHISTID = TBECORDER.DIORDERHISTID --受注変更履歴.履歴ID = 受注情報履歴.履歴ID
	AND C_TBECKESAI.DICANCEL = '0' --決済情報履歴.キャンセルフラグ = '0'(0:有効)
	AND C_TBECKESAI.DIELIMFLG = '0' --決済情報履歴.論理削除フラグ = '0'(0:有効)
	AND TBECORDER.DIELIMFLG = '0' --受注情報履歴.論理削除フラグ = '0'(0:有効)
	AND C_TBECKESAI.C_DIKESAIID = TBECKESAI.C_DIKESAIID --決済情報履歴.決済ID = 決済情報.決済ID
	AND TBECKESAI.DICANCEL = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
	AND TBECKESAI.DIELIMFLG = '0' --決済情報.論理削除フラグ = '0'(0:有効)
	AND TBECORDER_NOW.DIELIMFLG = '0' --受注情報.論理削除フラグ = '0'(0:有効)
	AND TBECORDER_NOW.DIORDERID = WEBKEIRO.DIORDERID(+)
	AND DUM.C_DIKESAIID_BEFORE = TBECKESAI.C_DIKESAIID

),
transformed as(
    select * from union1
    union all
    select * from union2
),
final as(
    select 
        saleno::varchar(63) as saleno,
        juchkbn::varchar(2) as juchkbn,
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
        henreasonname::varchar(2) as henreasonname,
        uketsukeusrid::number(18,0) as uketsukeusrid,
        uketsuketelcompanycd::varchar(5) as uketsuketelcompanycd,
        smkeiroid::number(18,0) as smkeiroid,
        dipromid::number(10,0) as dipromid,
        dicollectprc::number(18,0) as dicollectprc,
        ditoujitsuhaisoprc::number(18,0) as ditoujitsuhaisoprc,
        didiscountall::number(18,0) as didiscountall,
        c_didiscountprc::number(18,0) as c_didiscountprc,
        point_exchange::number(18,0) as point_exchange,
        lastupdusrid::number(10,0) as lastupdusrid,
        divouchercode::varchar(768) as divouchercode,
        ditaxrate::number(3,0) as ditaxrate,
        diseikyuremain::number(18,0) as diseikyuremain,
        dinyukinsts::varchar(2) as dinyukinsts,
        dicardnyukinsts::varchar(2) as dicardnyukinsts,
        disokoid::number(10,0) as disokoid,
        dihaisokeitai::number(10,0) as dihaisokeitai
    from transformed
)
select * from final
