with c_tbecinquiremeisai as(
    select * from {{ ref('jpndclitg_integration__c_tbecinquiremeisai') }}
),
c_tbeckesai as(
    select * from {{ ref('jpndclitg_integration__c_tbeckesai') }}
),
c_tbecinquirekesai as(
    select * from {{ ref('jpndclitg_integration__c_tbecinquirekesai') }}
),
KESAIID_DUMMY as(
    select * from {{ ref('jpndcledw_integration__kesaiid_dummy') }}
),
HEN_MEISAI AS (
		SELECT 'H' || cast((C_TBECINQUIREMEISAI.DIINQUIREKESAIID) AS NUMERIC) AS SALENO, --返品交換依頼依頼ID,
			NVL(C_TBECINQUIREMEISAI.DIORDERMEISAIID, '0') AS GYONO, --明細番号,
			CASE 
				WHEN C_TBECINQUIREMEISAI.C_DSPOINTITEMFLG = '0'
					THEN '商品'
				ELSE 'ポイント交換商品'
				END AS MEISAIKBN ,--明細区分,
			C_TBECINQUIREMEISAI.DSITEMID AS ITEMCODE ,--商品コード,
			C_TBECINQUIREMEISAI.DSITEMNAME AS ITEMNAME ,--商品名,
			CAST(C_TBECINQUIREMEISAI.DIID AS VARCHAR) AS DIID ,--商品内部ID,
			CAST(C_TBECINQUIREMEISAI.DISETID AS VARCHAR) AS DISETID ,--セット商品内部ID,
			NVL(C_TBECINQUIREMEISAI.C_DIHENPINNUM, '0') AS SURYO, --返品数量,
			-1 * (
				NVL(C_TBECINQUIREMEISAI.DITOTALPRC, '0') - --税込み額(単価)
				C_TBECINQUIREMEISAI.C_DIDISCOUNTMEISAI --割引額
				) AS TANKA, --割引後税込単価：税込み額(単価) - 割引額,
			-1 * (
				(NVL(C_TBECINQUIREMEISAI.DITOTALPRC, '0') * NVL(C_TBECINQUIREMEISAI.C_DIHENPINNUM, '0')) --税込金額(単価) * 返品数量
				- (NVL(C_TBECINQUIREMEISAI.C_DIDISCOUNTMEISAI, '0') * NVL(C_TBECINQUIREMEISAI.C_DIHENPINNUM, '0')) --割引額 * 返品数量
				) AS KINGAKU, --割引後明細税込金額：税込金額(単価) * 返品数量 - 割引額 * 返品数量,
			CASE 
				WHEN C_TBECINQUIREMEISAI.C_DSPOINTITEMFLG = '0'
					THEN decode(C_TBECINQUIREMEISAI.DITOTALPRC, 0, 0, - 1 * CEIL((
									(NVL(cast(C_TBECINQUIREMEISAI.DITOTALPRC AS DECIMAL(10, 2)), 0) * NVL(cast(C_TBECINQUIREMEISAI.C_DIHENPINNUM AS DECIMAL(10, 2)), 0)) --税込金額(単価) * 返品数量
									- (NVL(cast(C_TBECINQUIREMEISAI.C_DIDISCOUNTMEISAI AS DECIMAL(10, 2)), 0) * NVL(cast(C_TBECINQUIREMEISAI.C_DIHENPINNUM AS DECIMAL(10, 2)), 0)) --割引額 * 返品数量
									) / (DECODE(C_TBECINQUIREMEISAI.DIITEMPRC, 0, 1, (cast(C_TBECINQUIREMEISAI.DITOTALPRC AS DECIMAL(10, 2)) / cast(C_TBECINQUIREMEISAI.DIITEMPRC AS DECIMAL(10, 2))))) --消費税率
							))
						--ポイント交換商品
				ELSE - 1 * CEIL((
							(NVL(cast(C_TBECINQUIREMEISAI.C_DISETITEMPRC AS DECIMAL(10, 2)), 0) * NVL(cast(C_TBECINQUIREMEISAI.C_DIHENPINNUM AS DECIMAL(10, 2)), 0)) --税込金額(単価) * 返品数量
							- (NVL(cast(C_TBECINQUIREMEISAI.C_DIDISCOUNTMEISAI AS DECIMAL(10, 2)), 0) * NVL(cast(C_TBECINQUIREMEISAI.C_DIHENPINNUM AS DECIMAL(10, 2)), 0)) --割引額 * 返品数量
							) / (DECODE(C_TBECINQUIREMEISAI.DIITEMPRC, 0, 1, DECODE(C_TBECINQUIREMEISAI.C_DISETITEMPRC, 0, 1, (cast(C_TBECINQUIREMEISAI.C_DISETITEMPRC AS DECIMAL(10, 2)) / cast(C_TBECINQUIREMEISAI.DIITEMPRC AS DECIMAL(10, 2)))))))
					--消費税率
				END MEISAINUKIKINGAKU, --割引後明細税抜金額：(税込金額(単価) * 返品数量 - 割引額 * 返品数量) * 消費税率,
			NVL(C_TBECINQUIREMEISAI.C_DIDISCOUNTRATE, '0') AS WARIRITU ,--割引率,
			- 1 * (NVL(C_TBECINQUIREMEISAI.DITOTALPRC, '0')) AS WARIMAEKOMITANKA, --割引前税込単価：単価(税込),
			- 1 * (NVL((C_TBECINQUIREMEISAI.DIITEMPRC * C_TBECINQUIREMEISAI.C_DIHENPINNUM), '0')) AS WARIMAENUKIKINGAKU ,--割引前明細税抜金額：販売単価 * 返品数量,
			- 1 * (NVL((C_TBECINQUIREMEISAI.DITOTALPRC * C_TBECINQUIREMEISAI.C_DIHENPINNUM), '0')) AS WARIMAEKOMIKINGAKU, --割引前明細税込金額：税込金額(単価) * 返品数量
			--分解後,
			CASE 
				WHEN C_TBECINQUIREMEISAI.C_DSPOINTITEMFLG = '0'
					THEN - 1 * (
							NVL(C_TBECINQUIREMEISAI.DITOTALPRC + C_TBECINQUIREMEISAI.C_DIADJUST, '0') - --税込み額(単価)
							C_TBECINQUIREMEISAI.C_DIDISCOUNTMEISAI --割引額
							)
				ELSE - 1 * (
						NVL(C_TBECINQUIREMEISAI.C_DISETITEMPRC + C_TBECINQUIREMEISAI.C_DIADJUST, '0') - --税込み額(単価)
						C_TBECINQUIREMEISAI.C_DIDISCOUNTMEISAI --割引額
						)
				END BUN_TANKA ,--割引後税込単価：税込み額(単価) - 割引額,
			CASE 
				WHEN C_TBECINQUIREMEISAI.C_DSPOINTITEMFLG = '0'
					THEN - 1 * (
							(
								(NVL(C_TBECINQUIREMEISAI.DITOTALPRC, '0') * NVL(C_TBECINQUIREMEISAI.C_DIHENPINNUM, '0')) --税込金額(単価) * 返品数量
								- (NVL(C_TBECINQUIREMEISAI.C_DIDISCOUNTMEISAI, '0') * NVL(C_TBECINQUIREMEISAI.C_DIHENPINNUM, '0')) --割引額 * 返品数量
								) + C_TBECINQUIREMEISAI.C_DIADJUST
							)
				ELSE - 1 * (
						(NVL(C_TBECINQUIREMEISAI.DITOTALPRC, '0') * NVL(C_TBECINQUIREMEISAI.C_DIHENPINNUM, '0')) --税込金額(単価) * 返品数量
						- (NVL(C_TBECINQUIREMEISAI.C_DIDISCOUNTMEISAI, '0') * NVL(C_TBECINQUIREMEISAI.C_DIHENPINNUM, '0')) --割引額 * 返品数量
						)
				END BUN_KINGAKU ,--割引後明細税込金額：税込金額(単価) * 返品数量 - 割引額 * 返品数量,
			CASE 
				WHEN C_TBECINQUIREMEISAI.C_DSPOINTITEMFLG = '0'
					THEN DECODE(C_TBECINQUIREMEISAI.DITOTALPRC, 0, 0, - 1 * CEIL((
									(NVL(cast(C_TBECINQUIREMEISAI.DITOTALPRC AS DECIMAL(10, 2)), 0) * NVL(cast(C_TBECINQUIREMEISAI.C_DIHENPINNUM AS DECIMAL(10, 2)), 0)) --税込金額(単価) * 返品数量
									- (NVL(cast(C_TBECINQUIREMEISAI.C_DIHENPINNUM AS DECIMAL(10, 2)), 0)) --割引額 * 返品数量
									) / (DECODE(C_TBECINQUIREMEISAI.DIITEMPRC, 0, 1, (cast(C_TBECINQUIREMEISAI.DITOTALPRC AS DECIMAL(10, 2)) / cast(C_TBECINQUIREMEISAI.DIITEMPRC AS DECIMAL(10, 2))))) --消費税率 --消費税率
							)) + DECODE(C_TBECINQUIREMEISAI.DITOTALPRC, 0, 0, - 1 * CEIL((
									NVL(cast(C_TBECINQUIREMEISAI.C_DIADJUST AS DECIMAL(10, 2)), 0) --税込金額(単価) * 返品数量)
									/ (DECODE(C_TBECINQUIREMEISAI.C_DIADJUST, 0, 1, (cast(C_TBECINQUIREMEISAI.DITOTALPRC AS DECIMAL(10, 2)) / cast(C_TBECINQUIREMEISAI.DIITEMPRC AS DECIMAL(10, 2)))))
									) --消費税率
							))
						--ポイント交換商品
				ELSE - 1 * CEIL((
							(NVL(cast(C_TBECINQUIREMEISAI.C_DISETITEMPRC AS DECIMAL(10, 2)) + cast(C_TBECINQUIREMEISAI.C_DIADJUST AS DECIMAL(10, 2)), 0) * NVL(cast(C_TBECINQUIREMEISAI.C_DIHENPINNUM AS DECIMAL(10, 2)), 0)) --税込金額(単価) * 返品数量
							- (NVL(cast(C_TBECINQUIREMEISAI.C_DIDISCOUNTMEISAI AS DECIMAL(10, 2)), 0) * NVL(cast(C_TBECINQUIREMEISAI.C_DIHENPINNUM AS DECIMAL(10, 2)), 0)) --割引額 * 返品数量
							) / (DECODE(C_TBECINQUIREMEISAI.DIITEMPRC + C_TBECINQUIREMEISAI.C_DIADJUST, 0, 1, DECODE(C_TBECINQUIREMEISAI.C_DISETITEMPRC + C_TBECINQUIREMEISAI.C_DIADJUST, 0, 1, ((cast(C_TBECINQUIREMEISAI.C_DISETITEMPRC AS DECIMAL(10, 2)) + C_TBECINQUIREMEISAI.C_DIADJUST) / cast(C_TBECINQUIREMEISAI.DIITEMPRC AS DECIMAL(10, 2)) + C_TBECINQUIREMEISAI.C_DIADJUST))))
						--消費税率
					)
				END BUN_MEISAINUKIKINGAKU, --割引後明細税抜金額：(税込金額(単価) * 返品数量 - 割引額 * 返品数量) * 消費税率,
			NVL(C_TBECINQUIREMEISAI.C_DIDISCOUNTRATE, '0') AS BUN_WARIRITU, --割引率,
			CASE 
				WHEN C_TBECINQUIREMEISAI.C_DSPOINTITEMFLG = '0'
					THEN - 1 * (NVL(C_TBECINQUIREMEISAI.DITOTALPRC + C_TBECINQUIREMEISAI.C_DIADJUST, '0'))
						--ポイント交換商品
				ELSE - 1 * (NVL(C_TBECINQUIREMEISAI.C_DISETITEMPRC + C_TBECINQUIREMEISAI.C_DIADJUST, '0'))
				END BUN_WARIMAEKOMITANKA, --割引前税込単価：単価(税込),
			CASE 
				WHEN C_TBECINQUIREMEISAI.C_DSPOINTITEMFLG = '0'
					THEN - 1 * (NVL(((C_TBECINQUIREMEISAI.DIITEMPRC + C_TBECINQUIREMEISAI.C_DIADJUST) * C_TBECINQUIREMEISAI.C_DIHENPINNUM), '0'))
						--ポイント交換商品
				ELSE - 1 * (NVL(((C_TBECINQUIREMEISAI.C_DISETITEMPRC + C_TBECINQUIREMEISAI.C_DIADJUST) * C_TBECINQUIREMEISAI.C_DIHENPINNUM), '0'))
				END BUN_WARIMAENUKIKINGAKU ,--割引前明細税抜金額：販売単価 * 返品数量,
			CASE 
				WHEN C_TBECINQUIREMEISAI.C_DSPOINTITEMFLG = '0'
					THEN - 1 * (NVL(((C_TBECINQUIREMEISAI.DITOTALPRC + C_TBECINQUIREMEISAI.C_DIADJUST) * C_TBECINQUIREMEISAI.C_DIHENPINNUM), '0'))
						--ポイント交換商品
				ELSE - 1 * (NVL(((C_TBECINQUIREMEISAI.C_DISETITEMPRC + C_TBECINQUIREMEISAI.C_DIADJUST) * C_TBECINQUIREMEISAI.C_DIHENPINNUM), '0'))
				END BUN_WARIMAEKOMIKINGAKU, --割引前明細税込金額：税込金額(単価) * 返品数量
			--BGN MOD 20190325 takano ***課題0027***
			--      , TO_CHAR(C_TBECINQUIREMEISAI.C_DIKESAIID)                                           AS DISPSALENO
			--      , C_TBECINQUIREMEISAI.C_DIKESAIID                                                    AS KESAIID                    --決済ID,
			NVL(DUM.C_DIKESAIID_AFTER, cast((C_TBECINQUIREKESAI.C_DIKESAIID) AS VARCHAR)) AS DISPSALENO,
			NVL(DUM.C_DIKESAIID_AFTER, cast((C_TBECINQUIREKESAI.C_DIKESAIID) AS VARCHAR)) AS KESAIID, --決済ID
			--END MOD 20190325 takano ***課題0027***,
			C_TBECKESAI.DIORDERID AS DIORDERID,
			NVL(C_TBECINQUIREKESAI.C_DSHENPINSTS, '0') AS HENPINSTS, --返品ステータス,
			C_TBECINQUIREMEISAI.C_DSPOINTITEMFLG AS C_DSPOINTITEMFLG, --ポイント交換商品フラグ,
			C_TBECINQUIREMEISAI.C_DIITEMTYPE AS C_DIITEMTYPE, --商品区分,
			C_TBECINQUIREMEISAI.C_DIADJUST AS C_DIADJUSTPRC, --調整金額(売上に項目名をあわせる),
			C_TBECINQUIREMEISAI.DITOTALPRC AS DITOTALPRC, --税込み額,
			C_TBECINQUIREMEISAI.DIITEMTAX AS DIITEMTAX, --消費税額,
			C_TBECINQUIREMEISAI.C_DIITEMTOTALPRC AS C_DIITEMTOTALPRC, --販売小計(税込),
			C_TBECINQUIREMEISAI.C_DIDISCOUNTMEISAI AS C_DIDISCOUNTMEISAI, --割引金額,
			C_TBECINQUIREMEISAI.DIOYAORDERMEISAIID AS DISETMEISAIID --親行取得用
			--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
			--DnA側でデータマートを作成するため廃止
			--, TO_NUMBER(GREATEST(
			--      TO_NUMBER(NVL(TO_CHAR(NVL(C_TBECINQUIREMEISAI.DSREN,C_TBECINQUIREMEISAI.DSPREP),'YYYYMMDDHH24MISS'),0))
			--    , TO_NUMBER(NVL(TO_CHAR(NVL(C_TBECINQUIREKESAI.DSREN,C_TBECINQUIREKESAI.DSPREP),'YYYYMMDDHH24MISS'),0))
			--    , TO_NUMBER(NVL(TO_CHAR(NVL(C_TBECKESAI.DSREN,C_TBECKESAI.DSPREP),'YYYYMMDDHH24MISS'),0))
			--    , NVL(DUM.JOIN_REC_UPDDATE,0)
			--   ))                                                                                AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
			--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
			--返品交換依頼決済情報に紐付く返品交換依頼明細情報のみ */
		FROM C_TBECINQUIREMEISAI, --返品交換依頼明細情報,
			C_TBECINQUIREKESAI, --返品交換依頼決済情報,
			C_TBECKESAI, --決済情報
			--BGN ADD 20190325 takano ***課題0027***,
			KESAIID_DUMMY DUM
		--END ADD 20190325 takano ***課題0027***
		WHERE C_TBECINQUIREMEISAI.DIINQUIREID = C_TBECINQUIREKESAI.DIINQUIREID --返品交換依頼明細情報.返品交換依頼ID = 返品交換依頼決済情報.返品交換依頼ID
			AND C_TBECINQUIREMEISAI.DIINQUIREKESAIID = C_TBECINQUIREKESAI.C_DIINQUIREKESAIID --返品交換依頼明細情報.返品交換依頼決済ID = 返品交換依頼決済情報.返品交換依頼決済ID
			AND C_TBECINQUIREKESAI.C_DIKESAIID = C_TBECKESAI.C_DIKESAIID --返品交換依頼決済情報.決済ID = 決済情報.決済ID
			AND C_TBECKESAI.DICANCEL = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
			--削除フラグ
			AND C_TBECINQUIREMEISAI.DIELIMFLG = '0'
			AND C_TBECINQUIREKESAI.DIELIMFLG = '0'
			AND C_TBECKESAI.DIELIMFLG = '0'
			--返品ステータス（返品済み、交換実施済みのものだけを表示）
			AND NVL(C_TBECINQUIREKESAI.C_DSHENPINSTS, '0') IN ('3010', '5020')
			AND (
				C_TBECINQUIREMEISAI.C_DIITEMTYPE <> '08'
				OR C_TBECINQUIREMEISAI.DISETID = C_TBECINQUIREMEISAI.DIID
				) --不要なダミーを除く
			--BGN ADD 20190325 takano ***課題0027***
			AND C_TBECINQUIREKESAI.DIINQUIREID = DUM.DIINQUIREID(+)
			AND C_TBECINQUIREKESAI.C_DIINQUIREKESAIID = DUM.C_DIINQUIREKESAIID(+)
		),
--END ADD 20190325 takano ***課題0027***
--返品明細

union1 as(
SELECT 
	SALENO, --売上NO(KEY),
	GYONO, --行NO,
	MEISAIKBN, --明細区分,
	ITEMCODE, --アイテムコード,
	ITEMNAME, --商品名,
	DIID, --商品内部ID,
	DISETID, --セット商品内部ID,
	SURYO, --数量,
	TANKA, --単価,
	KINGAKU ,--金額,
	MEISAINUKIKINGAKU, --明細税抜金額,
	WARIRITU, --適用割引率,
	WARIMAEKOMITANKA, --割引前税込単価,
	WARIMAENUKIKINGAKU, --割引前明細金額,
	WARIMAEKOMIKINGAKU, --割引前明細税込金額,
	TANKA AS BUN_TANKA, --分解後単価,
	KINGAKU AS BUN_KINGAKU, --分解後金額,
	MEISAINUKIKINGAKU AS BUN_MEISAINUKIKINGAKU, --分解後明細税抜金額,
	WARIRITU AS BUN_WARIRITU, --分解後適用割引率,
	WARIMAEKOMITANKA AS BUN_WARIMAEKOMITANKA,--分解後割引前税込単価,
	WARIMAENUKIKINGAKU AS BUN_WARIMAENUKIKINGAKU, --分解後割引前明細金額,
	WARIMAEKOMIKINGAKU AS BUN_WARIMAEKOMIKINGAKU ,--分解後割引前明細税込金額,
	DISPSALENO, --売上NO,
	KESAIID, --決済ID,
	DIORDERID, --受注内部ID,
	HENPINSTS, --返品ステータス,
	C_DSPOINTITEMFLG, --ポイント交換商品フラグ,
	C_DIITEMTYPE, --商品区分,
	C_DIADJUSTPRC, --調整金額,
	DITOTALPRC, --税込み額,
	DIITEMTAX ,--消費税額,
	C_DIITEMTOTALPRC, --販売小計(税込),
	C_DIDISCOUNTMEISAI, --割引金額,
	DISETMEISAIID ,--親行取得用,
	'DUMMY' AS C_DSSETITEMKBN ,--セット品区分,
	1 AS MAKER --マーカー
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--, JOIN_REC_UPDDATE            --結合レコード更新日時(JJ連携の差分抽出に使用)
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
FROM HEN_MEISAI
),

union2 as(
SELECT SALENO AS SALENO, -- 売上NO（KEY）,
	9000000002 AS GYONO, -- 行NO,
	'利用ポイント数(交換)' AS MEISAIKBN, -- 明細区分,
	'X000000002' AS ITEMCODE, -- アイテムコード,
	'利用ポイント数(交換)' AS ITEMNAME, -- 商品名,
	'X000000002' AS DIID ,-- 商品内部ID,
	'X000000002' AS DISETID, -- セット商品内部ID,
	1 AS SURYO, -- 数量,
	- 1 * SUM(KINGAKU) AS TANKA, -- 単価,
	- 1 * SUM(KINGAKU) AS KINGAKU ,-- 金額,
	- 1 * SUM(MEISAINUKIKINGAKU) AS MEISAINUKIKINGAKU, -- 明細税抜金額,
	0 AS WARIRITU, -- 適用割引率,
	- 1 * SUM(WARIMAEKOMITANKA) AS WARIMAEKOMITANKA, -- 割引前税込単価,
	- 1 * SUM(WARIMAENUKIKINGAKU) AS WARIMAENUKIKINGAKU, -- 割引前明細金額,
	- 1 * SUM(WARIMAEKOMIKINGAKU) AS WARIMAEKOMIKINGAKU, -- 割引前明細税込金額,
	- 1 * SUM(KINGAKU) AS BUN_TANKA, -- 分解後単価,
	- 1 * SUM(KINGAKU) AS BUN_KINGAKU, -- 分解後金額,
	- 1 * SUM(MEISAINUKIKINGAKU) AS BUN_MEISAINUKIKINGAKU, -- 分解後明細税抜金額,
	0 AS BUN_WARIRITU ,-- 分解後適用割引率,
	- 1 * SUM(WARIMAEKOMITANKA) AS BUN_WARIMAEKOMITANKA, -- 分解後割引前税込単価,
	- 1 * SUM(WARIMAENUKIKINGAKU) AS BUN_WARIMAENUKIKINGAKU, -- 分解後割引前明細金額,
	- 1 * SUM(WARIMAEKOMIKINGAKU) AS BUN_WARIMAEKOMIKINGAKU, -- 分解後割引前明細税込金額,
	DISPSALENO AS DISPSALENO, -- 売上NO,
	KESAIID AS KESAIID, -- 決済ID,
	DIORDERID AS DIORDERID, -- 受注内部ID,
	HENPINSTS AS HENPINSTS, -- 返品ステータス,
	C_DSPOINTITEMFLG AS C_DSPOINTITEMFLG, -- ポイント交換商品フラグ,
	'0' AS C_DIITEMTYPE, -- 商品区分,
	- 1 * SUM(C_DIADJUSTPRC) AS C_DIADJUSTPRC, -- 調整金額,
	- 1 * SUM(DITOTALPRC) AS DITOTALPRC, -- 税込み額,
	- 1 * SUM(DIITEMTAX) AS DIITEMTAX, -- 消費税額,
	- 1 * SUM(C_DIITEMTOTALPRC) AS C_DIITEMTOTALPRC, -- 販売小計(税込),
	- 1 * SUM(C_DIDISCOUNTMEISAI) AS C_DIDISCOUNTMEISAI, -- 割引金額,
	9000000002 AS DISETMEISAIID ,-- 親行取得用,
	'DUMMY' AS C_DSSETITEMKBN ,-- セット品区分,
	2 AS MAKER, -- マーカー
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--, MAX(JOIN_REC_UPDDATE)        AS JOIN_REC_UPDDATE       --結合レコード更新日時(JJ連携の差分抽出に使用)
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
FROM HEN_MEISAI
WHERE MEISAIKBN = 'ポイント交換商品'
	AND DISETID = DIID --親の値のみ取得
GROUP BY SALENO,
	DISPSALENO,
	KESAIID,
	DIORDERID,
	HENPINSTS, --現行同様に加える(件数は変動せず)。
	C_DSPOINTITEMFLG
),
transformed as(
    select * from union1
    union all
    select * from union2
),
final as(
    select 
        saleno::varchar(64) as saleno,
        gyono::number(18,0) as gyono,
        meisaikbn::varchar(30) as meisaikbn,
        itemcode::varchar(45) as itemcode,
        itemname::varchar(190) as itemname,
        diid::varchar(60) as diid,
        disetid::varchar(60) as disetid,
        suryo::number(18,0) as suryo,
        tanka::number(18,0) as tanka,
        kingaku::number(18,0) as kingaku,
        meisainukikingaku::number(18,0) as meisainukikingaku,
        wariritu::number(18,0) as wariritu,
        warimaekomitanka::number(18,0) as warimaekomitanka,
        warimaenukikingaku::number(18,0) as warimaenukikingaku,
        warimaekomikingaku::number(18,0) as warimaekomikingaku,
        bun_tanka::number(18,0) as bun_tanka,
        bun_kingaku::number(18,0) as bun_kingaku,
        bun_meisainukikingaku::number(18,0) as bun_meisainukikingaku,
        bun_wariritu::number(18,0) as bun_wariritu,
        bun_warimaekomitanka::number(18,0) as bun_warimaekomitanka,
        bun_warimaenukikingaku::number(18,0) as bun_warimaenukikingaku,
        bun_warimaekomikingaku::number(18,0) as bun_warimaekomikingaku,
        dispsaleno::varchar(64) as dispsaleno,
        kesaiid::varchar(64) as kesaiid,
        diorderid::number(10,0) as diorderid,
        henpinsts::varchar(6) as henpinsts,
        c_dspointitemflg::varchar(3) as c_dspointitemflg,
        c_diitemtype::varchar(4) as c_diitemtype,
        c_diadjustprc::number(18,0) as c_diadjustprc,
        ditotalprc::number(18,0) as ditotalprc,
        diitemtax::number(18,0) as diitemtax,
        c_diitemtotalprc::number(18,0) as c_diitemtotalprc,
        c_didiscountmeisai::number(18,0) as c_didiscountmeisai,
        disetmeisaiid::number(18,0) as disetmeisaiid,
        c_dssetitemkbn::varchar(8) as c_dssetitemkbn,
        maker::number(18,0) as maker
    from transformed
)
select * from final

