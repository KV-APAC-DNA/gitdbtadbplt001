with KESAI_M_DATA_MART_SUB_N_H_NOTP as(
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_M_DATA_MART_SUB_N_H_NOTP
),
KESAI_H_DATA_MART_SUB_N_H as(
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_H_DATA_MART_SUB_N_H
),
union1 as(
 SELECT 
    SALENO, -- 売上NO（KEY）
	GYONO, -- 行NO
	MEISAIKBN, -- 明細区分
	ITEMCODE, -- アイテムコード
	ITEMNAME, -- 商品名
	DIID, -- 商品内部ID
	DISETID, -- セット商品内部ID
	SURYO, -- 数量
	TANKA, -- 単価
	KINGAKU, -- 金額
	MEISAINUKIKINGAKU, -- 明細税抜金額
	WARIRITU, -- 適用割引率
	WARIMAEKOMITANKA, -- 割引前税込単価
	WARIMAENUKIKINGAKU, -- 割引前明細金額
	WARIMAEKOMIKINGAKU, -- 割引前明細税込金額
	BUN_TANKA, -- 分解後単価
	BUN_KINGAKU, -- 分解後金額
	BUN_MEISAINUKIKINGAKU, -- 分解後明細税抜金額
	BUN_WARIRITU, -- 分解後適用割引率
	BUN_WARIMAEKOMITANKA, -- 分解後割引前税込単価
	BUN_WARIMAENUKIKINGAKU, -- 分解後割引前明細金額
	BUN_WARIMAEKOMIKINGAKU, -- 分解後割引前明細税込金額
	DISPSALENO, -- 売上NO
	KESAIID, -- 決済ID
	DIORDERID, -- 受注内部ID
	HENPINSTS, -- 返品ステータス
	C_DSPOINTITEMFLG, -- ポイント交換商品フラグ
	C_DIITEMTYPE, -- 商品区分
	C_DIADJUSTPRC, -- 調整金額
	DITOTALPRC, -- 税込み額
	DIITEMTAX, -- 消費税額
	C_DIITEMTOTALPRC, --, 販売小計(税込)
	C_DIDISCOUNTMEISAI, -- 割引金額
	DISETMEISAIID, -- 親行取得用
	C_DSSETITEMKBN, -- セット品区分
	MAKER -- マーカー
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--, JOIN_REC_UPDDATE       --結合レコード更新日時(JJ連携の差分抽出に使用)
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
    FROM KESAI_M_DATA_MART_SUB_N_H_NOTP
--利用ポイント数(交換以外) ダミー

),
union2 as(
 
SELECT 
    SALENO AS SALENO, -- 売上NO（KEY）
	9000000001 AS GYONO, -- 行NO
	'利用ポイント数(交換以外)' AS MEISAIKBN, -- 明細区分
	'X000000001' AS ITEMCODE, -- アイテムコード
	'利用ポイント数(交換以外)' AS ITEMNAME, -- 商品名
	'X000000001' AS DIID, -- 商品内部ID
	'X000000001' AS DISETID, -- セット商品内部ID
	1 AS SURYO, -- 数量
	RIYOPOINT AS TANKA, -- 単価
	RIYOPOINT AS KINGAKU, -- 金額
	RIYOPOINT AS MEISAINUKIKINGAKU, -- 明細税抜金額
	0 AS WARIRITU, -- 適用割引率
	RIYOPOINT AS WARIMAEKOMITANKA, -- 割引前税込単価
	RIYOPOINT AS WARIMAENUKIKINGAKU, -- 割引前明細金額
	RIYOPOINT AS WARIMAEKOMIKINGAKU, -- 割引前明細税込金額
	RIYOPOINT AS BUN_TANKA, -- 分解後単価
	RIYOPOINT AS BUN_KINGAKU, -- 分解後金額
	RIYOPOINT AS BUN_MEISAINUKIKINGAKU, -- 分解後明細税抜金額
	0 AS BUN_WARIRITU, -- 分解後適用割引率
	RIYOPOINT AS BUN_WARIMAEKOMITANKA, -- 分解後割引前税込単価
	RIYOPOINT AS BUN_WARIMAENUKIKINGAKU, -- 分解後割引前明細金額
	RIYOPOINT AS BUN_WARIMAEKOMIKINGAKU, -- 分解後割引前明細税込金額
	DISPSALENO AS DISPSALENO, -- 売上NO
	KESAIID AS KESAIID, -- 決済ID
	DIORDERID AS DIORDERID, -- 受注内部ID
	HENPINSTS AS HENPINSTS, -- 返品ステータス
	'0' AS C_DSPOINTITEMFLG, -- ポイント交換商品フラグ
	'0' AS C_DIITEMTYPE, -- 商品区分
	0 AS C_DIADJUSTPRC, -- 調整金額
	0 AS DITOTALPRC, -- 税込み額
	0 AS DIITEMTAX, -- 消費税額
	0 AS C_DIITEMTOTALPRC, --, 販売小計(税込)
	0 AS C_DIDISCOUNTMEISAI, -- 割引金額
	9000000001 AS DISETMEISAIID, -- 親行取得用
	'DUMMY' AS C_DSSETITEMKBN, -- セット品区分
	3 AS MAKER -- マーカー
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--, JOIN_REC_UPDDATE           AS JOIN_REC_UPDDATE       --結合レコード更新日時(JJ連携の差分抽出に使用)
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
FROM KESAI_H_DATA_MART_SUB_N_H
WHERE RIYOPOINT <> 0

),
transformed as(
    select * from union1
    union all
    select * from union2
)
select * from transformed