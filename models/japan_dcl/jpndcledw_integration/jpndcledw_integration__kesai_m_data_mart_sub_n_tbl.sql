with KESAI_M_DATA_MART_SUB_N_U as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_M_DATA_MART_SUB_N_U
),
KESAI_M_DATA_MART_SUB_N_H as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_M_DATA_MART_SUB_N_H
),
KESAI_H_DATA_MART_SUB_N as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_H_DATA_MART_SUB_N
),
union1 as(
    SELECT SALENO, --売上No（KEY）
			GYONO, --行No
			MEISAIKBN, --明細区分
			ITEMCODE, --アイテムコード
			ITEMNAME, --商品名
			DIID, --商品内部ID
			DISETID, --セット商品内部ID
			SURYO, --数量
			TANKA, --単価
			KINGAKU, --金額
			MEISAINUKIKINGAKU, --明細税抜金額
			WARIRITU, --適用割引率
			WARIMAEKOMITANKA, --割引前税込単価
			WARIMAENUKIKINGAKU, --割引前明細金額
			WARIMAEKOMIKINGAKU, --割引前明細税込金額
			BUN_TANKA, -- 分解後単価
			BUN_KINGAKU, -- 分解後金額
			BUN_MEISAINUKIKINGAKU, -- 分解後明細税抜金額
			BUN_WARIRITU, -- 分解後適用割引率
			BUN_WARIMAEKOMITANKA, -- 分解後割引前税込単価
			BUN_WARIMAENUKIKINGAKU, -- 分解後割引前明細金額
			BUN_WARIMAEKOMIKINGAKU, -- 分解後割引前明細税込金額
			DISPSALENO, --売上No
			KESAIID, --決済ID
			DIORDERID, --受注内部ID
			'0' AS HENPINSTS, --返品ステータス
			C_DSPOINTITEMFLG, --ポイント交換商品フラグ
			C_DIITEMTYPE, --商品区分
			C_DIADJUSTPRC, --調整金額
			DITOTALPRC, --税込み額
			DIITEMTAX, --消費税額
			C_DIITEMTOTALPRC, --販売小計(税込)
			C_DIDISCOUNTMEISAI, --割引金額
			DISETMEISAIID, --親行取得用
			C_DSSETITEMKBN, --セット品区分
			MAKER --マーカー
			--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
			--DnA側でデータマートを作成するため廃止
			--, JOIN_REC_UPDDATE   --結合レコード更新日時(JJ連携の差分抽出に使用)
			--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		FROM KESAI_M_DATA_MART_SUB_N_U

),
union2 as( 
    SELECT SALENO, --売上No（KEY）
			GYONO, --行No
			MEISAIKBN, --明細区分
			ITEMCODE, --アイテムコード
			ITEMNAME, --商品名
			DIID, --商品内部ID
			DISETID, --セット商品内部ID
			SURYO, --数量
			TANKA, --単価
			KINGAKU, --金額
			MEISAINUKIKINGAKU, --明細税抜金額
			WARIRITU, --適用割引率
			WARIMAEKOMITANKA, --割引前税込単価
			WARIMAENUKIKINGAKU, --割引前明細金額
			WARIMAEKOMIKINGAKU, --割引前明細税込金額
			BUN_TANKA, -- 分解後単価
			BUN_KINGAKU, -- 分解後金額
			BUN_MEISAINUKIKINGAKU, -- 分解後明細税抜金額
			BUN_WARIRITU, -- 分解後適用割引率
			BUN_WARIMAEKOMITANKA, -- 分解後割引前税込単価
			BUN_WARIMAENUKIKINGAKU, -- 分解後割引前明細金額
			BUN_WARIMAEKOMIKINGAKU, -- 分解後割引前明細税込金額
			DISPSALENO, --売上No
			KESAIID, --決済ID
			DIORDERID, --受注内部ID
			HENPINSTS, --返品ステータス
			C_DSPOINTITEMFLG, --ポイント交換商品フラグ
			C_DIITEMTYPE, --商品区分
			C_DIADJUSTPRC, --調整金額
			DITOTALPRC, --税込み額
			DIITEMTAX, --消費税額
			C_DIITEMTOTALPRC, --販売小計(税込)
			C_DIDISCOUNTMEISAI, --割引金額
			DISETMEISAIID, --親行取得用
			C_DSSETITEMKBN, --セット品区分
			MAKER --マーカー
			--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
			--DnA側でデータマートを作成するため廃止
			--, JOIN_REC_UPDDATE   --結合レコード更新日時(JJ連携の差分抽出に使用)
			--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		FROM KESAI_M_DATA_MART_SUB_N_H
),
MEISAI_SUM as(
    select * from union1 
    union all
    select * from union2
),

union3 as(
SELECT SALENO, --売上No（KEY）
	GYONO, --行No
	MEISAIKBN, --明細区分
	ITEMCODE, --アイテムコード
	ITEMNAME, --商品名
	DIID, --商品内部ID
	DISETID, --セット商品内部ID
	SURYO, --数量
	TANKA, --単価
	KINGAKU, --金額
	MEISAINUKIKINGAKU, --明細税抜金額
	WARIRITU, --適用割引率
	WARIMAEKOMITANKA, --割引前税込単価
	WARIMAENUKIKINGAKU, --割引前明細金額
	WARIMAEKOMIKINGAKU, --割引前明細税込金額
	BUN_TANKA, -- 分解後単価
	BUN_KINGAKU, -- 分解後金額
	BUN_MEISAINUKIKINGAKU, -- 分解後明細税抜金額
	BUN_WARIRITU, -- 分解後適用割引率
	BUN_WARIMAEKOMITANKA, -- 分解後割引前税込単価
	BUN_WARIMAENUKIKINGAKU, -- 分解後割引前明細金額
	BUN_WARIMAEKOMIKINGAKU, -- 分解後割引前明細税込金額
	DISPSALENO, --売上No
	KESAIID, --決済ID
	DIORDERID, --受注内部ID
	HENPINSTS, --返品ステータス
	C_DSPOINTITEMFLG, --ポイント交換商品フラグ
	C_DIITEMTYPE, --商品区分
	C_DIADJUSTPRC, --調整金額
	DITOTALPRC, --税込み額
	DIITEMTAX, --消費税額
	C_DIITEMTOTALPRC, --販売小計(税込)
	C_DIDISCOUNTMEISAI, --割引金額
	DISETMEISAIID, --親行取得用
	C_DSSETITEMKBN, --セット品区分
	MAKER --マーカー
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--, JOIN_REC_UPDDATE   --結合レコード更新日時(JJ連携の差分抽出に使用)
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
FROM MEISAI_SUM
--差分用行

),
union4 as(

SELECT H_SUM.SALENO AS SALENO, --売上No（KEY）
	0 AS GYONO, --行No--行No
	'DUMMY' AS MEISAIKBN, --明細区分--明細区分
	'DUMMY' AS ITEMCODE, --アイテムコード--アイテムコード
	'調整行(ヘッダーと明細の差分)' AS ITEMNAME, --商品名--商品名
	'DUMMY' AS DIID ,--商品内部ID--商品内部ID
	'DUMMY' AS DISETID ,--セット商品内部ID--セット商品内部ID
	1 AS SURYO, --数量--数量
	H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS TANKA, --単価
	H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS KINGAKU, --金額
	--BGN-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
	--, H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS MEISAINUKIKINGAKU  --明細税抜金額
	H_SUM.SUM_H_KIN_NUKI - M_SUM.SUM_M_KIN_NUKI AS MEISAINUKIKINGAKU ,--明細税抜金額
	--END-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
	0 AS WARIRITU, --適用割引率
	H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS WARIMAEKOMITANKA, --割引前税込単価
	--BGN-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
	--, H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS WARIMAENUKIKINGAKU --割引前明細金額
	H_SUM.SUM_H_KIN_NUKI - M_SUM.SUM_M_KIN_NUKI AS WARIMAENUKIKINGAKU, --割引前明細金額
	--END-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
	H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS WARIMAEKOMIKINGAKU, --割引前明細税込金額
	H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS BUN_TANKA ,-- 分解後単価
	H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS BUN_KINGAKU ,-- 分解後金額
	--BGN-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
	--, H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS BUN_MEISAINUKIKINGAKU  -- 分解後明細税抜金額
	H_SUM.SUM_H_KIN_NUKI - M_SUM.SUM_M_KIN_NUKI AS BUN_MEISAINUKIKINGAKU, -- 分解後明細税抜金額
	--END-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
	H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS BUN_WARIRITU ,-- 分解後適用割引率
	H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS BUN_WARIMAEKOMITANKA ,-- 分解後割引前税込単価
	--BGN-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
	--, H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS BUN_WARIMAENUKIKINGAKU -- 分解後割引前明細金額
	H_SUM.SUM_H_KIN_NUKI - M_SUM.SUM_M_KIN_NUKI AS BUN_WARIMAENUKIKINGAKU, -- 分解後割引前明細金額
	--END-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
	H_SUM.SUM_H_KIN - M_SUM.SUM_M_KIN AS BUN_WARIMAEKOMIKINGAKU, -- 分解後割引前明細税込金額
	H_SUM.SALENO AS DISPSALENO, --売上No
	H_SUM.KESAIID AS KESAIID, --決済ID
	0 AS DIORDERID ,--受注内部ID
	'DUMMY' AS HENPINSTS, --返品ステータス
	'DUMMY' AS C_DSPOINTITEMFLG ,--ポイント交換商品フラグ
	'DUMMY' AS C_DIITEMTYPE, --商品区分
	0 AS C_DIADJUSTPRC, --調整金額
	0 AS DITOTALPRC, --税込み額
	0 AS DIITEMTAX, --消費税額
	0 AS C_DIITEMTOTALPRC ,--販売小計(税込)
	0 AS C_DIDISCOUNTMEISAI, --割引金額
	0 AS DISETMEISAIID ,--親行取得用
	'DUMMY' AS C_DSSETITEMKBN, --セット品区分
	0 AS MAKER --マーカー
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--, TO_NUMBER(GREATEST(
	--      NVL(H_SUM.JOIN_REC_UPDDATE,0)
	--    , NVL(M_SUM.JOIN_REC_UPDDATE,0)
	--))                               AS JOIN_REC_UPDDATE   --結合レコード更新日時(JJ連携の差分抽出に使用)
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
FROM --ヘッダーの集計金額
	(
	SELECT H.SALENO,
		H.KESAIID,
		--BGN-ADD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
		H.SOGOKEI - H.SORYO - H.DICOLLECTPRC - H.DITOUJITSUHAISOPRC - H.TAX AS SUM_H_KIN_NUKI,
		--END-ADD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
		H.SOGOKEI - H.SORYO - H.DICOLLECTPRC - H.DITOUJITSUHAISOPRC AS SUM_H_KIN
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--     , H.JOIN_REC_UPDDATE
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	FROM KESAI_H_DATA_MART_SUB_N H
	) H_SUM,
	--明細の集計金額
	(
		SELECT M.SALENO
			--BGN-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
			--         , SUM(TRUNC(DECODE(M.MEISAIKBN,'商品',M.MEISAINUKIKINGAKU,'特典',M.MEISAINUKIKINGAKU,0)/ M.SURYO * 1.08)*M.SURYO) + SUM(DECODE(M.MEISAIKBN,'商品',0,'特典',0,M.MEISAINUKIKINGAKU)) SUM_M_KIN
			,
			SUM(M.KINGAKU) SUM_M_KIN,
			SUM(M.MEISAINUKIKINGAKU) SUM_M_KIN_NUKI
		--END-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
		--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		--DnA側でデータマートを作成するため廃止
		--     , MAX(M.JOIN_REC_UPDDATE) AS JOIN_REC_UPDDATE
		--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
		FROM MEISAI_SUM M
		--BGN-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
		--    WHERE  M.SURYO <> 0
		--    AND    M.DIID = M.DISETID
		WHERE M.DIID = M.DISETID
		--END-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
		GROUP BY M.SALENO
		) M_SUM
WHERE H_SUM.SALENO = M_SUM.SALENO
	--BGN-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
	--AND   H_SUM.SUM_H_KIN <> M_SUM.SUM_M_KIN
	AND (
		H_SUM.SUM_H_KIN <> M_SUM.SUM_M_KIN
		OR H_SUM.SUM_H_KIN_NUKI <> M_SUM.SUM_M_KIN_NUKI
		)

--END-UPD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
--************************************************************
--* DWH事後移行：KESAI_M_DATA_MART_SUB_N
--*
--* 処理概要：
--*  DM023:決済・受注データマート_サブ_売上返品明細_CN
--*  CI-NEXTの売上返品明細
--* 変更履歴
--*  2018/06/08 井部 新規作成
--*  2020/01/08 山下 変更19855(JJ連携処理の追加)対応
--*  2020/06/15 山下 変更20940(調整行の税抜金額に税込金額が設定されている)
--************************************************************

),
transformed as(
    select * from union3
    union all
    select * from union4
),
final as(
    select 
       	saleno::varchar(63) as saleno,
        gyono::number(18,0) as gyono,
        meisaikbn::varchar(36) as meisaikbn,
        itemcode::varchar(60) as itemcode,
        itemname::varchar(192) as itemname,
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
        dispsaleno::varchar(63) as dispsaleno,
        kesaiid::varchar(62) as kesaiid,
        diorderid::number(18,0) as diorderid,
        henpinsts::varchar(8) as henpinsts,
        c_dspointitemflg::varchar(8) as c_dspointitemflg,
        c_diitemtype::varchar(8) as c_diitemtype,
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