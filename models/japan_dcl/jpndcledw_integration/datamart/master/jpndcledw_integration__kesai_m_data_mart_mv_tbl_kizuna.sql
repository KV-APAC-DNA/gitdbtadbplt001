{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
    )
}}

with KESAI_M_DATA_MART_SUB_OLD as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_M_DATA_MART_SUB_OLD
),
KESAI_M_DATA_MART_SUB_kizuna as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_M_DATA_MART_SUB_TBL_KIZUNA
),
KESAI_H_DATA_MART_SUB_kizuna as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_H_DATA_MART_SUB_KIZUNA
),
KESAI_M_DATA_MART_SUB_OLD_CHSI as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_M_DATA_MART_SUB_OLD_CHSI_TBL
),

union1 as(

SELECT nvl(SALENO, '') AS SALENO_KEY,
	SALENO
	--END MOD 20190416 takano ***課題0040***
	,
	GYONO,
	BUN_GYONO,
	MEISAIKBN,
	ITEMCODE --分解前のアイテムコード
	,
	NULL AS SETITEMNM --分解前商品名
	,
	BUN_ITEMCODE --分解後のアイテムコード
	,
	'DUMMY' AS DIID,
	'DUMMY' AS DISETID,
	SURYO,
	TANKA,
	KINGAKU,
	MEISAINUKIKINGAKU,
	WARIRITU,
	HENSU,
	WARIMAEKOMITANKA,
	WARIMAEKOMIKINGAKU,
	WARIMAENUKIKINGAKU,
	MEISAITAX,
	DISPSALENO,
	KESAIID,
	SALENO_TRIM,
	0 AS DIORDERID,
	NULL AS HENPINSTS,
	NULL AS C_DSPOINTITEMFLG,
	NULL AS C_DIITEMTYPE,
	0 AS C_DIADJUSTPRC,
	0 AS DITOTALPRC,
	0 AS C_DIITEMTOTALPRC,
	0 AS C_DIDISCOUNTMEISAI,
	BUN_SURYO,
	BUN_TANKA,
	BUN_KINGAKU,
	BUN_MEISAINUKIKINGAKU,
	BUN_WARIRITU,
	BUN_HENSU,
	BUN_WARIMAEKOMITANKA,
	BUN_WARIMAEKOMIKINGAKU,
	BUN_WARIMAENUKIKINGAKU,
	BUN_MEISAITAX
	-- delete by Kizuna Project 2022/11/25 start --
	--, CI_PORT_FLG
	-- delete by Kizuna Project 2022/11/25 end --
	,
	0 AS MAKER
	-- delete by Kizuna Project 2022/11/25 start --
	--, SALENO                         AS SALENO_P
	--, GYONO_P
	--, BUN_GYONO_P
	--, MEISAIKBN_P
	--, BUN_ITEMCODE_P
	--, BUN_ITEMCODE_P                 AS ITEMCODE_HANBAI_P
	-- delete by Kizuna Project 2022/11/25 end --
	,
	'1' AS KAKOKBN
-- delete by Kizuna Project 2022/11/25 end --
--, BUN_SURYO_P                    AS SURYO_P
--, BUN_TANKA_P                    AS TANKA_P
--, BUN_HENSU_P                    AS HENSU_P
--, BUN_HENUSU                     AS HENUSU_P
--, JYU_SURYO                      AS JYU_SURYO
--, Null                             AS OYAFLG_P
--, 0                              AS JUCHGYONO_P
--, Null                             AS DISPSALENO_P
--, CAST(JUCH_SHUR AS VARCHAR)             AS JUCH_SHUR_P
--, TYOSEIKIKINGAKU                AS TYOSEIKIKINGAKU_P
--, 0                              AS DEN_NEBIKI_ABN_KIN_P
--, 0                              AS DEN_NB_AB_SZ_KIN_P
--, Null                             AS DCLSM_HIN_HIN_NIBU_ID_P
----Ci-Port一致用
--, BUN_KINGAKU                AS KINGAKU_P                 --Ci-Port一致用
--, BUN_MEISAINUKIKINGAKU      AS MEISAINUKIKINGAKU_P       --Ci-Port一致用
--, BUN_MEISAITAX              AS MEISAITAX_P               --Ci-Port一致用
----BGN MOD 20190409 takano ***課題0027***
----     , BUN_MEISAINUKIKINGAKU AS ANBUNMEISAINUKIKINGAKU_P  --Ci-Port一致用
--, BUN_MEISAINUKIKINGAKU_P AS ANBUNMEISAINUKIKINGAKU_P  --Ci-Port一致用
----END MOD 20190409 takano ***課題0027***
--, BUN_KINGAKU           AS KINGAKU_TUKA_P            --Ci-Port一致用
--, BUN_MEISAINUKIKINGAKU AS MEISAINUKIKINGAKU_TUKA_P  --Ci-Port一致用
--, BUN_MEISAITAX         AS MEISAITAX_TUKA_P          --Ci-Port一致用
--, ANBN_BN_KINGAKU + HASUU_KINGAKU AS HASUU_KINGAKU
--, ANBN_BN_MEISAINUKIKINGAKU + HASUU_MEISAINUKIKINGAKU AS HASUU_MEISAINUKIKINGAKU
--, ANBN_BN_MEISAITAX + HASUU_MEISAITAX AS HASUU_MEISAITAX
--, ANBN_BN_ANBUNMEISAINUKIKINGAKU + HASUU_ANBUNMEISAINUKIKINGAKU AS HASUU_ANBUNMEISAINUKIKINGAKU
--, ANBN_BN_KINGAKU_TUKA + HASUU_KINGAKU_TUKA AS HASUU_KINGAKU_TUKA
--, ANBN_BN_MEISAINUKIKINGAKU_TUKA + HASUU_MEISAINUKIKINGAKU_TUKA AS HASUU_MEISAINUKIKINGAKU_TUKA
--, ANBN_BN_MEISAITAX_TUKA + HASUU_MEISAITAX_TUKA AS HASUU_MEISAINUKIKINGAKU_TUKA
--, HAS_BN_KINGAKU                 AS HAS_BN_KINGAKU                 --Ci-Port一致用(端数計算後) 
--, HAS_BN_MEISAINUKIKINGAKU       AS HAS_BN_MEISAINUKIKINGAKU       --Ci-Port一致用(端数計算後) 
--, HAS_BN_MEISAITAX               AS HAS_BN_MEISAITAX               --Ci-Port一致用(端数計算後) 
--, HAS_BN_ANBUNMEISAINUKIKINGAKU  AS HAS_BN_ANBUNMEISAINUKIKINGAKU  --Ci-Port一致用(端数計算後) 
--, HAS_BN_KINGAKU_TUKA            AS HAS_BN_KINGAKU_TUKA            --Ci-Port一致用(端数計算後)
--, HAS_BN_MEISAINUKIKINGAKU_TUKA  AS HAS_BN_MEISAINUKIKINGAKU_TUKA  --Ci-Port一致用(端数計算後)
--, HAS_BN_MEISAITAX_TUKA          AS HAS_BN_MEISAITAX_TUKA          --Ci-Port一致用(端数計算後)
--, 0                              AS MARKER_P
--, Null                             AS URI_HEN_KBN_P
--, Null                             AS SAL_JISK_IMP_SNSH_NO_P
--, Null                             AS DCLJUCH_ID_P 
----BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
----DnA側でデータマートを作成するため廃止
----, 0                            AS JOIN_REC_UPDDATE               --結合レコード更新日時(JJ連携の差分抽出に使用)
----END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
-- delete by Kizuna Project 2022/11/25 end --
FROM KESAI_M_DATA_MART_SUB_OLD SUB_OLD
--limit 10
--◆SELECT ② 次期データ
--Ci-Nextデータ

),
union2 as( 

SELECT
	-- udpate by Kizuna Project 2022/11/25 start --
	--cast((nvl(SUB_NEW.SALENO,'')||nvl(SUB_NEW.SALENO_P,'')) as varchar)     AS SALENO_KEY
	cast((nvl(SUB_NEW.SALENO, '')) AS VARCHAR) AS SALENO_KEY
	-- udpate by Kizuna Project 2022/11/25 end --
	,
	SUB_NEW.SALENO
	--END MOD 20190416 takano ***課題0040***
	,
	SUB_NEW.GYONO,
	SUB_NEW.GYONO AS BUN_GYONO --分解後行番号
	,
	SUB_NEW.MEISAIKBN,
	SUB_NEW.SETITEMCD AS ITEMCODE --分解前のアイテムコード
	,
	SUB_NEW.SETITEMNM,
	SUB_NEW.ITEMCODE AS BUN_ITEMCODE --分解後のアイテムコード
	,
	SUB_NEW.DIID,
	SUB_NEW.DISETID
	--セット品と商品の一致する場合の項目
	,
	CASE 
		WHEN SUB_NEW.DISETID = SUB_NEW.DIID
			THEN --セット品と商品の一致する
				CASE 
					WHEN (
							NEWH.JUCHKBN <> '90'
							AND NEWH.JUCHKBN <> '91'
							AND NEWH.JUCHKBN <> '92'
							)
						THEN SUB_NEW.SURYO
					ELSE 0
					END
		ELSE 0
		END SURYO,
	CASE 
		WHEN SUB_NEW.DISETID = SUB_NEW.DIID
			THEN --セット品と商品の一致する
				SUB_NEW.TANKA
		ELSE 0
		END TANKA,
	CASE 
		WHEN SUB_NEW.DISETID = SUB_NEW.DIID
			THEN --セット品と商品の一致する
				SUB_NEW.KINGAKU
		ELSE 0
		END KINGAKU,
	CASE 
		WHEN SUB_NEW.DISETID = SUB_NEW.DIID
			THEN --セット品と商品の一致する
				SUB_NEW.MEISAINUKIKINGAKU
		ELSE 0
		END MEISAINUKIKINGAKU,
	CASE 
		WHEN SUB_NEW.DISETID = SUB_NEW.DIID
			THEN --セット品と商品の一致する
				SUB_NEW.WARIRITU
		ELSE 0
		END WARIRITU,
	CASE 
		WHEN SUB_NEW.DISETID = SUB_NEW.DIID
			THEN --セット品と商品の一致する
				CASE 
					WHEN (
							NEWH.JUCHKBN <> '0'
							AND NEWH.JUCHKBN <> '1'
							AND NEWH.JUCHKBN <> '2'
							)
						THEN - 1 * SUB_NEW.SURYO
					ELSE 0
					END
		ELSE 0
		END HENSU,
	CASE 
		WHEN SUB_NEW.DISETID = SUB_NEW.DIID
			THEN --セット品と商品の一致する
				SUB_NEW.WARIMAEKOMITANKA
		ELSE 0
		END WARIMAEKOMITANKA,
	CASE 
		WHEN SUB_NEW.DISETID = SUB_NEW.DIID
			THEN --セット品と商品の一致する
				SUB_NEW.WARIMAEKOMIKINGAKU
		ELSE 0
		END WARIMAEKOMIKINGAKU,
	CASE 
		WHEN SUB_NEW.DISETID = SUB_NEW.DIID
			THEN --セット品と商品の一致する
				SUB_NEW.WARIMAENUKIKINGAKU
		ELSE 0
		END WARIMAENUKIKINGAKU,
	CASE 
		WHEN SUB_NEW.DISETID = SUB_NEW.DIID
			THEN --セット品と商品の一致する
				SUB_NEW.DIITEMTAX
		ELSE 0
		END MEISAITAX,
	SUB_NEW.DISPSALENO,
	CAST(SUB_NEW.KESAIID AS VARCHAR) AS KESAIID,
	TRIM(SUB_NEW.SALENO) AS SALENO_TRIM,
	SUB_NEW.DIORDERID,
	SUB_NEW.HENPINSTS,
	SUB_NEW.C_DSPOINTITEMFLG,
	SUB_NEW.C_DIITEMTYPE,
	SUB_NEW.C_DIADJUSTPRC,
	SUB_NEW.DITOTALPRC,
	SUB_NEW.C_DIITEMTOTALPRC,
	SUB_NEW.C_DIDISCOUNTMEISAI
	--セット品と商品の一致しない場合の項目
	,
	CASE 
		WHEN (
				SUB_NEW.DISETID = SUB_NEW.DIID
				AND SUB_NEW.C_DSSETITEMKBN <> '1'
				)
			OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
			THEN CASE 
					WHEN (
							NEWH.JUCHKBN <> '90'
							AND NEWH.JUCHKBN <> '91'
							AND NEWH.JUCHKBN <> '92'
							)
						THEN SUB_NEW.SURYO
					ELSE 0
					END
		ELSE 0
		END BUN_SURYO,
	CASE 
		WHEN (
				SUB_NEW.DISETID = SUB_NEW.DIID
				AND SUB_NEW.C_DSSETITEMKBN <> '1'
				)
			OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
			THEN SUB_NEW.BUN_TANKA
		ELSE 0
		END BUN_TANKA,
	CASE 
		WHEN (
				SUB_NEW.DISETID = SUB_NEW.DIID
				AND SUB_NEW.C_DSSETITEMKBN <> '1'
				)
			OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
			THEN SUB_NEW.BUN_KINGAKU
		ELSE 0
		END BUN_KINGAKU,
	CASE 
		WHEN (
				SUB_NEW.DISETID = SUB_NEW.DIID
				AND SUB_NEW.C_DSSETITEMKBN <> '1'
				)
			OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
			THEN SUB_NEW.BUN_MEISAINUKIKINGAKU
		ELSE 0
		END BUN_MEISAINUKIKINGAKU,
	CASE 
		WHEN (
				SUB_NEW.DISETID = SUB_NEW.DIID
				AND SUB_NEW.C_DSSETITEMKBN <> '1'
				)
			OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
			THEN SUB_NEW.BUN_WARIRITU
		ELSE 0
		END BUN_WARIRITU,
	CASE 
		WHEN (
				SUB_NEW.DISETID = SUB_NEW.DIID
				AND SUB_NEW.C_DSSETITEMKBN <> '1'
				)
			OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
			THEN CASE 
					WHEN (
							NEWH.JUCHKBN <> '0'
							AND NEWH.JUCHKBN <> '1'
							AND NEWH.JUCHKBN <> '2'
							)
						THEN - 1 * SUB_NEW.SURYO
					ELSE 0
					END
		ELSE 0
		END BUN_HENSU,
	CASE 
		WHEN (
				SUB_NEW.DISETID = SUB_NEW.DIID
				AND SUB_NEW.C_DSSETITEMKBN <> '1'
				)
			OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
			THEN SUB_NEW.BUN_WARIMAEKOMITANKA
		ELSE 0
		END BUN_WARIMAEKOMITANKA,
	CASE 
		WHEN (
				SUB_NEW.DISETID = SUB_NEW.DIID
				AND SUB_NEW.C_DSSETITEMKBN <> '1'
				)
			OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
			THEN SUB_NEW.BUN_WARIMAEKOMIKINGAKU
		ELSE 0
		END BUN_WARIMAEKOMIKINGAKU,
	CASE 
		WHEN (
				SUB_NEW.DISETID = SUB_NEW.DIID
				AND SUB_NEW.C_DSSETITEMKBN <> '1'
				)
			OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
			THEN SUB_NEW.BUN_WARIMAENUKIKINGAKU
		ELSE 0
		END BUN_WARIMAENUKIKINGAKU,
	CASE 
		WHEN (
				SUB_NEW.DISETID = SUB_NEW.DIID
				AND SUB_NEW.C_DSSETITEMKBN <> '1'
				)
			OR (SUB_NEW.DISETID <> SUB_NEW.DIID)
			THEN SUB_NEW.DIITEMTAX
		ELSE 0
		END BUN_MEISAITAX
	-- delete by Kizuna Project 2022/11/25 start --
	--,CASE SUB_NEW.MEISAIKBN
	--    WHEN '商品' THEN '1'
	--    WHEN '特典' THEN '1'
	--    WHEN '送料' THEN '1'
	--    ELSE             '0'
	--END AS CI_PORT_FLG
	-- delete by Kizuna Project 2022/11/25 end --
	,
	SUB_NEW.MAKER
	-- delete by Kizuna Project 2022/11/25 start --
	--, SUB_NEW.SALENO_P
	--, SUB_NEW.GYONO_P
	--, SUB_NEW.GYONO_P    AS BUN_GYONO_P--分解後行番号
	--, Null             AS MEISAIKBN_P
	--, SUB_NEW.ITEMCODE_P
	--, SUB_NEW.ITEMCODE_HANBAI_P
	-- delete by Kizuna Project 2022/11/25 end --
	,
	'0' AS KAKOKBN
-- delete by Kizuna Project 2022/11/25 start --
--, SUB_NEW.SURYO_P    AS SURYO_P
--, SUB_NEW.TANKA_P    AS TANKA_P
--, SUB_NEW.HENSU_P    AS HENSU_P
--, SUB_NEW.HENSU_P    AS HENUSU_P
--, SUB_NEW.JYU_SURYO_P
--, SUB_NEW.OYAFLG_P
--, SUB_NEW.JUCHGYONO_P
--, SUB_NEW.DISPSALENO_P
--, SUB_NEW.JUCH_SHUR_P
--, SUB_NEW.TYOSEIKIKINGAKU_P
--, SUB_NEW.DEN_NEBIKI_ABN_KIN_P
--, SUB_NEW.DEN_NB_AB_SZ_KIN_P
--, SUB_NEW.DCLSM_HIN_HIN_NIBU_ID_P
----Ci-Port一致用
--, SUB_NEW.KINGAKU_P                AS KINGAKU_P                 --Ci-Port一致用
--, SUB_NEW.MEISAINUKIKINGAKU_P      AS MEISAINUKIKINGAKU_P       --Ci-Port一致用
--, SUB_NEW.MEISAITAX_P              AS MEISAITAX_P               --Ci-Port一致用
--, SUB_NEW.ANBUNMEISAINUKIKINGAKU_P AS ANBUNMEISAINUKIKINGAKU_P  --Ci-Port一致用
--, SUB_NEW.KINGAKU_TUKA_P           AS KINGAKU_TUKA_P            --Ci-Port一致用
--, SUB_NEW.MEISAINUKIKINGAKU_TUKA_P AS MEISAINUKIKINGAKU_TUKA_P  --Ci-Port一致用
--, SUB_NEW.MEISAITAX_TUKA_P         AS MEISAITAX_TUKA_P          --Ci-Port一致用
--, 0                            AS HASUU_KINGAKU
--, 0                            AS HASUU_MEISAINUKIKINGAKU
--, 0                            AS HASUU_MEISAITAX
--, 0                            AS HASUU_ANBUNMEISAINUKIKINGAKU
--, 0                            AS HASUU_KINGAKU_TUKA
--, 0                            AS HASUU_MEISAINUKIKINGAKU_TUKA
--, 0                            AS HASUU_MEISAITAX_TUKA
--, SUB_NEW.KINGAKU_P                AS HAS_BN_KINGAKU                 --Ci-Port一致用(端数計算後) 
--, SUB_NEW.MEISAINUKIKINGAKU_P      AS HAS_BN_MEISAINUKIKINGAKU       --Ci-Port一致用(端数計算後) 
--, SUB_NEW.MEISAITAX_P              AS HAS_BN_MEISAITAX               --Ci-Port一致用(端数計算後)
--, SUB_NEW.ANBUNMEISAINUKIKINGAKU_P AS HAS_BN_ANBUNMEISAINUKIKINGAKU  --Ci-Port一致用(端数計算後) 
--, CAST ((SUB_NEW.KINGAKU_TUKA_P ) AS NUMERIC)         AS HAS_BN_KINGAKU_TUKA            --Ci-Port一致用(端数計算後)   --- issue
--, CAST((SUB_NEW.MEISAINUKIKINGAKU_TUKA_P) as numeric) AS HAS_BN_MEISAINUKIKINGAKU_TUKA  --Ci-Port一致用(端数計算後) --- issue
--, Cast((SUB_NEW.MEISAITAX_TUKA_P) as Numeric)         AS HAS_BN_MEISAITAX_TUKA          --Ci-Port一致用(端数計算後) ---- issue
--, Cast((SUB_NEW.MARKER_P) as Numeric)
--, SUB_NEW.URI_HEN_KBN_P
--, SUB_NEW.SAL_JISK_IMP_SNSH_NO_P
--, SUB_NEW.DCLJUCH_ID_P
----BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
----DnA側でデータマートを作成するため廃止
----, TO_NUMBER(GREATEST(
----      NVL(SUB_NEW.JOIN_REC_UPDDATE,0)
----    , NVL(NEWH.JOIN_REC_UPDDATE,0)
----))                          AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
----END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
-- delete by Kizuna Project 2022/11/25 end --
FROM KESAI_M_DATA_MART_SUB_kizuna SUB_NEW
INNER JOIN (
	SELECT SALENO,
		MIN(JUCHKBN) JUCHKBN
	--BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	--DnA側でデータマートを作成するため廃止
	--   ,      MAX(JOIN_REC_UPDDATE) JOIN_REC_UPDDATE
	--END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
	FROM KESAI_H_DATA_MART_SUB_kizuna
	GROUP BY SALENO
	) NEWH ON trim(SUB_NEW.SALENO) = trim(NEWH.SALENO)
-- delete by Kizuna Project 2022/11/25 start --
--LEFT   JOIN KESAI_M_DATA_MART_SUB_N_WARI WARI --割引額按分用
--ON     SUB_NEW.SALENO = WARI.SALENO
--AND    SUB_NEW.GYONO  = WARI.GYONO
--BGN-ADD 20200615 D.YAMASHITA ***変更20940(調整行の税抜金額に税込金額が設定されている)****
-- delete by Kizuna Project 2022/11/25 end --




),

union3 as(


SELECT nvl(OLD_CHSI.SALENO, '') || '調整行DUMMY' AS SALENO_KEY,
	OLD_CHSI.SALENO AS SALENO,
	0 AS GYONO,
	0 AS BUN_GYONO,
	'DUMMY' AS MEISAIKBN,
	'DUMMY' AS ITEMCODE,
	'調整行(ヘッダーと明細の差分)' AS SETITEMNM,
	'DUMMY' AS BUN_ITEMCODE,
	'DUMMY' AS DIID,
	'DUMMY' AS DISETID,
	1 AS SURYO,
	OLD_CHSI.KINGAKU AS TANKA --単価
	,
	OLD_CHSI.KINGAKU AS KINGAKU --金額
	,
	OLD_CHSI.MEISAINUKIKINGAKU AS MEISAINUKIKINGAKU,
	0 AS WARIRITU --適用割引率
	,
	0 AS HENSU,
	OLD_CHSI.KINGAKU AS WARIMAEKOMITANKA --割引前税込単価
	,
	OLD_CHSI.KINGAKU AS WARIMAEKOMIKINGAKU --割引前明細税込金額
	,
	OLD_CHSI.MEISAINUKIKINGAKU AS WARIMAENUKIKINGAKU --割引前明細金額 ,
	,
	0 AS MEISAITAX,
	OLD_CHSI.SALENO AS DISPSALENO --売上No
	,
	OLD_CHSI.KESAIID AS KESAIID --決済ID
	,
	TRIM(OLD_CHSI.SALENO) AS SALENO_TRIM,
	0 AS DIORDERID,
	'DUMMY' AS HENPINSTS,
	'DUMMY' AS C_DSPOINTITEMFLG,
	'DUMMY' AS C_DIITEMTYPE,
	0 AS C_DIADJUSTPRC,
	0 AS DITOTALPRC,
	0 AS C_DIITEMTOTALPRC,
	0 AS C_DIDISCOUNTMEISAI,
	1 AS BUN_SURYO,
	OLD_CHSI.KINGAKU AS BUN_TANKA -- 分解後単価
	,
	OLD_CHSI.KINGAKU AS BUN_KINGAKU -- 分解後金額
	,
	OLD_CHSI.MEISAINUKIKINGAKU AS BUN_MEISAINUKIKINGAKU -- 分解後明細税抜金額
	,
	OLD_CHSI.KINGAKU AS BUN_WARIRITU -- 分解後適用割引率
	,
	0 AS BUN_HENSU,
	OLD_CHSI.KINGAKU AS BUN_WARIMAEKOMITANKA -- 分解後割引前税込単価
	,
	OLD_CHSI.KINGAKU AS BUN_WARIMAEKOMIKINGAKU -- 分解後割引前明細税込金額
	,
	OLD_CHSI.MEISAINUKIKINGAKU AS BUN_WARIMAENUKIKINGAKU -- 分解後割引前明細金額
	,
	0 AS BUN_MEISAITAX
	-- delete by Kizuna Project 2022/11/25 start --
	--, '0'                              AS CI_PORT_FLG
	-- delete by Kizuna Project 2022/11/25 end --
	,
	0 AS MAKER
	-- delete by Kizuna Project 2022/11/25 start --
	--, '調整行DUMMY'                    AS SALENO_P
	--, CAST((NULL) As Numeric)                             AS GYONO_P
	--, CAST((NULL) As Numeric)                             AS BUN_GYONO_P
	--, NULL                             AS MEISAIKBN_P
	--, NULL                             AS BUN_ITEMCODE_P
	--, NULL                             AS ITEMCODE_HANBAI_P
	-- delete by Kizuna Project 2022/11/25 end --
	,
	'1' AS KAKOKBN
-- delete by Kizuna Project 2022/11/25 start --
--, CAST((NULL) As Numeric)                             AS SURYO_P
--, CAST((NULL) As Numeric)                             AS TANKA_P
--, CAST((NULL) As Numeric)                             AS HENSU_P
--, CAST((NULL) As Numeric)                             AS HENUSU_P
--, CAST((NULL) As Numeric)                             AS JYU_SURYO
--, NULL                             AS OYAFLG_P
--, CAST((NULL) As Numeric)                             AS JUCHGYONO_P
--, NULL                             AS DISPSALENO_P
--, NULL                             AS JUCH_SHUR_P
--, CAST((NULL) As Numeric)                             AS TYOSEIKIKINGAKU_P
--, CAST((NULL) As Numeric)                             AS DEN_NEBIKI_ABN_KIN_P
--, CAST((NULL) As Numeric)                             AS DEN_NB_AB_SZ_KIN_P
--, NULL                             AS DCLSM_HIN_HIN_NIBU_ID_P
--, CAST((NULL) As Numeric)                             AS KINGAKU_P
--, CAST((NULL) As Numeric)                             AS MEISAINUKIKINGAKU_P
--, CAST((NULL) As Numeric)                             AS MEISAITAX_P
--, CAST((NULL) As Numeric)                             AS ANBUNMEISAINUKIKINGAKU_P
--, CAST((NULL) As Numeric)                             AS KINGAKU_TUKA_P
--, CAST((NULL) As Numeric)                             AS MEISAINUKIKINGAKU_TUKA_P
--, CAST((NULL) As Numeric)                             AS MEISAITAX_TUKA_P
--, 0                                AS HASUU_KINGAKU
--, 0                                AS HASUU_MEISAINUKIKINGAKU
--, 0                                AS HASUU_MEISAITAX
--, 0                                AS HASUU_ANBUNMEISAINUKIKINGAKU
--, 0                                AS HASUU_KINGAKU_TUKA
--, 0                                AS HASUU_MEISAINUKIKINGAKU_TUKA
--, 0                                AS HASUU_MEISAITAX_TUKA
--, CAST((NULL) As Numeric)                             AS HAS_BN_KINGAKU 
--, CAST((NULL) As Numeric)                             AS HAS_BN_MEISAINUKIKINGAKU 
--, CAST((NULL) As Numeric)                            AS HAS_BN_MEISAITAX 
--, CAST((NULL) As Numeric)                            AS HAS_BN_ANBUNMEISAINUKIKINGAKU
--, CAST((NULL) As Numeric)                             AS HAS_BN_KINGAKU_TUKA                  -- issue
--, Cast((NULL) as Numeric)                           AS HAS_BN_MEISAINUKIKINGAKU_TUKA ---- issue
--, Cast((NULL) as Numeric)                            AS HAS_BN_MEISAITAX_TUKA     ----- issue
--, CAST((NULL) AS Numeric)                             AS MARKER_P
--, NULL                             AS URI_HEN_KBN_P
--, NULL                             AS SAL_JISK_IMP_SNSH_NO_P
--, NULL                             AS DCLJUCH_ID_P 
----BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
----DnA側でデータマートを作成するため廃止
----, 0                                AS JOIN_REC_UPDDATE
----END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
-- delete by Kizuna Project 2022/11/25 end --
FROM KESAI_M_DATA_MART_SUB_OLD_CHSI OLD_CHSI --過去データ用調整行格納MV



),
transformed as(
    select * from union1
    union all
    select * from union2
    union all
    select * from union3
),
final as(
    select 
        saleno_key::varchar(83) as saleno_key,
        saleno::varchar(63) as saleno,
        gyono::number(18,0) as gyono,
        bun_gyono::number(18,0) as bun_gyono,
        meisaikbn::varchar(36) as meisaikbn,
        itemcode::varchar(30) as itemcode,
        setitemnm::varchar(192) as setitemnm,
        bun_itemcode::varchar(60) as bun_itemcode,
        diid::varchar(60) as diid,
        disetid::varchar(60) as disetid,
        suryo::number(18,0) as suryo,
        tanka::number(18,0) as tanka,
        kingaku::number(18,0) as kingaku,
        meisainukikingaku::number(18,0) as meisainukikingaku,
        wariritu::number(18,0) as wariritu,
        hensu::number(18,0) as hensu,
        warimaekomitanka::number(18,0) as warimaekomitanka,
        warimaekomikingaku::number(18,0) as warimaekomikingaku,
        warimaenukikingaku::number(18,0) as warimaenukikingaku,
        meisaitax::number(18,0) as meisaitax,
        dispsaleno::varchar(63) as dispsaleno,
        kesaiid::varchar(62) as kesaiid,
        saleno_trim::varchar(63) as saleno_trim,
        diorderid::number(18,0) as diorderid,
        henpinsts::varchar(8) as henpinsts,
        c_dspointitemflg::varchar(8) as c_dspointitemflg,
        c_diitemtype::varchar(8) as c_diitemtype,
        c_diadjustprc::number(18,0) as c_diadjustprc,
        ditotalprc::number(18,0) as ditotalprc,
        c_diitemtotalprc::number(18,0) as c_diitemtotalprc,
        c_didiscountmeisai::number(18,0) as c_didiscountmeisai,
        bun_suryo::number(18,0) as bun_suryo,
        bun_tanka::number(18,0) as bun_tanka,
        bun_kingaku::number(18,0) as bun_kingaku,
        bun_meisainukikingaku::number(18,0) as bun_meisainukikingaku,
        bun_wariritu::number(18,0) as bun_wariritu,
        bun_hensu::number(18,0) as bun_hensu,
        bun_warimaekomitanka::number(18,0) as bun_warimaekomitanka,
        bun_warimaekomikingaku::number(18,0) as bun_warimaekomikingaku,
        bun_warimaenukikingaku::number(18,0) as bun_warimaenukikingaku,
        bun_meisaitax::number(18,0) as bun_meisaitax,
        maker::number(18,0) as maker,
        kakokbn::varchar(2) as kakokbn,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by ,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final