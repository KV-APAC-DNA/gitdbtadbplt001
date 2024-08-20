{{
    config(sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";")
}}

WITH tm14shkos
AS (
	SELECT *
	FROM {{ source('jpdcledw_integration', 'tm14shkos') }}
	),
cim08shkos_bunkai
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cim08shkos_bunkai') }}
	),
hanyo_attr
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__hanyo_attr') }}
	),
zaiko_shohin_attr
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__zaiko_shohin_attr') }}
	),
tm67juch_nm
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__tm67juch_nm') }}
	),
syouhincd_henkan
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__syouhincd_henkan') }}
	),
get_ci_next_sale
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__get_ci_next_sale') }}
	),
cit86osalm_kaigai
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cit86osalm_kaigai') }}
	),
cit86osalm
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cit86osalm') }}
	),
cit85osalh_kaigai
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cit85osalh_kaigai') }}
	),
cit85osalh
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cit85osalh') }}
	),
cit81salem
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cit81salem') }}
	),
cit80saleh
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cit80saleh') }}
	),
cim24itbun
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cim24itbun') }}
	),
cim03item_zaiko
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cim03item_zaiko') }}
	),
cim02tokui
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cim02tokui') }}
	),
cim03item_hanbai
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cim03item_hanbai') }}
	),
item_data_mart_mv
AS (
	SELECT *
	FROM {{ source('jpdcledw_integration', 'item_data_mart_mv') }}
	),
"wqtm07属性未設定名称マスタ"
AS (
	SELECT *
	FROM {{ source('jpdcledw_integration', '"wqtm07属性未設定名称マスタ"') }}
	),
ssmthdclthanjuchhedda
AS (
	SELECT *
	FROM {{ source('jpdclitg_integration', 'ssmthdclthanjuchhedda') }}
	),
ssmtbsalmei
AS (
	SELECT *
	FROM {{ source('jpdclitg_integration', 'ssmtbsalmei') }} 
	),
tbecitem
AS (
	SELECT *
	FROM {{ ref('jpndclitg_integration__tbecitem') }}
	),
c_tbecclient
AS (
	SELECT *
	FROM {{ ref('jpndclitg_integration__c_tbecclient') }}
	),
ssmthsalhedda
AS (
	SELECT *
	FROM {{ ref('jpndclitg_integration__ssmthsalhedda') }}
	),
ct1
AS (
	SELECT h.saleno,
		h.shukadate AS shukanengetubi,
		CASE 
			WHEN ((h.kaisha)::TEXT = ('000'::CHARACTER VARYING)::TEXT)
				THEN CASE 
						WHEN ((h.daihanrobunname)::TEXT = ('Web'::CHARACTER VARYING)::TEXT)
							THEN '121'::CHARACTER VARYING
						ELSE '111'::CHARACTER VARYING
						END
			WHEN ((h.kaisha)::TEXT = ('001'::CHARACTER VARYING)::TEXT)
				THEN '112'::CHARACTER VARYING
			ELSE '114'::CHARACTER VARYING
			END AS channel,
		h.daihanrobunname AS konyuchubuncode,
		h.torikeikbn,
		'ダミーコード'::CHARACTER VARYING AS tokuicode,
		m.itemcode_hanbai AS itemcode,
		"k".kosecode,
		h.juchkbn,
		((m.suryo * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0))) AS suryo,
		CASE 
			WHEN (
					(substring((h.juchkbn)::TEXT, 1, 1) = ('9'::CHARACTER VARYING)::TEXT)
					OR (
						(substring((h.juchkbn)::TEXT, 1, 1) IS NULL)
						AND ('9' IS NULL)
						)
					)
				THEN ((m.hensu * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
			END AS hensu,
		0 AS anbunmaegaku_tyouseimae,
		0 AS meisainukikingaku_tyouseimae,
		m.meisainukikingaku AS anbunmaegaku_tyouseigo,
		(((m.meisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION) AS meisainukikingaku_tyouseigo,
		(((m.anbunmeisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION) AS anbunmeisainukikingaku,
		CASE 
			WHEN (h.riyopoint = ((0)::NUMERIC)::NUMERIC(18, 0))
				THEN (0)::DOUBLE PRECISION
			WHEN (h.nukikingaku = ((0)::NUMERIC)::NUMERIC(18, 0))
				THEN (0)::DOUBLE PRECISION
			ELSE (
					(
						(
							(
								(
									(
										CASE 
											WHEN (h.shukadate >= ((20191001)::NUMERIC)::NUMERIC(18, 0))
												THEN 0.90
											WHEN (h.shukadate >= ((20140401)::NUMERIC)::NUMERIC(18, 0))
												THEN 0.92
											WHEN (h.shukadate >= ((19970401)::NUMERIC)::NUMERIC(18, 0))
												THEN 0.95
											WHEN (h.shukadate >= ((19890401)::NUMERIC)::NUMERIC(18, 0))
												THEN 0.97
											ELSE ((1)::NUMERIC)::NUMERIC(18, 0)
											END * h.riyopoint
										) * m.anbunmeisainukikingaku
									)
								)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)
							) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION
						) / ((h.nukikingaku + (h.riyopoint * (- ((1)::NUMERIC)::NUMERIC(18, 0)))))::DOUBLE PRECISION
					)
			END AS anbun_riyopoint_notax,
		CASE 
			WHEN (h.riyopoint = ((0)::NUMERIC)::NUMERIC(18, 0))
				THEN (0)::DOUBLE PRECISION
			WHEN (h.nukikingaku = ((0)::NUMERIC)::NUMERIC(18, 0))
				THEN (0)::DOUBLE PRECISION
			ELSE (((((h.riyopoint * m.anbunmeisainukikingaku))::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION) / ((h.nukikingaku + (h.riyopoint * (- ((1)::NUMERIC)::NUMERIC(18, 0)))))::DOUBLE PRECISION)
			END AS anbun_riyopoint_taxkomi,
		h.henreasoncode,
		h.henreasonname,
		''::CHARACTER VARYING AS sokoname,
		h.bmn_hyouji_cd,
		h.bmn_nms,
		tbecitem.dsoption002 AS hanbaiitem_kubun,
		CASE 
			WHEN ((tbecitem.dsoption002)::TEXT = ('ピッキング対応'::CHARACTER VARYING)::TEXT)
				THEN (m.suryo / COALESCE(item_mv.suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			WHEN ((tbecitem.dsoption002)::TEXT = ('事前組上'::CHARACTER VARYING)::TEXT)
				THEN m.suryo
			ELSE ((m.suryo * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			END AS hanbaiitem_suryo,
		h.komiwarikingaku AS tokuten_gaku,
		CASE 
			WHEN (h.nukikingaku = ((0)::NUMERIC)::NUMERIC(18, 0))
				THEN (0)::DOUBLE PRECISION
			ELSE (((((h.komiwarikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION) * (m.anbunmeisainukikingaku)::DOUBLE PRECISION) / ((h.nukikingaku + (h.riyopoint * (- ((1)::NUMERIC)::NUMERIC(18, 0)))))::DOUBLE PRECISION)
			END AS anbun_tokuten_gaku,
		(
			CASE 
				WHEN (h.shukadate >= ((20191001)::NUMERIC)::NUMERIC(18, 0))
					THEN 0.90
				WHEN (h.shukadate >= ((20140401)::NUMERIC)::NUMERIC(18, 0))
					THEN 0.92
				WHEN (h.shukadate >= ((19970401)::NUMERIC)::NUMERIC(18, 0))
					THEN 0.95
				WHEN (h.shukadate >= ((19890401)::NUMERIC)::NUMERIC(18, 0))
					THEN 0.97
				ELSE ((1)::NUMERIC)::NUMERIC(18, 0)
				END * h.soryo
			) AS soryo_zeinuki,
		CASE 
			WHEN (h.nukikingaku = ((0)::NUMERIC)::NUMERIC(18, 0))
				THEN (0)::DOUBLE PRECISION
			ELSE (
					(
						(
							(
								(
									(
										CASE 
											WHEN (h.shukadate >= ((20191001)::NUMERIC)::NUMERIC(18, 0))
												THEN 0.90
											WHEN (h.shukadate >= ((20140401)::NUMERIC)::NUMERIC(18, 0))
												THEN 0.92
											WHEN (h.shukadate >= ((19970401)::NUMERIC)::NUMERIC(18, 0))
												THEN 0.95
											WHEN (h.shukadate >= ((19890401)::NUMERIC)::NUMERIC(18, 0))
												THEN 0.97
											ELSE ((1)::NUMERIC)::NUMERIC(18, 0)
											END * h.soryo
										)
									)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)
								) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION
							) * (m.anbunmeisainukikingaku)::DOUBLE PRECISION
						) / ((h.nukikingaku + (h.riyopoint * (- ((1)::NUMERIC)::NUMERIC(18, 0)))))::DOUBLE PRECISION
					)
			END AS anbun_soryo_zeinuki,
		(h.nukikingaku + (h.riyopoint * (- ((1)::NUMERIC)::NUMERIC(18, 0)))) AS meisainukikingaku_gokei,
		COALESCE(tbecitem.diitemsalesprc, (0)::BIGINT) AS diitemsalesprc
	FROM (
		(
			(
				(
					(
						(
							cit80saleh h JOIN cit81salem m ON (((h.saleno)::TEXT = (m.saleno)::TEXT))
							) LEFT JOIN cim08shkos_bunkai item ON (((m.itemcode_hanbai)::TEXT = (item.itemcode)::TEXT))
						) LEFT JOIN syouhincd_henkan henkan ON (((item.bunkai_itemcode)::TEXT = (henkan.itemcode)::TEXT))
					) LEFT JOIN tm14shkos "k" ON (((COALESCE(henkan.koseiocode, COALESCE(item.bunkai_itemcode, m.itemcode)))::TEXT = ("k".itemcode)::TEXT))
				) LEFT JOIN tbecitem tbecitem ON (
					(
						(
							((tbecitem.dsitemid)::TEXT = (m.itemcode_hanbai)::TEXT)
							AND ((tbecitem.dsoption001)::TEXT = ('販売商品'::CHARACTER VARYING)::TEXT)
							)
						AND ((tbecitem.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
						)
					)
			) LEFT JOIN (
			SELECT DISTINCT item_data_mart_mv.h_o_item_cd,
				item_data_mart_mv.z_item_cd,
				item_data_mart_mv.h_bun_suryo AS suryo
			FROM item_data_mart_mv
			) item_mv ON (
				(
					((item_mv.h_o_item_cd)::TEXT = (m.itemcode_hanbai)::TEXT)
					AND ((item_mv.z_item_cd)::TEXT = (m.itemcode)::TEXT)
					)
				)
		)
	WHERE (
			(
				(
					(
						(h.cancelflg = ((0)::NUMERIC)::NUMERIC(18, 0))
						AND ((h.torikeikbn)::TEXT = ('01'::CHARACTER VARYING)::TEXT)
						)
					AND ((m.itemcode)::TEXT <> ('9990000100'::CHARACTER VARYING)::TEXT)
					)
				AND ((m.itemcode)::TEXT <> ('9990000200'::CHARACTER VARYING)::TEXT)
				)
			AND (h.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
			)
	),
ct2
AS (
	SELECT h.saleno,
		h.shukadate AS shukanengetubi,
		CASE 
			WHEN (h.smkeiroid = ((5)::NUMERIC)::NUMERIC(18, 0))
				THEN '121'::CHARACTER VARYING
			ELSE CASE 
					WHEN (h.smkeiroid = ((6)::NUMERIC)::NUMERIC(18, 0))
						THEN '112'::CHARACTER VARYING
					WHEN (h.marker = ((4)::NUMERIC)::NUMERIC(18, 0))
						THEN '511'::CHARACTER VARYING
					ELSE '111'::CHARACTER VARYING
					END
			END AS channel,
		h.daihanrobunname AS konyuchubuncode,
		h.torikeikbn,
		'ダミーコード'::CHARACTER VARYING AS tokuicode,
		m.itemcode_hanbai AS itemcode,
		"k".kosecode,
		h.juchkbn,
		(m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0))) AS suryo,
		CASE 
			WHEN (
					(substring((h.juchkbn)::TEXT, 1, 1) = ('9'::CHARACTER VARYING)::TEXT)
					OR (
						(substring((h.juchkbn)::TEXT, 1, 1) IS NULL)
						AND ('9' IS NULL)
						)
					)
				THEN (m.hensu * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
			END AS hensu,
		"next".tyouseimaekingaku AS anbunmaegaku_tyouseimae,
		("next".tyouseimaekingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS meisainukikingaku_tyouseimae,
		m.meisainukikingaku AS anbunmaegaku_tyouseigo,
		(m.meisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS meisainukikingaku_tyouseigo,
		(m.anbunmeisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS anbunmeisainukikingaku,
		CASE 
			WHEN (h.riyopoint = ((0)::NUMERIC)::NUMERIC(18, 0))
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			WHEN (
					(m.itemcode_hanbai IS NULL)
					OR (m.meisainukikingaku = ((0)::NUMERIC)::NUMERIC(18, 0))
					)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE (
					(
						(
							CASE 
								WHEN (h.shukadate >= ((20191001)::NUMERIC)::NUMERIC(18, 0))
									THEN 0.90
								WHEN (h.shukadate >= ((20140401)::NUMERIC)::NUMERIC(18, 0))
									THEN 0.92
								WHEN (h.shukadate >= ((19970401)::NUMERIC)::NUMERIC(18, 0))
									THEN 0.95
								WHEN (h.shukadate >= ((19890401)::NUMERIC)::NUMERIC(18, 0))
									THEN 0.97
								ELSE ((1)::NUMERIC)::NUMERIC(18, 0)
								END * h.riyopoint
							) * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))
						) * (m.meisainukikingaku / h.warimaenukigokei)
					)
			END AS anbun_riyopoint_notax,
		CASE 
			WHEN (h.riyopoint = ((0)::NUMERIC)::NUMERIC(18, 0))
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			WHEN (
					(m.itemcode_hanbai IS NULL)
					OR (m.meisainukikingaku = ((0)::NUMERIC)::NUMERIC(18, 0))
					)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ((h.riyopoint * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) * (m.meisainukikingaku / h.warimaenukigokei))
			END AS anbun_riyopoint_taxkomi,
		h.henreasoncode,
		h.henreasonname,
		''::CHARACTER VARYING AS sokoname,
		h.bmn_hyouji_cd,
		h.bmn_nms,
		tbecitem.dsoption002 AS hanbaiitem_kubun,
		CASE 
			WHEN ((tbecitem.dsoption002)::TEXT = ('ピッキング対応'::CHARACTER VARYING)::TEXT)
				THEN (m.suryo / COALESCE(item_mv.suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			WHEN ((tbecitem.dsoption002)::TEXT = ('事前組上'::CHARACTER VARYING)::TEXT)
				THEN m.suryo
			ELSE (m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			END AS hanbaiitem_suryo,
		CASE 
			WHEN (
					((ssmthdclthanjuchhedda.dcldousou_kbn)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
					OR (
						(ssmthdclthanjuchhedda.dcldousou_kbn IS NULL)
						AND ('0' IS NULL)
						)
					)
				THEN COALESCE(to_number("replace" (
								(ssmthdclthanjuchhedda.dclwari_nebiki)::TEXT,
								(','::CHARACTER VARYING)::TEXT,
								(''::CHARACTER VARYING)::TEXT
								), ('99999999'::CHARACTER VARYING)::TEXT), ((0)::NUMERIC)::NUMERIC(18, 0))
			ELSE COALESCE(to_number("replace" (
							(ssmthdclthanjuchhedda.dclwari_nebiki_c)::TEXT,
							(','::CHARACTER VARYING)::TEXT,
							(''::CHARACTER VARYING)::TEXT
							), ('99999999'::CHARACTER VARYING)::TEXT), ((0)::NUMERIC)::NUMERIC(18, 0))
			END AS tokuten_gaku,
		CASE 
			WHEN (
					(m.itemcode_hanbai IS NULL)
					OR (m.meisainukikingaku = ((0)::NUMERIC)::NUMERIC(18, 0))
					)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE (
					(
						CASE 
							WHEN (
									((ssmthdclthanjuchhedda.dcldousou_kbn)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
									OR (
										(ssmthdclthanjuchhedda.dcldousou_kbn IS NULL)
										AND ('0' IS NULL)
										)
									)
								THEN COALESCE(to_number("replace" (
												(ssmthdclthanjuchhedda.dclwari_nebiki)::TEXT,
												(','::CHARACTER VARYING)::TEXT,
												(''::CHARACTER VARYING)::TEXT
												), ('99999999'::CHARACTER VARYING)::TEXT), ((0)::NUMERIC)::NUMERIC(18, 0))
							ELSE COALESCE(to_number("replace" (
											(ssmthdclthanjuchhedda.dclwari_nebiki_c)::TEXT,
											(','::CHARACTER VARYING)::TEXT,
											(''::CHARACTER VARYING)::TEXT
											), ('99999999'::CHARACTER VARYING)::TEXT), ((0)::NUMERIC)::NUMERIC(18, 0))
							END * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))
						) * (m.meisainukikingaku / h.warimaenukigokei)
					)
			END AS anbun_tokuten_gaku,
		(
			CASE 
				WHEN (h.shukadate >= ((20191001)::NUMERIC)::NUMERIC(18, 0))
					THEN 0.90
				WHEN (h.shukadate >= ((20140401)::NUMERIC)::NUMERIC(18, 0))
					THEN 0.92
				WHEN (h.shukadate >= ((19970401)::NUMERIC)::NUMERIC(18, 0))
					THEN 0.95
				WHEN (h.shukadate >= ((19890401)::NUMERIC)::NUMERIC(18, 0))
					THEN 0.97
				ELSE ((1)::NUMERIC)::NUMERIC(18, 0)
				END * h.soryo
			) AS soryo_zeinuki,
		CASE 
			WHEN (
					(m.itemcode_hanbai IS NULL)
					OR (m.meisainukikingaku = ((0)::NUMERIC)::NUMERIC(18, 0))
					)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE (
					(
						(
							CASE 
								WHEN (h.shukadate >= ((20191001)::NUMERIC)::NUMERIC(18, 0))
									THEN 0.90
								WHEN (h.shukadate >= ((20140401)::NUMERIC)::NUMERIC(18, 0))
									THEN 0.92
								WHEN (h.shukadate >= ((19970401)::NUMERIC)::NUMERIC(18, 0))
									THEN 0.95
								WHEN (h.shukadate >= ((19890401)::NUMERIC)::NUMERIC(18, 0))
									THEN 0.97
								ELSE ((1)::NUMERIC)::NUMERIC(18, 0)
								END * h.soryo
							) * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))
						) * (m.meisainukikingaku / h.warimaenukigokei)
					)
			END AS anbun_soryo_zeinuki,
		h.warimaenukigokei AS meisainukikingaku_gokei,
		COALESCE(tbecitem.diitemsalesprc, (0)::BIGINT) AS diitemsalesprc
	FROM (
		(
			(
				(
					(
						(
							(
								(
									cit80saleh h JOIN cit81salem m ON (((h.saleno)::TEXT = (m.saleno)::TEXT))
									) LEFT JOIN tm14shkos "k" ON (((m.itemcode)::TEXT = ("k".itemcode)::TEXT))
								) LEFT JOIN get_ci_next_sale "next" ON (
									(
										((m.saleno)::TEXT = ("next".saleno)::TEXT)
										AND (m.juchgyono = "next".juchgyono)
										)
									)
							) LEFT JOIN tbecitem tbecitem ON (
								(
									(
										((tbecitem.dsitemid)::TEXT = (m.itemcode_hanbai)::TEXT)
										AND ((tbecitem.dsoption001)::TEXT = ('販売商品'::CHARACTER VARYING)::TEXT)
										)
									AND ((tbecitem.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
									)
								)
						) LEFT JOIN (
						SELECT DISTINCT item_data_mart_mv.h_o_item_cd,
							item_data_mart_mv.z_item_cd,
							item_data_mart_mv.h_bun_suryo AS suryo
						FROM item_data_mart_mv
						) item_mv ON (
							(
								((item_mv.h_o_item_cd)::TEXT = (m.itemcode_hanbai)::TEXT)
								AND ((item_mv.z_item_cd)::TEXT = (m.itemcode)::TEXT)
								)
							)
					) LEFT JOIN hanyo_attr hanyo_attr ON (((hanyo_attr.kbnmei)::TEXT = ('KAISYA'::CHARACTER VARYING)::TEXT))
				) LEFT JOIN ssmthsalhedda ssmthsalhedda ON (
					(
						((ssmthsalhedda.kaisha_cd)::TEXT = (hanyo_attr.attr1)::TEXT)
						AND ((ssmthsalhedda.sal_no)::TEXT = (h.saleno)::TEXT)
						)
					)
			) LEFT JOIN ssmthdclthanjuchhedda ssmthdclthanjuchhedda ON (
				(
					((ssmthdclthanjuchhedda.kaisha_cd)::TEXT = (ssmthsalhedda.kaisha_cd)::TEXT)
					AND ((ssmthdclthanjuchhedda.juch_no)::TEXT = (ssmthsalhedda.hassei_mt_den_no)::TEXT)
					)
				)
		)
	WHERE (
			(
				(
					(
						(h.cancelflg = ((0)::NUMERIC)::NUMERIC(18, 0))
						AND ((h.torikeikbn)::TEXT = ('01'::CHARACTER VARYING)::TEXT)
						)
					AND ((m.itemcode)::TEXT <> ('9990000100'::CHARACTER VARYING)::TEXT)
					)
				AND ((m.itemcode)::TEXT <> ('9990000200'::CHARACTER VARYING)::TEXT)
				)
			AND (h.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
			)
	),
ct3
AS (
	SELECT h.ourino AS saleno,
		h.shukadate AS shukanengetubi,
		CASE 
			WHEN ((h.torikeikbn)::TEXT = ('02'::CHARACTER VARYING)::TEXT)
				THEN CASE 
						WHEN ((h.tokuicode)::TEXT = ('QVC0000001'::CHARACTER VARYING)::TEXT)
							THEN '321'::CHARACTER VARYING
						WHEN (
								((h.tokuicode)::TEXT = ('CC00100001'::CHARACTER VARYING)::TEXT)
								OR ((h.tokuicode)::TEXT = ('CC00100000'::CHARACTER VARYING)::TEXT)
								)
							THEN '111'::CHARACTER VARYING
						ELSE CASE 
								WHEN (
										(h.fskbn = ((1)::NUMERIC)::NUMERIC(18, 0))
										OR (h.fskbn = ((0)::NUMERIC)::NUMERIC(18, 0))
										)
									THEN '511'::CHARACTER VARYING
								ELSE CASE 
										WHEN ((h.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
											THEN '312'::CHARACTER VARYING
										WHEN ((h.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
											THEN '313'::CHARACTER VARYING
										WHEN ((h.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
											THEN '314'::CHARACTER VARYING
										ELSE CASE 
												WHEN (substring((h.tokuicode)::TEXT, 2, 9) <= ('000499999'::CHARACTER VARYING)::TEXT)
													THEN '311'::CHARACTER VARYING
												WHEN (
														(substring((h.tokuicode)::TEXT, 2, 9) >= ('000600000'::CHARACTER VARYING)::TEXT)
														AND (substring((h.tokuicode)::TEXT, 2, 9) <= ('000799999'::CHARACTER VARYING)::TEXT)
														)
													THEN '412'::CHARACTER VARYING
												WHEN (
														(substring((h.tokuicode)::TEXT, 2, 9) >= ('000500001'::CHARACTER VARYING)::TEXT)
														AND (substring((h.tokuicode)::TEXT, 2, 9) <= ('000599999'::CHARACTER VARYING)::TEXT)
														)
													THEN '312'::CHARACTER VARYING
												WHEN (
														(substring((h.tokuicode)::TEXT, 2, 9) >= ('000800001'::CHARACTER VARYING)::TEXT)
														AND (substring((h.tokuicode)::TEXT, 2, 9) <= ('000899999'::CHARACTER VARYING)::TEXT)
														)
													THEN '311'::CHARACTER VARYING
												WHEN (substring((h.tokuicode)::TEXT, 2, 9) >= ('000900000'::CHARACTER VARYING)::TEXT)
													THEN CASE 
															WHEN ((h.tokuicode)::TEXT = ('000099%'::CHARACTER VARYING)::TEXT)
																THEN '411'::CHARACTER VARYING
															WHEN ((h.tokuicode)::TEXT = ('VIP%'::CHARACTER VARYING)::TEXT)
																THEN '113'::CHARACTER VARYING
															WHEN ((h.tokuicode)::TEXT = ('CINEXT%'::CHARACTER VARYING)::TEXT)
																THEN '111'::CHARACTER VARYING
															ELSE '412'::CHARACTER VARYING
															END
												ELSE ''::CHARACTER VARYING
												END
										END
								END
						END
			ELSE CASE 
					WHEN ((h.torikeikbn)::TEXT = ('03'::CHARACTER VARYING)::TEXT)
						THEN '211'::CHARACTER VARYING
					WHEN ((h.torikeikbn)::TEXT = ('04'::CHARACTER VARYING)::TEXT)
						THEN '212'::CHARACTER VARYING
					WHEN ((h.torikeikbn)::TEXT = ('05'::CHARACTER VARYING)::TEXT)
						THEN '213'::CHARACTER VARYING
					WHEN ((h.torikeikbn)::TEXT = ('06'::CHARACTER VARYING)::TEXT)
						THEN '214'::CHARACTER VARYING
					ELSE NULL::CHARACTER VARYING
					END
			END AS channel,
		''::CHARACTER VARYING AS konyuchubuncode,
		h.torikeikbn,
		h.tokuicode,
		m.itemcode_hanbai AS itemcode,
		"k".kosecode,
		h.juchkbn,
		((m.suryo * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0))) AS suryo,
		CASE 
			WHEN (
					(substring((h.juchkbn)::TEXT, 1, 1) = ('9'::CHARACTER VARYING)::TEXT)
					OR (
						(substring((h.juchkbn)::TEXT, 1, 1) IS NULL)
						AND ('9' IS NULL)
						)
					)
				THEN ((m.hensu * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
			END AS hensu,
		0 AS anbunmaegaku_tyouseimae,
		0 AS meisainukikingaku_tyouseimae,
		m.meisainukikingaku AS anbunmaegaku_tyouseigo,
		(((m.meisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION) AS meisainukikingaku_tyouseigo,
		(((m.anbunmeisainukikingaku)::DOUBLE PRECISION * COALESCE(item.bunkai_kosritu, (1)::DOUBLE PRECISION)) * (COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0)))::DOUBLE PRECISION) AS anbunmeisainukikingaku,
		0 AS anbun_riyopoint_notax,
		0 AS anbun_riyopoint_taxkomi,
		h.henreasoncode,
		h.henreasonname,
		''::CHARACTER VARYING AS sokoname,
		h.bmn_hyouji_cd,
		h.bmn_nms,
		tbecitem.dsoption002 AS hanbaiitem_kubun,
		CASE 
			WHEN ((tbecitem.dsoption002)::TEXT = ('事前組上'::CHARACTER VARYING)::TEXT)
				THEN m.suryo
			ELSE ((m.suryo * COALESCE(item.bunkai_kossu, ((1)::NUMERIC)::NUMERIC(18, 0))) * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			END AS hanbaiitem_suryo,
		0 AS tokuten_gaku,
		0 AS anbun_tokuten_gaku,
		0 AS soryo_zeinuki,
		0 AS anbun_soryo_zeinuki,
		h.shokei AS meisainukikingaku_gokei,
		COALESCE(tbecitem.diitemsalesprc, (0)::BIGINT) AS diitemsalesprc
	FROM (
		(
			(
				(
					(
						cit85osalh h JOIN cit86osalm m ON (((h.ourino)::TEXT = (m.ourino)::TEXT))
						) LEFT JOIN cim08shkos_bunkai item ON (((m.itemcode_hanbai)::TEXT = (item.itemcode)::TEXT))
					) LEFT JOIN syouhincd_henkan henkan ON (((item.bunkai_itemcode)::TEXT = (henkan.itemcode)::TEXT))
				) LEFT JOIN tm14shkos "k" ON (((COALESCE(henkan.koseiocode, COALESCE(item.bunkai_itemcode, m.itemcode)))::TEXT = ("k".itemcode)::TEXT))
			) LEFT JOIN tbecitem tbecitem ON (
				(
					(
						((tbecitem.dsitemid)::TEXT = (m.itemcode_hanbai)::TEXT)
						AND ((tbecitem.dsoption001)::TEXT = ('販売商品'::CHARACTER VARYING)::TEXT)
						)
					AND ((tbecitem.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
					)
				)
		)
	WHERE (h.kakokbn = ((1)::NUMERIC)::NUMERIC(18, 0))
	),
ct4
AS (
	SELECT h.ourino AS saleno,
		h.shukadate AS shukanengetubi,
		CASE 
			WHEN ((h.torikeikbn)::TEXT = ('02'::CHARACTER VARYING)::TEXT)
				THEN CASE 
						WHEN ((h.tokuicode)::TEXT = ('QVC0000001'::CHARACTER VARYING)::TEXT)
							THEN '321'::CHARACTER VARYING
						WHEN (
								((h.tokuicode)::TEXT = ('CC00100001'::CHARACTER VARYING)::TEXT)
								OR ((h.tokuicode)::TEXT = ('CC00100000'::CHARACTER VARYING)::TEXT)
								)
							THEN '111'::CHARACTER VARYING
						ELSE CASE 
								WHEN (
										(h.fskbn = ((1)::NUMERIC)::NUMERIC(18, 0))
										OR (h.fskbn = ((0)::NUMERIC)::NUMERIC(18, 0))
										)
									THEN '511'::CHARACTER VARYING
								ELSE CASE 
										WHEN ((h.shokuikibunrui)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
											THEN '312'::CHARACTER VARYING
										WHEN ((h.shokuikibunrui)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
											THEN '313'::CHARACTER VARYING
										WHEN ((h.shokuikibunrui)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
											THEN '314'::CHARACTER VARYING
										ELSE CASE 
												WHEN (substring((h.tokuicode)::TEXT, 2, 9) <= ('000499999'::CHARACTER VARYING)::TEXT)
													THEN '311'::CHARACTER VARYING
												WHEN (
														(substring((h.tokuicode)::TEXT, 2, 9) >= ('000600000'::CHARACTER VARYING)::TEXT)
														AND (substring((h.tokuicode)::TEXT, 2, 9) <= ('000799999'::CHARACTER VARYING)::TEXT)
														)
													THEN '412'::CHARACTER VARYING
												WHEN (
														(substring((h.tokuicode)::TEXT, 2, 9) >= ('000500001'::CHARACTER VARYING)::TEXT)
														AND (substring((h.tokuicode)::TEXT, 2, 9) <= ('000599999'::CHARACTER VARYING)::TEXT)
														)
													THEN '312'::CHARACTER VARYING
												WHEN (
														(substring((h.tokuicode)::TEXT, 2, 9) >= ('000800001'::CHARACTER VARYING)::TEXT)
														AND (substring((h.tokuicode)::TEXT, 2, 9) <= ('000899999'::CHARACTER VARYING)::TEXT)
														)
													THEN '311'::CHARACTER VARYING
												WHEN (substring((h.tokuicode)::TEXT, 2, 9) >= ('000900000'::CHARACTER VARYING)::TEXT)
													THEN CASE 
															WHEN ((h.tokuicode)::TEXT = ('000099%'::CHARACTER VARYING)::TEXT)
																THEN '411'::CHARACTER VARYING
															WHEN ((h.tokuicode)::TEXT = ('VIP%'::CHARACTER VARYING)::TEXT)
																THEN '113'::CHARACTER VARYING
															WHEN ((h.tokuicode)::TEXT = ('CINEXT%'::CHARACTER VARYING)::TEXT)
																THEN '111'::CHARACTER VARYING
															ELSE '412'::CHARACTER VARYING
															END
												ELSE ''::CHARACTER VARYING
												END
										END
								END
						END
			ELSE CASE 
					WHEN ((h.torikeikbn)::TEXT = ('03'::CHARACTER VARYING)::TEXT)
						THEN '211'::CHARACTER VARYING
					WHEN ((h.torikeikbn)::TEXT = ('04'::CHARACTER VARYING)::TEXT)
						THEN '212'::CHARACTER VARYING
					WHEN ((h.torikeikbn)::TEXT = ('05'::CHARACTER VARYING)::TEXT)
						THEN '213'::CHARACTER VARYING
					WHEN ((h.torikeikbn)::TEXT = ('06'::CHARACTER VARYING)::TEXT)
						THEN '214'::CHARACTER VARYING
					ELSE NULL::CHARACTER VARYING
					END
			END AS channel,
		''::CHARACTER VARYING AS konyuchubuncode,
		h.torikeikbn,
		h.tokuicode,
		m.itemcode,
		"k".kosecode,
		h.juchkbn,
		(m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0))) AS suryo,
		CASE 
			WHEN (
					(substring((h.juchkbn)::TEXT, 1, 1) = ('9'::CHARACTER VARYING)::TEXT)
					OR (
						(substring((h.juchkbn)::TEXT, 1, 1) IS NULL)
						AND ('9' IS NULL)
						)
					)
				THEN (m.hensu * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
			END AS hensu,
		0 AS anbunmaegaku_tyouseimae,
		0 AS meisainukikingaku_tyouseimae,
		m.meisainukikingaku AS anbunmaegaku_tyouseigo,
		(m.meisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS meisainukikingaku_tyouseigo,
		(m.anbunmeisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS anbunmeisainukikingaku,
		0 AS anbun_riyopoint_notax,
		0 AS anbun_riyopoint_taxkomi,
		h.henreasoncode,
		h.henreasonname,
		(rtrim(rtrim(t.sokoname)))::CHARACTER VARYING AS sokoname,
		h.bmn_hyouji_cd,
		h.bmn_nms,
		tbecitem.dsoption002 AS hanbaiitem_kubun,
		CASE 
			WHEN ((tbecitem.dsoption002)::TEXT = ('事前組上'::CHARACTER VARYING)::TEXT)
				THEN m.suryo
			ELSE (m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			END AS hanbaiitem_suryo,
		COALESCE(ssmthsalhedda.den_nebiki_kin, ((0)::NUMERIC)::NUMERIC(18, 0)) AS tokuten_gaku,
		(COALESCE(ssmtbsalmei.den_nebiki_abn_kin, ((0)::NUMERIC)::NUMERIC(18, 0)) * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS anbun_tokuten_gaku,
		0 AS soryo_zeinuki,
		0 AS anbun_soryo_zeinuki,
		h.shokei AS meisainukikingaku_gokei,
		COALESCE(tbecitem.diitemsalesprc, (0)::BIGINT) AS diitemsalesprc
	FROM (
		(
			(
				(
					(
						(
							(
								cit85osalh h JOIN cit86osalm m ON (((h.ourino)::TEXT = (m.ourino)::TEXT))
								) LEFT JOIN tm14shkos "k" ON (((m.itemcode)::TEXT = ("k".itemcode)::TEXT))
							) LEFT JOIN (
							SELECT tm.c_dstempocode AS sokocode,
								(((((tm.c_dstempocode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (tm.c_dstemponame)::TEXT) || (' '::CHARACTER VARYING)::TEXT) || (tm.c_dstemponame)::TEXT) AS sokoname
							FROM c_tbecclient tm
							) t ON (((h.tenpocode)::TEXT = (t.sokocode)::TEXT))
						) LEFT JOIN tbecitem tbecitem ON (
							(
								(
									((tbecitem.dsitemid)::TEXT = (m.itemcode)::TEXT)
									AND ((tbecitem.dsoption001)::TEXT = ('在庫商品'::CHARACTER VARYING)::TEXT)
									)
								AND ((tbecitem.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
								)
							)
					) LEFT JOIN hanyo_attr hanyo_attr ON (((hanyo_attr.kbnmei)::TEXT = ('KAISYA'::CHARACTER VARYING)::TEXT))
				) LEFT JOIN ssmthsalhedda ssmthsalhedda ON (
					(
						((ssmthsalhedda.kaisha_cd)::TEXT = (hanyo_attr.attr1)::TEXT)
						AND ((ssmthsalhedda.sal_no)::TEXT = (m.ourino)::TEXT)
						)
					)
			) LEFT JOIN ssmtbsalmei ssmtbsalmei ON (
				(
					(
						((ssmtbsalmei.kaisha_cd)::TEXT = (hanyo_attr.attr1)::TEXT)
						AND ((ssmtbsalmei.sal_no)::TEXT = (m.ourino)::TEXT)
						)
					AND (((ssmtbsalmei.sal_meirow_no)::NUMERIC)::NUMERIC(18, 0) = m.gyono)
					)
				)
		)
	WHERE (h.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
	),
ct5
AS (
	SELECT h.ourino AS saleno,
		h.shukadate AS shukanengetubi,
		CASE 
			WHEN ((h.tokuicode)::TEXT = ('000099%'::CHARACTER VARYING)::TEXT)
				THEN '411'::CHARACTER VARYING
			WHEN ((h.tokuicode)::TEXT = ('000091%'::CHARACTER VARYING)::TEXT)
				THEN '413'::CHARACTER VARYING
			ELSE '412'::CHARACTER VARYING
			END AS channel,
		''::CHARACTER VARYING AS konyuchubuncode,
		h.torikeikbn,
		h.tokuicode,
		m.itemcode,
		"k".kosecode,
		h.juchkbn,
		(m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0))) AS suryo,
		CASE 
			WHEN (
					(substring((h.juchkbn)::TEXT, 1, 1) = ('9'::CHARACTER VARYING)::TEXT)
					OR (
						(substring((h.juchkbn)::TEXT, 1, 1) IS NULL)
						AND ('9' IS NULL)
						)
					)
				THEN (m.hensu * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
			END AS hensu,
		0 AS anbunmaegaku_tyouseimae,
		0 AS meisainukikingaku_tyouseimae,
		m.meisainukikingaku AS anbunmaegaku_tyouseigo,
		(m.meisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS meisainukikingaku_tyouseigo,
		(m.anbunmeisainukikingaku * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS anbunmeisainukikingaku,
		0 AS anbun_riyopoint_notax,
		0 AS anbun_riyopoint_taxkomi,
		h.henreasoncode,
		h.henreasonname,
		''::CHARACTER VARYING AS sokoname,
		h.bmn_hyouji_cd,
		h.bmn_nms,
		tbecitem.dsoption002 AS hanbaiitem_kubun,
		CASE 
			WHEN ((tbecitem.dsoption002)::TEXT = ('事前組上'::CHARACTER VARYING)::TEXT)
				THEN m.suryo
			ELSE (m.suryo * COALESCE("k".suryo, ((1)::NUMERIC)::NUMERIC(18, 0)))
			END AS hanbaiitem_suryo,
		COALESCE(ssmthsalhedda.den_nebiki_kin, ((0)::NUMERIC)::NUMERIC(18, 0)) AS tokuten_gaku,
		(COALESCE(ssmtbsalmei.den_nebiki_abn_kin, ((0)::NUMERIC)::NUMERIC(18, 0)) * COALESCE("k".koseritsu, ((1)::NUMERIC)::NUMERIC(18, 0))) AS anbun_tokuten_gaku,
		0 AS soryo_zeinuki,
		0 AS anbun_soryo_zeinuki,
		h.shokei AS meisainukikingaku_gokei,
		COALESCE(tbecitem.diitemsalesprc, (0)::BIGINT) AS diitemsalesprc
	FROM (
		(
			(
				(
					(
						(
							cit85osalh_kaigai h JOIN cit86osalm_kaigai m ON (((h.ourino)::TEXT = (m.ourino)::TEXT))
							) LEFT JOIN tm14shkos "k" ON (((m.itemcode)::TEXT = ("k".itemcode)::TEXT))
						) LEFT JOIN tbecitem tbecitem ON (
							(
								(
									((tbecitem.dsitemid)::TEXT = (m.itemcode)::TEXT)
									AND ((tbecitem.dsoption001)::TEXT = ('在庫商品'::CHARACTER VARYING)::TEXT)
									)
								AND ((tbecitem.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
								)
							)
					) LEFT JOIN hanyo_attr hanyo_attr ON (((hanyo_attr.kbnmei)::TEXT = ('KAISYA'::CHARACTER VARYING)::TEXT))
				) LEFT JOIN ssmthsalhedda ssmthsalhedda ON (
					(
						((ssmthsalhedda.kaisha_cd)::TEXT = (hanyo_attr.attr1)::TEXT)
						AND ((ssmthsalhedda.sal_no)::TEXT = (m.ourino)::TEXT)
						)
					)
			) LEFT JOIN ssmtbsalmei ssmtbsalmei ON (
				(
					(
						((ssmtbsalmei.kaisha_cd)::TEXT = (hanyo_attr.attr1)::TEXT)
						AND ((ssmtbsalmei.sal_no)::TEXT = (m.ourino)::TEXT)
						)
					AND (((ssmtbsalmei.sal_meirow_no)::NUMERIC)::NUMERIC(18, 0) = m.gyono)
					)
				)
		)
	WHERE (h.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
	),
t
AS (
	SELECT *
	FROM ct1
	
	UNION ALL
	
	SELECT *
	FROM ct2
	
	UNION ALL
	
	SELECT *
	FROM ct3
	
	UNION ALL
	
	SELECT *
	FROM ct4
	
	UNION ALL
	
	SELECT *
	FROM ct5
	),
misettei1
AS (
	SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
		"wqtm07属性未設定名称マスタ"."区分名称その他"
	FROM "wqtm07属性未設定名称マスタ"
	WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('1'::CHARACTER VARYING)::TEXT)
	),
misettei2
AS (
	SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
		"wqtm07属性未設定名称マスタ"."区分名称その他"
	FROM "wqtm07属性未設定名称マスタ"
	WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('2'::CHARACTER VARYING)::TEXT)
	),
misettei3
AS (
	SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
		"wqtm07属性未設定名称マスタ"."区分名称その他"
	FROM "wqtm07属性未設定名称マスタ"
	WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('3'::CHARACTER VARYING)::TEXT)
	),
misettei4
AS (
	SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
		"wqtm07属性未設定名称マスタ"."区分名称その他"
	FROM "wqtm07属性未設定名称マスタ"
	WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('4'::CHARACTER VARYING)::TEXT)
	),
misettei5
AS (
	SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
		"wqtm07属性未設定名称マスタ"."区分名称その他"
	FROM "wqtm07属性未設定名称マスタ"
	WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('5'::CHARACTER VARYING)::TEXT)
	),
misettei6
AS (
	SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
		"wqtm07属性未設定名称マスタ"."区分名称その他"
	FROM "wqtm07属性未設定名称マスタ"
	WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('6'::CHARACTER VARYING)::TEXT)
	),
misettei7
AS (
	SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
		"wqtm07属性未設定名称マスタ"."区分名称その他"
	FROM "wqtm07属性未設定名称マスタ"
	WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('7'::CHARACTER VARYING)::TEXT)
	),
misettei8
AS (
	SELECT "wqtm07属性未設定名称マスタ"."区分名称その他no",
		"wqtm07属性未設定名称マスタ"."区分名称その他"
	FROM "wqtm07属性未設定名称マスタ"
	WHERE (("wqtm07属性未設定名称マスタ"."属性区分コード")::TEXT = ('8'::CHARACTER VARYING)::TEXT)
	),
main
AS (
	SELECT to_date((
				(
					(
						(
							"left" (
								(((rtrim(t.shukanengetubi))::CHARACTER(8))::CHARACTER VARYING)::TEXT,
								4
								) || ('-'::CHARACTER VARYING)::TEXT
							) || substring((((rtrim(t.shukanengetubi))::CHARACTER(8))::CHARACTER VARYING)::TEXT, 5, 2)
						) || ('-'::CHARACTER VARYING)::TEXT
					) || "right" (
					(((rtrim(t.shukanengetubi))::CHARACTER(8))::CHARACTER VARYING)::TEXT,
					2
					)
				), ('YYYY-MM-DD'::CHARACTER VARYING)::TEXT) AS shukanengetubi,
		rtrim(t.channel) as channel,
		rtrim(t.konyuchubuncode) as konyuchubuncode,
		rtrim(t.torikeikbn) as torikeikbn,
		(((rtrim(t.tokuicode))::TEXT || (nvl2(rtrim(cim02.tokuiname), ' : '::CHARACTER VARYING, ''::CHARACTER VARYING))::TEXT) || COALESCE((((rtrim(cim02.tokuiname))::TEXT || (' '::CHARACTER VARYING)::TEXT) || (rtrim(cim02.tokuiname_ryaku))::TEXT), (''::CHARACTER VARYING)::TEXT)) AS tokuicode,
		rtrim(t.tokuicode) AS tokuicode_key,
		(((rtrim(t.itemcode))::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (COALESCE(rtrim(ia.itemname), rtrim(ia2.itemname)))::TEXT) AS hanbaiitem,
		rtrim(t.hanbaiitem_kubun) as hanbaiitem_kubun,
		sum(t.hanbaiitem_suryo) AS hanbaiitem_suryo,
		CASE 
			WHEN ((rtrim(t.hanbaiitem_kubun))::TEXT = ('単品'::CHARACTER VARYING)::TEXT)
				THEN (((rtrim(t.diitemsalesprc))::NUMERIC)::NUMERIC(18, 0) * COALESCE(sum((t.suryo + t.hensu)), ((0)::NUMERIC)::NUMERIC(18, 0)))
			WHEN (
					((rtrim(t.hanbaiitem_kubun))::TEXT = ('事前組上'::CHARACTER VARYING)::TEXT)
					OR ((rtrim(t.hanbaiitem_kubun))::TEXT = ('ピッキング対応'::CHARACTER VARYING)::TEXT)
					)
				THEN (((rtrim(t.diitemsalesprc))::NUMERIC)::NUMERIC(18, 0) * sum(t.hanbaiitem_suryo))
			ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
			END AS hanbaiitem_gaku,
		((COALESCE(trim((ib.bar_cd2)::TEXT), (rtrim(ib.itemcode))::TEXT) || (' : '::CHARACTER VARYING)::TEXT) || (COALESCE(rtrim(ib.itemname), rtrim(ib2.itemname)))::TEXT) AS kouseiitem,
		rtrim(t.kosecode) as kosecode,
		(((COALESCE(rtrim(tbecitem_k.diitemsalesprc), (0)::BIGINT))::NUMERIC)::NUMERIC(18, 0) * COALESCE(sum((t.suryo + t.hensu)), ((0)::NUMERIC)::NUMERIC(18, 0))) AS kouseiitem_gaku,
		rtrim(t.juchkbn) as juchkbn,
		rtrim(tm67.cname) AS juchkbncname,
		CASE 
			WHEN (
					(
						(rtrim(tm67.cname) = ('90 : 返品（通常）'::CHARACTER VARYING)::TEXT)
						OR (rtrim(tm67.cname) = ('91 : 返品（定期）'::CHARACTER VARYING)::TEXT)
						)
					OR (rtrim(tm67.cname) = ('92 : 返品（おまとめ）'::CHARACTER VARYING)::TEXT)
					)
				THEN (sum(t.meisainukikingaku_tyouseigo) + sum(t.anbun_soryo_zeinuki))
			ELSE (0)::DOUBLE PRECISION
			END AS ciw_return,
		sum(t.tokuten_gaku) AS tokuten_gaku,
		sum(t.anbun_tokuten_gaku) AS anbun_tokuten_gaku,
		sum(t.soryo_zeinuki) AS soryo_zeinuki,
		sum(t.anbun_soryo_zeinuki) AS anbun_soryo_zeinuki,
		sum(t.meisainukikingaku_gokei) AS meisainukikingaku_gokei,
		CASE 
			WHEN (
					(
						(rtrim(tm67.cname) <> ('90 : 返品（通常）'::CHARACTER VARYING)::TEXT)
						AND (rtrim(tm67.cname) <> ('91 : 返品（定期）'::CHARACTER VARYING)::TEXT)
						)
					AND (rtrim(tm67.cname) <> ('92 : 返品（おまとめ）'::CHARACTER VARYING)::TEXT)
					)
				THEN (sum(t.meisainukikingaku_tyouseigo) + sum(t.anbun_soryo_zeinuki))
			ELSE (0)::DOUBLE PRECISION
			END AS gts,
		sum(t.suryo) AS suryo,
		sum(t.hensu) AS hensu,
		sum((t.suryo + t.hensu)) AS urisuryo,
		sum(t.anbunmaegaku_tyouseimae) AS anbunmaegaku_tyouseimae,
		sum(t.meisainukikingaku_tyouseimae) AS meisainukikingaku_tyouseimae,
		sum(t.anbunmaegaku_tyouseigo) AS anbunmaegaku_tyouseigo,
		sum(t.meisainukikingaku_tyouseigo) AS meisainukikingaku_tyouseigo,
		sum(t.anbunmeisainukikingaku) AS anbunmeisainukikingaku,
		sum(t.anbun_riyopoint_notax) AS anbun_riyopoint_zeinuki,
		sum(t.anbun_riyopoint_taxkomi) AS anbun_riyopoint_zeikomi,
		CASE 
			WHEN (
					(
						(
							(
								((rtrim(t.channel))::TEXT = ('111'::CHARACTER VARYING)::TEXT)
								OR ((rtrim(t.channel))::TEXT = ('112'::CHARACTER VARYING)::TEXT)
								)
							OR ((rtrim(t.channel))::TEXT = ('113'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('114'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('121'::CHARACTER VARYING)::TEXT)
					)
				THEN '通信販売'::CHARACTER VARYING
			WHEN (
					(
						(
							((rtrim(t.channel))::TEXT = ('211'::CHARACTER VARYING)::TEXT)
							OR ((rtrim(t.channel))::TEXT = ('212'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('213'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('214'::CHARACTER VARYING)::TEXT)
					)
				THEN '対面販売'::CHARACTER VARYING
			WHEN (
					(
						(
							(
								((rtrim(t.channel))::TEXT = ('311'::CHARACTER VARYING)::TEXT)
								OR ((rtrim(t.channel))::TEXT = ('312'::CHARACTER VARYING)::TEXT)
								)
							OR ((rtrim(t.channel))::TEXT = ('313'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('314'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('321'::CHARACTER VARYING)::TEXT)
					)
				THEN '卸売'::CHARACTER VARYING
			WHEN (
					(
						((rtrim(t.channel))::TEXT = ('411'::CHARACTER VARYING)::TEXT)
						OR ((rtrim(t.channel))::TEXT = ('412'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('413'::CHARACTER VARYING)::TEXT)
					)
				THEN '海外'::CHARACTER VARYING
			WHEN (
					((rtrim(t.channel))::TEXT = ('511'::CHARACTER VARYING)::TEXT)
					OR ((rtrim(t.channel))::TEXT = ('512'::CHARACTER VARYING)::TEXT)
					)
				THEN 'その他'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END AS channel1,
		CASE 
			WHEN (
					(
						(
							((rtrim(t.channel))::TEXT = ('111'::CHARACTER VARYING)::TEXT)
							OR ((rtrim(t.channel))::TEXT = ('112'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('113'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('114'::CHARACTER VARYING)::TEXT)
					)
				THEN '通販'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('121'::CHARACTER VARYING)::TEXT)
				THEN 'WEB'::CHARACTER VARYING
			WHEN (
					(
						(
							((rtrim(t.channel))::TEXT = ('211'::CHARACTER VARYING)::TEXT)
							OR ((rtrim(t.channel))::TEXT = ('212'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('213'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('214'::CHARACTER VARYING)::TEXT)
					)
				THEN '店舗'::CHARACTER VARYING
			WHEN (
					(
						(
							((rtrim(t.channel))::TEXT = ('311'::CHARACTER VARYING)::TEXT)
							OR ((rtrim(t.channel))::TEXT = ('312'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('313'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('314'::CHARACTER VARYING)::TEXT)
					)
				THEN '卸売'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('321'::CHARACTER VARYING)::TEXT)
				THEN 'QVC'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('411'::CHARACTER VARYING)::TEXT)
				THEN 'JJ'::CHARACTER VARYING
			WHEN (
					((rtrim(t.channel))::TEXT = ('412'::CHARACTER VARYING)::TEXT)
					OR ((rtrim(t.channel))::TEXT = ('413'::CHARACTER VARYING)::TEXT)
					)
				THEN '海外'::CHARACTER VARYING
			WHEN (
					((rtrim(t.channel))::TEXT = ('511'::CHARACTER VARYING)::TEXT)
					OR ((rtrim(t.channel))::TEXT = ('512'::CHARACTER VARYING)::TEXT)
					)
				THEN 'その他'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END AS channel2,
		CASE 
			WHEN ((rtrim(t.channel))::TEXT = ('111'::CHARACTER VARYING)::TEXT)
				THEN '通販'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('112'::CHARACTER VARYING)::TEXT)
				THEN '社販'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('113'::CHARACTER VARYING)::TEXT)
				THEN 'VIP'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('114'::CHARACTER VARYING)::TEXT)
				THEN '通販'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('121'::CHARACTER VARYING)::TEXT)
				THEN 'WEB'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('211'::CHARACTER VARYING)::TEXT)
				THEN '買取'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('212'::CHARACTER VARYING)::TEXT)
				THEN '直営'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('213'::CHARACTER VARYING)::TEXT)
				THEN '消化'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('214'::CHARACTER VARYING)::TEXT)
				THEN 'アウトレット'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('311'::CHARACTER VARYING)::TEXT)
				THEN '代理店'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('312'::CHARACTER VARYING)::TEXT)
				THEN '職域（特販）'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('313'::CHARACTER VARYING)::TEXT)
				THEN '職域（代理店）'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('314'::CHARACTER VARYING)::TEXT)
				THEN '職域（販売会）'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('321'::CHARACTER VARYING)::TEXT)
				THEN 'QVC'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('411'::CHARACTER VARYING)::TEXT)
				THEN 'JJ'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('412'::CHARACTER VARYING)::TEXT)
				THEN '国内免税'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('413'::CHARACTER VARYING)::TEXT)
				THEN '海外免税'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('511'::CHARACTER VARYING)::TEXT)
				THEN 'FS'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('512'::CHARACTER VARYING)::TEXT)
				THEN 'その他'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END AS channel3,
		rtrim(t.bmn_hyouji_cd) as bmn_hyouji_cd,
		rtrim(t.bmn_nms) as bmn_nms,
		rtrim(t.henreasoncode) as henreasoncode,
		(((rtrim(t.henreasoncode))::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (rtrim(t.henreasonname))::TEXT) AS henreasonname,
		rtrim(t.sokoname) as sokoname,
		CASE 
			WHEN (rtrim(z.bumon6_kubun2) IS NULL)
				THEN rtrim(misettei1."区分名称その他")
			ELSE rtrim(z.bumon6_kubun2) 
			END AS kubun2,
		CASE 
			WHEN (rtrim(z.bumon6_syohingun) IS NULL)
				THEN rtrim(misettei2."区分名称その他")
			ELSE rtrim(z.bumon6_syohingun)
			END AS syohingun,
		CASE 
			WHEN (rtrim(z.bumon6_jyuutenitem) IS NULL)
				THEN rtrim(misettei3."区分名称その他")
			ELSE rtrim(z.bumon6_jyuutenitem)
			END AS jyuutenitem,
		rtrim(ib.itemkbnname) AS item_bunr_val1,
		rtrim(cim24_daidai.itbunname) AS item_bunr_val2,
		rtrim(ib.bunruicode3_nm) AS item_bunr_val3,
		rtrim(z.bumon7_add_attr1) as bumon7_add_attr1,
		rtrim(rtrim(z.bumon7_add_attr2)) as bumon7_add_attr2,
		rtrim(z.bumon7_add_attr3) as bumon7_add_attr3,
		rtrim(z.bumon7_add_attr4) as bumon7_add_attr4,
		rtrim(z.bumon7_add_attr6) as bumon7_add_attr6,
		rtrim(z.bumon7_add_attr7) as bumon7_add_attr7,
		rtrim(z.bumon7_add_attr5) as bumon7_add_attr5,
		rtrim(z.bumon7_add_attr8) as bumon7_add_attr8,
		rtrim(z.bumon7_add_attr9) as bumon7_add_attr9,
		rtrim(z.bumon7_add_attr10) as bumon7_add_attr10,
		rtrim(z.bumon6_20kisyohingun) as bumon6_20kisyohingun,
		rtrim(z.bumon6_20kinaieki1) as bumon6_20kinaieki1,
		rtrim(z.bumon6_20kinaieki2) as bumon6_20kinaieki2,
		rtrim(z.bumon6_20kinaieki3) as bumon6_20kinaieki3,
		rtrim(z.bumon6_zyutensyohinyobi1) as bumon6_zyutensyohinyobi1,
		rtrim(z.bumon6_zyutensyohinyobi2) as bumon6_zyutensyohinyobi2,
		rtrim(z.bumon6_zyutensyohinyobi3) as bumon6_zyutensyohinyobi3,
		rtrim(z.bumon6_zyutensyohinyobi4) as bumon6_zyutensyohinyobi4,
		rtrim(z.bumon6_zyutensyohinyobi5) as bumon6_zyutensyohinyobi5,
		rtrim(z.bumon6_okikaename) as bumon6_okikaename, 
		rtrim(z.bumon6_zukyuyosoku1) as bumon6_zukyuyosoku1,
		rtrim(z.bumon6_zukyuyosoku2) as bumon6_zukyuyosoku2,
		rtrim(z.bumon6_zukyuyosoku3) as bumon6_zukyuyosoku3
	FROM t
	LEFT JOIN cim03item_hanbai ia ON (((rtrim(t.itemcode))::TEXT = (rtrim(ia.itemcode))::TEXT))
	LEFT JOIN cim03item_zaiko ib ON (((rtrim(t.kosecode))::TEXT = (rtrim(ib.itemcode))::TEXT))
	LEFT JOIN cim03item_zaiko ia2 ON ((rtrim((t.itemcode))::TEXT = (rtrim(ia2.itemcode))::TEXT))
	LEFT JOIN cim03item_hanbai ib2 ON (((rtrim(t.kosecode))::TEXT = (rtrim(ib2.itemcode))::TEXT))
	LEFT JOIN zaiko_shohin_attr z ON (((rtrim(t.kosecode))::TEXT = (rtrim(z.shohin_code))::TEXT))
	LEFT JOIN tm67juch_nm tm67 ON (((rtrim(t.juchkbn))::TEXT = (rtrim(tm67.code))::TEXT))
	LEFT JOIN cim02tokui cim02 ON (((rtrim(t.tokuicode))::TEXT = (rtrim(cim02.tokuicode))::TEXT))
	LEFT JOIN cim24itbun cim24_daidai ON (
			(
				(
					((rtrim(ib.bunruicode5))::TEXT = (rtrim(cim24_daidai.itbuncode))::TEXT)
					AND ((rtrim(cim24_daidai.itbunshcode))::TEXT = ((5)::CHARACTER VARYING)::TEXT)
					)
				AND ((rtrim(ib.syutoku_kbn))::TEXT = ('PORT'::CHARACTER VARYING)::TEXT)
				)
			)
	LEFT JOIN tbecitem tbecitem_k ON (
			(
				(
					((rtrim(tbecitem_k.dsitemid))::TEXT = COALESCE(trim((ib.bar_cd2)::TEXT), (rtrim(ib.itemcode))::TEXT))
					AND ((rtrim(tbecitem_k.dsoption001))::TEXT = (nvl2(trim((ib.bar_cd2)::TEXT), '販売商品'::CHARACTER VARYING, '在庫商品'::CHARACTER VARYING))::TEXT)
					)
				AND ((rtrim(tbecitem_k.dielimflg))::TEXT = ('0'::CHARACTER VARYING)::TEXT)
				)
			)
	JOIN misettei1 ON ((1 = 1))
	JOIN misettei2 ON ((1 = 1))
	JOIN misettei3 ON ((1 = 1))
	JOIN misettei4 ON ((1 = 1))
	JOIN misettei5 ON ((1 = 1))
	JOIN misettei6 ON ((1 = 1))
	JOIN misettei7 ON ((1 = 1))
	JOIN misettei8 ON ((1 = 1))
	WHERE (1 = 1)
	GROUP BY rtrim(t.shukanengetubi),
		rtrim(t.channel),
		rtrim(t.konyuchubuncode),
		rtrim(t.torikeikbn),
		(((rtrim(t.tokuicode))::TEXT || (nvl2(rtrim(cim02.tokuiname), ' : '::CHARACTER VARYING, ''::CHARACTER VARYING))::TEXT) || COALESCE((((rtrim(cim02.tokuiname))::TEXT || (' '::CHARACTER VARYING)::TEXT) || (rtrim(cim02.tokuiname_ryaku))::TEXT), (''::CHARACTER VARYING)::TEXT)),
		rtrim(t.tokuicode),
		(((rtrim(t.itemcode))::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (COALESCE(rtrim(ia.itemname), rtrim(ia2.itemname)))::TEXT),
		rtrim(t.hanbaiitem_kubun),
		((COALESCE(trim((ib.bar_cd2)::TEXT), (rtrim(ib.itemcode))::TEXT) || (' : '::CHARACTER VARYING)::TEXT) || (COALESCE(rtrim(ib.itemname), rtrim(ib2.itemname)))::TEXT),
		rtrim(t.kosecode),
		rtrim(t.diitemsalesprc),
		rtrim(tbecitem_k.diitemsalesprc),
		rtrim(t.juchkbn),
		rtrim(tm67.cname),
		CASE 
			WHEN (
					(
						(
							(
								((rtrim(t.channel))::TEXT = ('111'::CHARACTER VARYING)::TEXT)
								OR ((rtrim(t.channel))::TEXT = ('112'::CHARACTER VARYING)::TEXT)
								)
							OR ((rtrim(t.channel))::TEXT = ('113'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('114'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('121'::CHARACTER VARYING)::TEXT)
					)
				THEN '通信販売'::CHARACTER VARYING
			WHEN (
					(
						(
							((rtrim(t.channel))::TEXT = ('211'::CHARACTER VARYING)::TEXT)
							OR ((rtrim(t.channel))::TEXT = ('212'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('213'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('214'::CHARACTER VARYING)::TEXT)
					)
				THEN '対面販売'::CHARACTER VARYING
			WHEN (
					(
						(
							(
								((rtrim(t.channel))::TEXT = ('311'::CHARACTER VARYING)::TEXT)
								OR ((rtrim(t.channel))::TEXT = ('312'::CHARACTER VARYING)::TEXT)
								)
							OR ((rtrim(t.channel))::TEXT = ('313'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('314'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('321'::CHARACTER VARYING)::TEXT)
					)
				THEN '卸売'::CHARACTER VARYING
			WHEN (
					(
						((rtrim(t.channel))::TEXT = ('411'::CHARACTER VARYING)::TEXT)
						OR ((rtrim(t.channel))::TEXT = ('412'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('413'::CHARACTER VARYING)::TEXT)
					)
				THEN '海外'::CHARACTER VARYING
			WHEN (
					((rtrim(t.channel))::TEXT = ('511'::CHARACTER VARYING)::TEXT)
					OR ((rtrim(t.channel))::TEXT = ('512'::CHARACTER VARYING)::TEXT)
					)
				THEN 'その他'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END,
		CASE 
			WHEN (
					(
						(
							((rtrim(t.channel))::TEXT = ('111'::CHARACTER VARYING)::TEXT)
							OR ((rtrim(t.channel))::TEXT = ('112'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('113'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('114'::CHARACTER VARYING)::TEXT)
					)
				THEN '通販'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('121'::CHARACTER VARYING)::TEXT)
				THEN 'WEB'::CHARACTER VARYING
			WHEN (
					(
						(
							((rtrim(t.channel))::TEXT = ('211'::CHARACTER VARYING)::TEXT)
							OR ((rtrim(t.channel))::TEXT = ('212'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('213'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('214'::CHARACTER VARYING)::TEXT)
					)
				THEN '店舗'::CHARACTER VARYING
			WHEN (
					(
						(
							((rtrim(t.channel))::TEXT = ('311'::CHARACTER VARYING)::TEXT)
							OR ((rtrim(t.channel))::TEXT = ('312'::CHARACTER VARYING)::TEXT)
							)
						OR ((rtrim(t.channel))::TEXT = ('313'::CHARACTER VARYING)::TEXT)
						)
					OR ((rtrim(t.channel))::TEXT = ('314'::CHARACTER VARYING)::TEXT)
					)
				THEN '卸売'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('321'::CHARACTER VARYING)::TEXT)
				THEN 'QVC'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('411'::CHARACTER VARYING)::TEXT)
				THEN 'JJ'::CHARACTER VARYING
			WHEN (
					((rtrim(t.channel))::TEXT = ('412'::CHARACTER VARYING)::TEXT)
					OR ((rtrim(t.channel))::TEXT = ('413'::CHARACTER VARYING)::TEXT)
					)
				THEN '海外'::CHARACTER VARYING
			WHEN (
					((rtrim(t.channel))::TEXT = ('511'::CHARACTER VARYING)::TEXT)
					OR ((rtrim(t.channel))::TEXT = ('512'::CHARACTER VARYING)::TEXT)
					)
				THEN 'その他'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END,
		CASE 
			WHEN ((rtrim(t.channel))::TEXT = ('111'::CHARACTER VARYING)::TEXT)
				THEN '通販'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('112'::CHARACTER VARYING)::TEXT)
				THEN '社販'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('113'::CHARACTER VARYING)::TEXT)
				THEN 'VIP'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('114'::CHARACTER VARYING)::TEXT)
				THEN '通販'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('121'::CHARACTER VARYING)::TEXT)
				THEN 'WEB'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('211'::CHARACTER VARYING)::TEXT)
				THEN '買取'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('212'::CHARACTER VARYING)::TEXT)
				THEN '直営'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('213'::CHARACTER VARYING)::TEXT)
				THEN '消化'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('214'::CHARACTER VARYING)::TEXT)
				THEN 'アウトレット'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('311'::CHARACTER VARYING)::TEXT)
				THEN '代理店'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('312'::CHARACTER VARYING)::TEXT)
				THEN '職域（特販）'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('313'::CHARACTER VARYING)::TEXT)
				THEN '職域（代理店）'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('314'::CHARACTER VARYING)::TEXT)
				THEN '職域（販売会）'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('321'::CHARACTER VARYING)::TEXT)
				THEN 'QVC'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('411'::CHARACTER VARYING)::TEXT)
				THEN 'JJ'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('412'::CHARACTER VARYING)::TEXT)
				THEN '国内免税'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('413'::CHARACTER VARYING)::TEXT)
				THEN '海外免税'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('511'::CHARACTER VARYING)::TEXT)
				THEN 'FS'::CHARACTER VARYING
			WHEN ((rtrim(t.channel))::TEXT = ('512'::CHARACTER VARYING)::TEXT)
				THEN 'その他'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END,
		rtrim(t.bmn_hyouji_cd),
		rtrim(t.bmn_nms),
		rtrim(t.henreasoncode),
		(((rtrim(t.henreasoncode))::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (rtrim(t.henreasonname))::TEXT),
		rtrim(t.sokoname) ,
		CASE 
			WHEN (rtrim(z.bumon6_kubun2) IS NULL)
				THEN rtrim(misettei1."区分名称その他")
			ELSE rtrim(z.bumon6_kubun2)
			END,
		CASE 
			WHEN (rtrim(z.bumon6_syohingun) IS NULL)
				THEN rtrim(misettei2."区分名称その他")
			ELSE rtrim(z.bumon6_syohingun)
			END,
		CASE 
			WHEN (rtrim(z.bumon6_jyuutenitem) IS NULL)
				THEN rtrim(misettei3."区分名称その他")
			ELSE rtrim(z.bumon6_jyuutenitem)
			END,
		rtrim(ib.itemkbnname) ,
		rtrim(cim24_daidai.itbunname),
		rtrim(ib.bunruicode3_nm),
		rtrim(z.bumon7_add_attr1),
		rtrim(z.bumon7_add_attr2),
		rtrim(z.bumon7_add_attr3),
		rtrim(z.bumon7_add_attr4),
		rtrim(z.bumon7_add_attr5),
		rtrim(z.bumon7_add_attr6),
		rtrim(z.bumon7_add_attr7),
		rtrim(z.bumon7_add_attr8),
		rtrim(z.bumon7_add_attr9),
		rtrim(z.bumon7_add_attr10),
		rtrim(z.bumon6_20kisyohingun),
		rtrim(z.bumon6_20kinaieki1),
		rtrim(z.bumon6_20kinaieki2),
		rtrim(z.bumon6_20kinaieki3),
		rtrim(z.bumon6_zyutensyohinyobi1),
		rtrim(z.bumon6_zyutensyohinyobi2),
		rtrim(z.bumon6_zyutensyohinyobi3),
		rtrim(z.bumon6_zyutensyohinyobi4),
		rtrim(z.bumon6_zyutensyohinyobi5),
		rtrim(z.bumon6_okikaename),
		rtrim(z.bumon6_zukyuyosoku1),
		rtrim(z.bumon6_zukyuyosoku2),
		rtrim(z.bumon6_zukyuyosoku3)
	),
final
AS (
	SELECT main.shukanengetubi,
		main.channel,
		main.konyuchubuncode,
		main.torikeikbn,
		main.tokuicode,
		main.tokuicode_key,
		main.hanbaiitem,
		main.hanbaiitem_kubun,
		main.hanbaiitem_suryo,
		main.hanbaiitem_gaku,
		main.kouseiitem,
		main.kosecode,
		main.kouseiitem_gaku,
		main.juchkbn,
		main.juchkbncname,
		main.ciw_return,
		main.tokuten_gaku,
		main.anbun_tokuten_gaku,
		main.soryo_zeinuki,
		main.anbun_soryo_zeinuki,
		main.meisainukikingaku_gokei,
		main.gts,
		main.suryo,
		main.hensu,
		main.urisuryo,
		main.anbunmaegaku_tyouseimae,
		main.meisainukikingaku_tyouseimae,
		main.anbunmaegaku_tyouseigo,
		main.meisainukikingaku_tyouseigo,
		main.anbunmeisainukikingaku,
		main.anbun_riyopoint_zeinuki,
		main.anbun_riyopoint_zeikomi,
		main.channel1,
		main.channel2,
		main.channel3,
		main.bmn_hyouji_cd,
		main.bmn_nms,
		main.henreasoncode,
		main.henreasonname,
		main.sokoname,
		main.kubun2,
		main.syohingun,
		main.jyuutenitem,
		main.item_bunr_val1,
		main.item_bunr_val2,
		main.item_bunr_val3,
		main.bumon7_add_attr1,
		main.bumon7_add_attr2,
		main.bumon7_add_attr3,
		main.bumon7_add_attr4,
		main.bumon7_add_attr5,
		main.bumon7_add_attr6,
		main.bumon7_add_attr7,
		main.bumon7_add_attr8,
		main.bumon7_add_attr9,
		main.bumon7_add_attr10,
		main.bumon6_20kisyohingun,
		main.bumon6_20kinaieki1,
		main.bumon6_20kinaieki2,
		main.bumon6_20kinaieki3,
		main.bumon6_zyutensyohinyobi1,
		main.bumon6_zyutensyohinyobi2,
		main.bumon6_zyutensyohinyobi3,
		main.bumon6_zyutensyohinyobi4,
		main.bumon6_zyutensyohinyobi5,
		main.bumon6_okikaename,
		main.bumon6_zukyuyosoku1,
		main.bumon6_zukyuyosoku2,
		main.bumon6_zukyuyosoku3,
		row_number() OVER (
			PARTITION BY main.shukanengetubi,
			main.channel,
			main.konyuchubuncode,
			main.torikeikbn,
			main.tokuicode,
			main.hanbaiitem,
			main.hanbaiitem_kubun,
			main.juchkbn,
			main.juchkbncname,
			main.channel1,
			main.channel2,
			main.channel3,
			main.bmn_hyouji_cd,
			main.bmn_nms,
			main.henreasoncode,
			main.henreasonname,
			main.sokoname,
			main.kubun2,
			main.syohingun,
			main.jyuutenitem ORDER BY main.kosecode
			) AS num
	FROM main
	)
select * from final 