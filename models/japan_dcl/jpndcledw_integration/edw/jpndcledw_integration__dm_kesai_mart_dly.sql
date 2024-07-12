with dm_kesai_mart_dly_general as(
    select * from {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
),
cim01kokya as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM01KOKYA
),
cim08shkos_bunkai_qv as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.cim08shkos_bunkai_qv
),
edw_mds_jp_dcl_product_master as(
    select * from {{ ref('jpndcledw_integration__edw_mds_jp_dcl_product_master') }}
),
tm13item_qv as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tm13item_qv
),
kr_new_stage_point as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.kr_new_stage_point
),
transformed as(
SELECT 
    dly.kokyano::VARCHAR(60) as kokyano,
	dly.saleno_key::VARCHAR(83) as saleno_key,
	dly.saleno::VARCHAR(63) as saleno,
	to_date(dly.order_dt) as order_dt,
	to_date(dly.ship_dt) as ship_dt,
	dateadd('year', 1::BIGINT, CASE 
			WHEN dly.order_dt::CHARACTER VARYING::TEXT = 0::CHARACTER VARYING::TEXT
				THEN NULL::DATE
			ELSE dly.order_dt
			END::TIMESTAMP without TIME zone)::TIMESTAMP AS pl_order_dt,
	DATEADD('year', 1::BIGINT, CASE 
			WHEN dly.ship_dt::CHARACTER VARYING::TEXT = 0::CHARACTER VARYING::TEXT
				THEN NULL::DATE
			ELSE dly.ship_dt
			END::TIMESTAMP without TIME zone)::TIMESTAMP AS pl_ship_dt,
	dly.f_order::NUMBER(38,0) AS f_order,
	dly.f_ship445::NUMBER(38,0) AS f_ship445,
	dly.channel::VARCHAR(135) as channel,
	dly.juchkbn::VARCHAR(3) as juchkbn,
	dly.h_o_item_code::VARCHAR(30) AS kesai_itemcode,
	dly.h_o_item_name::VARCHAR(192) AS setitemnm,
	dly.anbun_amount_tax110_ex::NUMERIC(15, 5) AS total_price,
	dly.h_o_suryo::NUMERIC(18) AS qty,
	to_date(c.birthday::CHARACTER VARYING::TEXT, 'yyyymmdd'::CHARACTER VARYING::TEXT) AS "誕生日",
	c.rank::VARCHAR(12000) AS "顧客現在ランク",
	c.seibetukbn::VARCHAR(760) AS "性別コード",
	c.carrername::VARCHAR(12000) AS "職業",
	CASE 
		WHEN dly.h_o_item_code::TEXT = 'DUMMY'::CHARACTER VARYING::TEXT
			THEN NULL::CHARACTER VARYING
		ELSE dly.h_o_item_code
		END::VARCHAR(45) AS itemcode,
	item_bunkai.itemname::VARCHAR(192) as itemname,
	item_bunkai.itemcname::VARCHAR(240) as itemcname,
	item_bunkai.tanka::VARCHAR(21) as tanka,
	item_bunkai.chubuncode::VARCHAR(6000) as chubuncode,
	item_bunkai.chubunname::VARCHAR(100) as chubunname,
	item_bunkai.chubuncname::VARCHAR(6103) as chubuncname,
	item_bunkai.daibuncode::VARCHAR(7) as daibuncode,
	item_bunkai.daibunname::VARCHAR(100) as daibunname,
	item_bunkai.daibuncname::VARCHAR(110) as daibuncname,
	item_bunkai.daidaibuncode::VARCHAR(6000) as daidaibuncode,
	item_bunkai.daidaibunname::VARCHAR(100) as daidaibunname,
	item_bunkai.daidaibuncname::VARCHAR(6103) as daidaibuncname,
	dly.h_item_code::VARCHAR(65535) AS bunkai_itemcode,
	item_bunkai.bunkai_itemname::VARCHAR(65535) as bunkai_itemname,
	item_bunkai.bunkai_itemcname::VARCHAR(65535) as bunkai_itemcname,
	item_bunkai.bunkai_tanka::NUMERIC(38, 18) as bunkai_tanka,
	item_bunkai.bunkai_kossu::NUMERIC(36, 18) as bunkai_kossu,
	item_bunkai.bunkai_kosritu::float8 as bunkai_kosritu,
	dly.h_item_code::VARCHAR(45) AS kose_itemcode,
	COALESCE(dly.z_o_item_code, dly.h_item_code)::VARCHAR(45) AS koseiocode,
	COALESCE(dly.z_item_code, dly.h_item_code)::VARCHAR(65535) AS kosecode,
	CASE 
		WHEN dly.h_o_item_code::TEXT = 'DUMMY'::CHARACTER VARYING::TEXT
			THEN NULL::NUMERIC::NUMERIC(18, 0)
		ELSE dly.z_bun_suryo
		END::NUMERIC(14, 4) AS suryo,
	CASE 
		WHEN dly.h_o_item_code::TEXT = 'DUMMY'::CHARACTER VARYING::TEXT
			THEN NULL::NUMERIC::NUMERIC(18, 0)
		ELSE dly.z_koseritsu
		END::NUMERIC(18, 8) AS koseritsu,
	item_attr.mds_itemcode::VARCHAR(200) as mds_itemcode,
	item_attr.mds_itemname::VARCHAR(200) as mds_itemname,
	item_attr.attr01::VARCHAR(200) as attr01,
	item_attr.attr02::VARCHAR(200) as attr02,
	item_attr.attr03::VARCHAR(200) as attr03,
	item_attr.attr04::VARCHAR(200) as attr04,
	item_attr.attr05::VARCHAR(200) as attr05,
	item_attr.attr06::VARCHAR(200) as attr06,
	item_attr.attr07::VARCHAR(200) as attr07,
	item_attr.attr08::VARCHAR(200) as attr08,
	item_attr.attr09::VARCHAR(200) as attr09,
	item_attr.attr10::VARCHAR(200) as attr10,
	i13.i13_itemcode::VARCHAR(45) as i13_itemcode,
	i13.settanpinsetkbn::VARCHAR(12) as settanpinsetkbn,
	dly.teikikeiyaku::VARCHAR(6000) as teikikeiyaku,
	dly.f::NUMERIC(38, 18) as f,
	CASE 
		WHEN ls.stage IS NOT NULL
			THEN ls.stage
		ELSE 'レギュラー'::CHARACTER VARYING
		END::VARCHAR(18) AS latest_stage,
	ls.stage_ym::VARCHAR(9) as stage_ym,
	dly.y_order_f::NUMBER(38,0) as y_order_f,
	dly.y_ship_f::NUMBER(38,0) as y_ship_f
FROM dm_kesai_mart_dly_general dly
LEFT JOIN cim01kokya c ON dly.kokyano::TEXT = c.kokyano::TEXT
LEFT JOIN (
	SELECT cim08shkos_bunkai_qv.itemcode,
		cim08shkos_bunkai_qv.itemname,
		cim08shkos_bunkai_qv.itemcname,
		cim08shkos_bunkai_qv.tanka,
		cim08shkos_bunkai_qv.chubuncode,
		cim08shkos_bunkai_qv.chubunname,
		cim08shkos_bunkai_qv.chubuncname,
		cim08shkos_bunkai_qv.daibuncode,
		cim08shkos_bunkai_qv.daibunname,
		cim08shkos_bunkai_qv.daibuncname,
		cim08shkos_bunkai_qv.daidaibuncode,
		cim08shkos_bunkai_qv.daidaibunname,
		cim08shkos_bunkai_qv.daidaibuncname,
		cim08shkos_bunkai_qv.bunkai_itemcode,
		cim08shkos_bunkai_qv.bunkai_itemname,
		cim08shkos_bunkai_qv.bunkai_itemcname,
		cim08shkos_bunkai_qv.bunkai_tanka,
		cim08shkos_bunkai_qv.bunkai_kossu,
		cim08shkos_bunkai_qv.bunkai_kosritu
	FROM cim08shkos_bunkai_qv
	) item_bunkai ON dly.h_o_item_code::TEXT = item_bunkai.itemcode::TEXT
	AND dly.h_item_code::TEXT = item_bunkai.bunkai_itemcode::TEXT
LEFT JOIN (
	SELECT edw_mds_jp_dcl_product_master.itemcode AS mds_itemcode,
		edw_mds_jp_dcl_product_master.itemname AS mds_itemname,
		edw_mds_jp_dcl_product_master.attr01,
		edw_mds_jp_dcl_product_master.attr02,
		edw_mds_jp_dcl_product_master.attr03,
		edw_mds_jp_dcl_product_master.attr04,
		edw_mds_jp_dcl_product_master.attr05,
		edw_mds_jp_dcl_product_master.attr06,
		edw_mds_jp_dcl_product_master.attr07,
		edw_mds_jp_dcl_product_master.attr08,
		edw_mds_jp_dcl_product_master.attr09,
		edw_mds_jp_dcl_product_master.attr10
	FROM edw_mds_jp_dcl_product_master
	) item_attr ON dly.z_item_code::TEXT = item_attr.mds_itemcode::TEXT
LEFT JOIN (
	SELECT tm13item_qv.itemcode AS i13_itemcode,
		tm13item_qv.settanpinsetkbn,
		tm13item_qv.teikikeiyaku
	FROM tm13item_qv
	) i13 ON dly.h_o_item_code::TEXT = i13.i13_itemcode::TEXT
LEFT JOIN (
	SELECT a.yyyymm AS stage_ym,
		a.kokyano,
		a.stage
	FROM kr_new_stage_point a
	WHERE (
			EXISTS (
				SELECT 1
				FROM (
					SELECT "max" (kr_new_stage_point.yyyymm::TEXT) AS max_ym
					FROM kr_new_stage_point
					) stage_max_ym
				WHERE a.yyyymm::TEXT = stage_max_ym.max_ym
				)
			)
	) ls ON dly.kokyano::TEXT = ls.kokyano::TEXT
WHERE (
		dly.juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
		OR dly.juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
		OR dly.juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
		)
	AND dly.meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
)
select * from transformed