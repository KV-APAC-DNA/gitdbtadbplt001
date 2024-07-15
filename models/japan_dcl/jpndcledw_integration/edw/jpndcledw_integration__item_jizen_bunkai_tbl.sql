WITH item_zaiko_tbl AS (
	SELECT * FROM {{ ref('jpndcledw_integration__item_zaiko_tbl') }}
),
item_hanbai_tbl AS (
	SELECT * FROM {{ ref('jpndcledw_integration__item_hanbai_tbl') }}
),
bom_sap_v AS (
	SELECT * FROM dev_dna_core.snapjpdcledw_integration.bom_sap_v
),
item_bom_ikou_kizuna AS (
	SELECT * FROM dev_dna_core.snapjpdcledw_integration.item_bom_ikou_kizuna
),
tm14shkos_mainte_work AS (
	 SELECT * FROM dev_dna_core.snapjpdcledw_integration.tm14shkos_mainte_work
),
C_TBECPRIVILEGEMST AS (
	 SELECT * FROM {{ ref('jpndclitg_integration__c_tbecprivilegemst') }}
),
item_jizen_bunkai_w06 AS (
	 SELECT * FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
),
item_jizen_bunkai_wz AS (
	SELECT z_itemcode AS itemcode,
		tanka
	FROM item_zaiko_tbl
	WHERE marker IN (1, 2)
	GROUP BY z_itemcode,
		tanka
    
),
item_jizen_bunkai_wh as(
    SELECT h_itemcode AS itemcode,
		tanka
	FROM item_hanbai_tbl
	WHERE marker IN (1, 2, 3)
	GROUP BY h_itemcode,
		tanka
),
item_jizen_bunkai_w1 as(
    	SELECT bom_sap.oya_hin_cd AS itemcode,
		bom_sap.kos_hin_cd AS kosecode,
		nvl(bom_sap.kos_qut, 0)::NUMERIC(28, 4) AS suryo
	FROM bom_sap_v bom_sap
	WHERE bom_sap.kos_hin_cd NOT LIKE '009%'
		AND bom_sap.kos_hin_cd NOT LIKE '0082%'
		AND bom_sap.kos_hin_cd NOT LIKE '019%'
		AND bom_sap.kos_hin_cd NOT LIKE '0182%'
	
	UNION
	
	SELECT bom_ikou.oya_hin_cd AS itemcode,
		bom_ikou.kos_hin_cd AS kosecode,
		nvl(bom_ikou.kos_qut, 0)::NUMERIC(28, 4) AS suryo
	FROM item_bom_ikou_kizuna bom_ikou
	WHERE NOT EXISTS (
			SELECT 1
			FROM bom_sap_v bom_sap
			WHERE bom_sap.oya_hin_cd = bom_ikou.oya_hin_cd
			)
),
delete_item_jizen_bunkai_w1 as(
    select * from item_jizen_bunkai_w1
    minus
    select * from item_jizen_bunkai_w1
    WHERE NOT EXISTS (
		SELECT 'X'
		FROM ITEM_JIZEN_BUNKAI_WZ T1
		WHERE T1.ITEMCODE = ITEM_JIZEN_BUNKAI_W1.ITEMCODE
		)
	OR NOT EXISTS (
		SELECT 'X'
		FROM ITEM_JIZEN_BUNKAI_WZ T2
		WHERE T2.ITEMCODE = ITEM_JIZEN_BUNKAI_W1.KOSECODE
		)
	OR ITEM_JIZEN_BUNKAI_W1.ITEMCODE = ITEM_JIZEN_BUNKAI_W1.KOSECODE

),
ITEM_JIZEN_BUNKAI_W2 as (
    	SELECT 2 AS kaisou,
		t1.itemcode AS itemcode,
		t1.kosecode AS kosecode,
		t1.suryo AS suryo
	FROM delete_item_jizen_bunkai_w1 t1
	
	UNION ALL
	
	SELECT 3 AS kaisou,
		t1.itemcode AS itemcode,
		t2.kosecode AS kosecode,
		round(t1.suryo * t2.suryo, 4) AS suryo
	FROM delete_item_jizen_bunkai_w1 t1
	JOIN delete_item_jizen_bunkai_w1 t2 ON t1.kosecode = t2.itemcode
	
	UNION ALL
	
	SELECT 4 AS kaisou,
		t1.itemcode AS itemcode,
		t3.kosecode AS kosecode,
		round(t1.suryo * t2.suryo * t3.suryo, 4) AS suryo
	FROM delete_item_jizen_bunkai_w1 t1
	JOIN delete_item_jizen_bunkai_w1 t2 ON t1.kosecode = t2.itemcode
	JOIN delete_item_jizen_bunkai_w1 t3 ON t2.kosecode = t3.itemcode
	
	UNION ALL
	
	SELECT 5 AS kaisou,
		t1.itemcode AS itemcode,
		t4.kosecode AS kosecode,
		round(t1.suryo * t2.suryo * t3.suryo * t4.suryo, 4) AS suryo
	FROM delete_item_jizen_bunkai_w1 t1
	JOIN delete_item_jizen_bunkai_w1 t2 ON t1.kosecode = t2.itemcode
	JOIN delete_item_jizen_bunkai_w1 t3 ON t2.kosecode = t3.itemcode
	JOIN delete_item_jizen_bunkai_w1 t4 ON t3.kosecode = t4.itemcode
	
	UNION ALL
	
	SELECT 6 AS kaisou,
		t1.itemcode AS itemcode,
		t5.kosecode AS kosecode,
		round(t1.suryo * t2.suryo * t3.suryo * t4.suryo * t5.suryo, 4) AS suryo
	FROM delete_item_jizen_bunkai_w1 t1
	JOIN delete_item_jizen_bunkai_w1 t2 ON t1.kosecode = t2.itemcode
	JOIN delete_item_jizen_bunkai_w1 t3 ON t2.kosecode = t3.itemcode
	JOIN delete_item_jizen_bunkai_w1 t4 ON t3.kosecode = t4.itemcode
	JOIN delete_item_jizen_bunkai_w1 t5 ON t4.kosecode = t5.itemcode
),
delete_item_jizen_bunkai_w2 as(
select * from item_jizen_bunkai_w2
minus
select * from item_jizen_bunkai_w2
WHERE EXISTS (
		SELECT 'X'
		FROM ITEM_JIZEN_BUNKAI_W2 T2
		WHERE ITEM_JIZEN_BUNKAI_W2.KOSECODE = T2.ITEMCODE
		)
),
item_jizen_bunkai_w3 as(
    SELECT T.ITEMCODE AS ITEMCODE,
        T.KOSECODE AS KOSECODE,
        SUM(T.SURYO) AS SURYO
    FROM delete_item_jizen_bunkai_w2 T
    GROUP BY T.ITEMCODE,
        T.KOSECODE
),
delete_item_jizen_bunkai_w3 as(
select * from ITEM_JIZEN_BUNKAI_W3
minus
select * from ITEM_JIZEN_BUNKAI_W3
WHERE EXISTS (
		SELECT 'X'
		FROM TM14SHKOS_MAINTE_WORK T2
		WHERE ITEM_JIZEN_BUNKAI_W3.ITEMCODE = T2.ITEMCODE
			AND T2.KOSECODE NOT LIKE '009%'
			AND T2.KOSECODE NOT LIKE '0082%'
			AND T2.KOSECODE NOT LIKE '019%'
			AND T2.KOSECODE NOT LIKE '0182%'
		)
),
item_jizen_bunkai_w3_2 as (
    select * from delete_item_jizen_bunkai_w3 union all
    SELECT TM14.ITEMCODE AS ITEMCODE,
	TM14.KOSECODE AS KOSECODE,
	TM14.SURYO AS SURYO
    FROM TM14SHKOS_MAINTE_WORK TM14
    WHERE TM14.KOSECODE NOT LIKE '009%'
        AND TM14.KOSECODE NOT LIKE '0082%'
        AND TM14.KOSECODE NOT LIKE '019%'
        AND TM14.KOSECODE NOT LIKE '0182%'
),
item_jizen_bunkai_w91 as(
    	SELECT tm14.itemcode AS itemcode,
		count(*) AS cnt
        FROM item_jizen_bunkai_w3_2 tm14
        GROUP BY tm14.itemcode
),
item_jizen_bunkai_w92 as(
    	SELECT TM14.ITEMCODE AS ITEMCODE,
            COUNT(*) AS CNT
        FROM item_jizen_bunkai_w3_2 TM14
        WHERE TM14.KOSECODE LIKE '0083%'
            OR TM14.KOSECODE LIKE '0084%'
            OR TM14.KOSECODE LIKE '0085%' GROUP BY tm14.itemcode
),
item_jizen_bunkai_w93 as(
    	SELECT T1.ITEMCODE AS ITEMCODE
        FROM ITEM_JIZEN_BUNKAI_W91 T1,
            ITEM_JIZEN_BUNKAI_W92 T2
        WHERE rtrim(T1.ITEMCODE) = rtrim(T2.ITEMCODE)
            AND trim(T1.CNT) <> trim(T2.CNT)
),
item_jizen_bunkai_w99 as(
    SELECT TM14.ITEMCODE AS ITEMCODE,
	TM14.KOSECODE AS KOSECODE,
	TM14.SURYO AS SURYO
    FROM ITEM_JIZEN_BUNKAI_W3_2 TM14,
        ITEM_JIZEN_BUNKAI_W93 T1
    WHERE TM14.ITEMCODE = T1.ITEMCODE
        AND (
            TM14.KOSECODE LIKE '0083%'
            OR TM14.KOSECODE LIKE '0084%'
            OR TM14.KOSECODE LIKE '0085%'
            )
),
delete_item_jizen_bunkai_w3_2 as(
    select * from ITEM_JIZEN_BUNKAI_W3_2
    minus
    select *
    FROM item_jizen_bunkai_w3_2
WHERE EXISTS (
		SELECT 'X'
		FROM ITEM_JIZEN_BUNKAI_W99 T2
		WHERE ITEM_JIZEN_BUNKAI_W3_2.KOSECODE = T2.KOSECODE
		)
),
item_jizen_bunkai_wend as(
	SELECT 
		a.itemcode AS itemcode,
		a.kosecode AS kosecode,
		a.koseritsu AS koseritsu,
		b.suryo AS suryo
	FROM item_jizen_bunkai_w06 a,
		item_jizen_bunkai_w3_2 b
	WHERE a.itemcode = b.itemcode
		AND a.kosecode = b.kosecode 
),
item_jizen_bunkai_w012 as(
	SELECT 
		itemcode AS itemcode,
		sum(suryo) AS suryo
	FROM item_jizen_bunkai_wend
	GROUP BY itemcode
	HAVING sum(koseritsu) = 0 
),
updt_value_5 as(
SELECT  SHKOS.itemcode AS itemcode,
		SHKOS.kosecode AS kosecode,
		ROUND(SHKOS.SURYO / T1.SURYO, 8) AS KOSERITSU,
		SHKOS.suryo AS suryo
		FROM ITEM_JIZEN_BUNKAI_WEND SHKOS
		INNER JOIN ITEM_JIZEN_BUNKAI_W012 T1 ON SHKOS.ITEMCODE = T1.ITEMCODE
		-- WHERE SHKOS.ITEMCODE = ITEM_JIZEN_BUNKAI_WEND.ITEMCODE
		-- 	AND SHKOS.KOSECODE = ITEM_JIZEN_BUNKAI_WEND.KOSECODE
),
update_item_jizen_bunkai_wend as(
select item_jizen_bunkai_wend.itemcode as itemcode, 
    item_jizen_bunkai_wend.kosecode as kosecode,
    updt_value_5.KOSERITSU as koseritsu,
    item_jizen_bunkai_wend.suryo as suryo
    from item_jizen_bunkai_wend left join updt_value_5 on 
    updt_value_5.ITEMCODE = item_jizen_bunkai_wend.ITEMCODE
	AND updt_value_5.KOSECODE = item_jizen_bunkai_wend.KOSECODE
    left join ITEM_JIZEN_BUNKAI_W012 T2 on 
    T2.ITEMCODE = ITEM_JIZEN_BUNKAI_WEND.ITEMCODE
),
item_jizen_bunkai_w081 as(
    SELECT 
		t.itemcode AS itemcode,
		sum(t.koseritsu) AS koseritsukei,
		1 - sum(t.koseritsu) AS sa
	FROM update_item_jizen_bunkai_wend t
	WHERE t.koseritsu <> 0
	GROUP BY t.itemcode
	HAVING sum(t.koseritsu) <> 1
),
item_jizen_bunkai_w12 as(
    SELECT T1.ITEMCODE AS ITEMCODE,
        T1.KOSECODE AS KOSECODE,
        T1.KOSERITSU AS KOSERITSU,
        T2.KOSERITSUKEI AS KOSERITSUKEI,
        T2.SA AS SA
    FROM update_item_jizen_bunkai_wend T1,
        ITEM_JIZEN_BUNKAI_W081 T2
    WHERE T1.ITEMCODE = T2.ITEMCODE
),
updt_value_6 as(
SELECT ROUND(S1.SA * (update_item_jizen_bunkai_wend.KOSERITSU / S1.KOSERITSUKEI), 8) as val6, s1.*
		FROM ITEM_JIZEN_BUNKAI_W12 S1, update_item_jizen_bunkai_wend
		WHERE S1.ITEMCODE = update_item_jizen_bunkai_wend.ITEMCODE
			AND S1.KOSECODE = update_item_jizen_bunkai_wend.KOSECODE
),
update_item_jizen_bunkai_wend_2 as(
select update_item_jizen_bunkai_wend.itemcode as itemcode, 
    update_item_jizen_bunkai_wend.kosecode as kosecode,
    update_item_jizen_bunkai_wend.KOSERITSU+updt_value_6.val6 as koseritsu,
    update_item_jizen_bunkai_wend.suryo as suryo
    from update_item_jizen_bunkai_wend left join updt_value_6 on 
    updt_value_6.ITEMCODE = update_item_jizen_bunkai_wend.ITEMCODE
	AND updt_value_6.KOSECODE = update_item_jizen_bunkai_wend.KOSECODE
    left join ITEM_JIZEN_BUNKAI_W12 S2 on 
    S2.ITEMCODE = update_item_jizen_bunkai_wend.ITEMCODE
	AND S2.KOSECODE = update_item_jizen_bunkai_wend.KOSECODE
),
item_jizen_bunkai_w071 as(
    SELECT T.ITEMCODE AS ITEMCODE,
	S.KOSECODEMAX AS KOSECODE,
	T.KOSERITSUMAX AS KOSERITSU
FROM (
	SELECT T1.ITEMCODE AS ITEMCODE,
		MAX(T1.KOSERITSU) AS KOSERITSUMAX
	FROM update_item_jizen_bunkai_wend_2 T1
	GROUP BY T1.ITEMCODE
	) T
JOIN (
	SELECT S1.ITEMCODE AS ITEMCODE,
		S1.KOSERITSU AS KOSERITSU,
		MAX(S1.KOSECODE) AS KOSECODEMAX
	FROM update_item_jizen_bunkai_wend_2 S1
	GROUP BY S1.ITEMCODE,
		S1.KOSERITSU
	) S ON T.ITEMCODE = S.ITEMCODE
	AND T.KOSERITSUMAX = S.KOSERITSU
),
item_jizen_bunkai_w083 as(
    SELECT 
		t.itemcode AS itemcode,
		sum(t.koseritsu) AS koseritsukei,
		1 - sum(t.koseritsu) AS sa
	FROM update_item_jizen_bunkai_wend_2 t
	WHERE t.koseritsu <> 0
	GROUP BY t.itemcode
	HAVING sum(t.koseritsu) <> 1
),
updt_value_7 as(
    SELECT S1.SA as val7, s1.*
		FROM ITEM_JIZEN_BUNKAI_W083 S1, update_item_jizen_bunkai_wend_2
		WHERE S1.ITEMCODE = update_item_jizen_bunkai_wend_2.ITEMCODE
),
update_item_jizen_bunkai_wend_3 as(
select update_item_jizen_bunkai_wend_2.itemcode as itemcode, 
    update_item_jizen_bunkai_wend_2.kosecode as kosecode,
    update_item_jizen_bunkai_wend_2.KOSERITSU+updt_value_7.val7 as koseritsu,
    update_item_jizen_bunkai_wend_2.suryo as suryo
    from update_item_jizen_bunkai_wend_2 left join updt_value_7 on 
    updt_value_7.ITEMCODE = update_item_jizen_bunkai_wend_2.ITEMCODE
    left join ITEM_JIZEN_BUNKAI_W083 S2 on 
    S2.ITEMCODE = update_item_jizen_bunkai_wend_2.ITEMCODE
    left join ITEM_JIZEN_BUNKAI_W071 S3
	on S3.ITEMCODE = update_item_jizen_bunkai_wend_2.ITEMCODE and S3.KOSECODE = update_item_jizen_bunkai_wend_2.KOSECODE
),
item_jizen_bunkai_wend_2 as(
select * from update_item_jizen_bunkai_wend_3 union all
    SELECT ITEMCODE,
        KOSECODE,
        SURYO,
        0
    FROM ITEM_JIZEN_BUNKAI_W99
),
item_jizen_bunkai_wend1 as(
	SELECT itemcode,
		kosecode,
		suryo,
		koseritsu,
		1 AS bunkaikbn
	FROM update_item_jizen_bunkai_wend_3
	
	UNION ALL
	
	SELECT m03.itemcode AS itemcode,
		m03.itemcode AS kosecode,
		1 AS suryo,
		1 AS koseritsu,
		0 AS bunkaikbn
	FROM item_jizen_bunkai_wz m03
	WHERE NOT EXISTS (
			SELECT 'X'
			FROM item_jizen_bunkai_wz w3
			WHERE w3.itemcode = m03.itemcode
			)
	
	UNION ALL
	
	SELECT m03.itemcode AS itemcode,
		m03.itemcode AS kosecode,
		1 AS suryo,
		1 AS koseritsu,
		0 AS bunkaikbn
	FROM item_jizen_bunkai_wh m03
	WHERE NOT EXISTS (
			SELECT 'X'
			FROM item_jizen_bunkai_wz w3
			WHERE w3.itemcode = m03.itemcode
			)
),
item_jizen_bunkai_tbl as(
    SELECT 
		w14.itemcode as item_cd,
		w14.kosecode as kosei_cd,
		w14.suryo as suryo,
		w14.koseritsu as koseritsu,
		cast(to_char(current_timestamp(), 'YYYYMMDD') as numeric) as insertdate,
		cast(to_char(current_timestamp(), 'HH24MISS') as numeric) as inserttime,
		'DWH401' as insertid,
		bunkaikbn as bunkaikbn,
		1 as marker
	FROM item_jizen_bunkai_wend1 w14
),
item_jizen_bunkai_maint as(
    select
        tm14.item_cd as item_cd,
        tm14.kosei_cd as kosei_cd,
        tm14.suryo,
        tm14.koseritsu
    from item_jizen_bunkai_tbl as tm14
    where tm14.item_cd in (
        select w.itemcode
        from tm14shkos_mainte_work as w
    )
),
item_jizen_bunkai_tbl_2 as(
select * from item_jizen_bunkai_tbl
union all
    SELECT c_diprivilegeid,
	c_diprivilegeid,
	'1' as suryo,
	'1' as koseritsu,
    null as insertdate,
    null as inserttime,
    null as insertid,
    null as bunkaikbn,
	'2' as MARKER
    FROM c_tbecprivilegemst
    union ALL
    SELECT 
  'X000000001' as item_cd,
	'X000000001' as kosei_cd,
	1 as suryo,
	1 as koseritsu,
    null as insertdate,
    null as inserttime,
    null as insertid,
    null as bunkaikbn,
	3 as MARKER

UNION ALL

SELECT 
    'X000000002' as item_cd,
	'X000000002' as kosei_cd,
	1 as suryo,
	1 as koseritsu,
    null as insertdate,
    null as inserttime,
    null as insertid,
    null as bunkaikbn,
	3 as MARKER
),
final as(
    select 
        item_cd::varchar(45) as item_cd,
        kosei_cd::varchar(45) as kosei_cd,
        suryo::number(13,4) as suryo,
        koseritsu::number(16,8) as koseritsu,
        insertdate::number(18,0) as insertdate,
        inserttime::number(18,0) as inserttime,
        insertid::varchar(9) as insertid,
        bunkaikbn::varchar(1) as bunkaikbn,
        current_timestamp()::timestamp_tz(9) as inserted_date,
        NULL::varchar(100) as inserted_by,
        current_timestamp()::timestamp_tz(9) as updated_date,
        NULL::varchar(100) as updated_by,
        marker::number(38,0) as marker
    from item_jizen_bunkai_tbl_2
)
select * from final