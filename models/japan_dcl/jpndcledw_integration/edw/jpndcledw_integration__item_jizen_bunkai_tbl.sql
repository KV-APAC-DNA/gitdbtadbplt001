WITH item_zaiko_tbl AS (
	SELECT * FROM dev_dna_core.snapjpdcledw_integration.item_zaiko_tbl
),
item_hanbai_tbl AS (
	SELECT * FROM dev_dna_core.snapjpdcledw_integration.item_hanbai_tbl
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
item_jizen_bunkai_w7 as(
    SELECT t.itemcode AS itemcode,
		sum(zaiko.tanka * t.suryo) AS kotankasum
	FROM delete_item_jizen_bunkai_w3_2 t
	LEFT OUTER JOIN item_jizen_bunkai_wz zaiko ON t.kosecode = zaiko.itemcode
	GROUP BY t.itemcode
),
item_jizen_bunkai_w06 as(
    	SELECT bom.itemcode AS itemcode,
		bom.kosecode AS kosecode,
		(
			CASE 
				WHEN zaiko.tanka = 0
					THEN CASE 
							WHEN w7.kotankasum = 0
								THEN 0
							ELSE round(bom.suryo * zaiko2.tanka / w7.kotankasum::DECIMAL, 8)
							END
				ELSE round(bom.suryo * zaiko2.tanka / zaiko.tanka::DECIMAL, 8)
				END
			) AS koseritsu
	FROM delete_item_jizen_bunkai_w3_2 bom
	LEFT JOIN item_jizen_bunkai_wz zaiko ON rtrim(bom.itemcode) = rtrim(zaiko.itemcode)
	LEFT JOIN item_jizen_bunkai_wz zaiko2 ON rtrim(bom.kosecode) = rtrim(zaiko2.itemcode)
	LEFT JOIN item_jizen_bunkai_w7 w7 ON rtrim(bom.itemcode) = rtrim(w7.itemcode)
),
item_jizen_bunkai_w08 as(
    	SELECT t.itemcode AS itemcode,
            sum(t.koseritsu) AS koseritsukei,
            1 - sum(t.koseritsu) AS sa
        FROM item_jizen_bunkai_w06 t
        WHERE t.koseritsu <> 0
        GROUP BY t.itemcode
        HAVING sum(t.koseritsu) <> 1
),
item_jizen_bunkai_w11 as(
        select 
        t1.itemcode as itemcode,
        t1.kosecode as kosecode,
        t1.koseritsu as koseritsu,
        t2.koseritsukei as koseritsukei,
        t2.sa as sa
        from 
            item_jizen_bunkai_w06 t1,
            item_jizen_bunkai_w08 t2
        where 
            t1.itemcode = t2.itemcode
),

updt_value as(
SELECT distinct ROUND(S1.SA * (item_jizen_bunkai_w06.KOSERITSU / S1.KOSERITSUKEI), 8) as val, s1.*
		FROM SNAPJPDCLEDW_INTEGRATION.ITEM_JIZEN_BUNKAI_W11 S1, item_jizen_bunkai_w06
		WHERE S1.ITEMCODE = item_jizen_bunkai_w06.ITEMCODE
			AND S1.KOSECODE = item_jizen_bunkai_w06.KOSECODE 
),

update_item_jizen_bunkai_w06 as(
    select item_jizen_bunkai_w06.itemcode as itemcode, 
    item_jizen_bunkai_w06.kosecode as kosecode,
    item_jizen_bunkai_w06.koseritsu+updt_value.val as koseritsu
    from item_jizen_bunkai_w06 left join updt_value on 
    updt_value.ITEMCODE = item_jizen_bunkai_w06.ITEMCODE
	AND updt_value.KOSECODE = item_jizen_bunkai_w06.KOSECODE
    left join ITEM_JIZEN_BUNKAI_W11 S2 on 
    S2.ITEMCODE = item_jizen_bunkai_w06.ITEMCODE
	AND S2.KOSECODE = item_jizen_bunkai_w06.KOSECODE AND S2.SA < 0
),    
item_jizen_bunkai_w17 as(
    	SELECT 
            t.itemcode AS itemcode,
            sum(t.koseritsu) AS koseritsukei,
            1 - sum(t.koseritsu) AS sa
        FROM update_item_jizen_bunkai_w06 t
        WHERE t.koseritsu <> 0
        GROUP BY t.itemcode
        HAVING sum(t.koseritsu) <> 1
),
item_jizen_bunkai_w13 as(
        select 
            bom.itemcode as itemcode,
            bom.kosecode as kosecode,
            bom.suryo as suryo
        from item_jizen_bunkai_w3 bom
        where 
        (
            bom.kosecode like '0081%'
            or bom.kosecode like '0086%'
        )
        and bom.itemcode not in 
        (
            select itemcode
            from item_jizen_bunkai_w11
            where sa < 0
        )
),
item_jizen_bunkai_w14 as(
     select 
        bom.itemcode as itemcode,
	    sum(bom.suryo) as kosecode_cnt
    from item_jizen_bunkai_w13 bom
    group by bom.itemcode
),
item_jizen_bunkai_w15 as(
	SELECT bom.itemcode AS itemcode,
		w10.kosecode AS kosecode,
		w10.suryo AS suryo,
		bom.koseritsukei AS koseritsukei,
		bom.sa AS sa,
		w8.kosecode_cnt AS kosecode_cnt
	FROM item_jizen_bunkai_w17 bom
	INNER JOIN item_jizen_bunkai_w13 w10 ON bom.itemcode = w10.itemcode
	INNER JOIN item_jizen_bunkai_w14 w8 ON bom.itemcode = w8.itemcode   
),
updt_value_2 as(
		SELECT ROUND(S1.SA * (S1.SURYO / S1.KOSECODE_CNT), 8) as val2, s1.*
		FROM ITEM_JIZEN_BUNKAI_W15 S1, update_item_jizen_bunkai_w06
		WHERE S1.ITEMCODE = update_item_jizen_bunkai_w06.ITEMCODE
			AND S1.KOSECODE = update_item_jizen_bunkai_w06.KOSECODE
),
update_item_jizen_bunkai_w06_2 as(
    select update_item_jizen_bunkai_w06.itemcode as itemcode, 
    update_item_jizen_bunkai_w06.kosecode as kosecode,
    update_item_jizen_bunkai_w06.koseritsu+updt_value_2.val2 as koseritsu
    from update_item_jizen_bunkai_w06 left join updt_value_2 on 
    updt_value_2.ITEMCODE = update_item_jizen_bunkai_w06.ITEMCODE
	AND updt_value_2.KOSECODE = update_item_jizen_bunkai_w06.KOSECODE
    left join ITEM_JIZEN_BUNKAI_W15 S2 on 
    S2.ITEMCODE = update_item_jizen_bunkai_w06.ITEMCODE
	AND S2.KOSECODE = update_item_jizen_bunkai_w06.KOSECODE
),

item_jizen_bunkai_w16 as(
	SELECT 
		t.itemcode AS itemcode,
		sum(t.koseritsu) AS koseritsukei,
		1 - sum(t.koseritsu) AS sa
	FROM update_item_jizen_bunkai_w06_2 t
	WHERE t.koseritsu <> 0
	GROUP BY t.itemcode
	HAVING sum(t.koseritsu) <> 1  
),
item_jizen_bunkai_w10 as(
	SELECT bom.itemcode AS itemcode,
		bom.kosecode AS kosecode,
		bom.suryo AS suryo
	FROM item_jizen_bunkai_w3 bom
	LEFT JOIN item_jizen_bunkai_wz zaiko2 ON bom.kosecode = zaiko2.itemcode
	WHERE (bom.suryo * zaiko2.tanka) = 0
		AND bom.itemcode NOT IN (
			SELECT itemcode
			FROM item_jizen_bunkai_w11
			WHERE sa < 0
			)   
),
item_jizen_bunkai_w8 as(
    	SELECT bom.itemcode AS itemcode,
            sum(bom.suryo) AS kosecode_cnt
        FROM item_jizen_bunkai_w10 bom
        GROUP BY bom.itemcode
),
item_jizen_bunkai_w9 as(
	SELECT 
		bom.itemcode AS itemcode,
		w10.kosecode AS kosecode,
		w10.suryo AS suryo,
		bom.koseritsukei AS koseritsukei,
		bom.sa AS sa,
		w8.kosecode_cnt AS kosecode_cnt
	FROM item_jizen_bunkai_w16 bom
	INNER JOIN item_jizen_bunkai_w10 w10 ON bom.itemcode = w10.itemcode
	INNER JOIN item_jizen_bunkai_w8 w8 ON bom.itemcode = w8.itemcode    
),
updt_value_3 as(
    SELECT ROUND(S1.SA * (S1.SURYO / S1.KOSECODE_CNT), 8) as val3, s1.*
		FROM ITEM_JIZEN_BUNKAI_W9 S1, update_item_jizen_bunkai_w06_2
		WHERE S1.ITEMCODE = update_item_jizen_bunkai_w06_2.ITEMCODE
			AND S1.KOSECODE = update_item_jizen_bunkai_w06_2.KOSECODE
),
update_item_jizen_bunkai_w06_3 as(

    select update_item_jizen_bunkai_w06_2.itemcode as itemcode, 
    update_item_jizen_bunkai_w06_2.kosecode as kosecode,
    update_item_jizen_bunkai_w06_2.koseritsu+updt_value_3.val3 as koseritsu
    from update_item_jizen_bunkai_w06_2 left join updt_value_3 on 
    updt_value_3.ITEMCODE = update_item_jizen_bunkai_w06_2.ITEMCODE
	AND updt_value_3.KOSECODE = update_item_jizen_bunkai_w06_2.KOSECODE
    left join ITEM_JIZEN_BUNKAI_W9 S2 on 
    S2.ITEMCODE = update_item_jizen_bunkai_w06_2.ITEMCODE
	AND S2.KOSECODE = update_item_jizen_bunkai_w06_2.KOSECODE
),
item_jizen_bunkai_w07 as(
	SELECT 
		t.itemcode AS itemcode,
		s.kosecodemax AS kosecode,
		t.koseritsumax AS koseritsu
	FROM (
		SELECT 
			t1.itemcode AS itemcode,
			max(t1.koseritsu) AS koseritsumax
		FROM update_item_jizen_bunkai_w06_3 t1
		GROUP BY t1.itemcode
		) t
	JOIN (
		SELECT 
			s1.itemcode AS itemcode,
			s1.koseritsu AS koseritsu,
			max(s1.kosecode) AS kosecodemax
		FROM update_item_jizen_bunkai_w06_3 s1
		GROUP BY s1.itemcode,
			s1.koseritsu
		) s ON t.itemcode = s.itemcode
		AND t.koseritsumax = s.koseritsu    
),
item_jizen_bunkai_w082 as(
    SELECT
        t.itemcode,
        sum(t.koseritsu) AS koseritsukei,
        1 - sum(t.koseritsu) AS sa
    FROM update_item_jizen_bunkai_w06_3 AS t
    WHERE t.koseritsu <> 0
    GROUP BY t.itemcode
    HAVING sum(t.koseritsu) <> 1    
),
updt_value_4 as(
        SELECT S1.SA as val4, s1.*
		FROM ITEM_JIZEN_BUNKAI_W082 S1, update_item_jizen_bunkai_w06_3
		WHERE S1.ITEMCODE = update_item_jizen_bunkai_w06_3.ITEMCODE
),
update_item_jizen_bunkai_w06_4 as(
select update_item_jizen_bunkai_w06_3.itemcode as itemcode, 
    update_item_jizen_bunkai_w06_3.kosecode as kosecode,
    update_item_jizen_bunkai_w06_3.koseritsu+updt_value_4.val4 as koseritsu
    from update_item_jizen_bunkai_w06_3 left join updt_value_4 on 
    updt_value_4.ITEMCODE = update_item_jizen_bunkai_w06_3.ITEMCODE
    left join ITEM_JIZEN_BUNKAI_W082 S2 on 
    S2.ITEMCODE = update_item_jizen_bunkai_w06_3.ITEMCODE
    left join ITEM_JIZEN_BUNKAI_W07 S3
	on S3.ITEMCODE = update_item_jizen_bunkai_w06_3.ITEMCODE and S3.KOSECODE = update_item_jizen_bunkai_w06_3.KOSECODE
),
item_jizen_bunkai_wend as(
	SELECT 
		a.itemcode AS itemcode,
		a.kosecode AS kosecode,
		a.koseritsu AS koseritsu,
		b.suryo AS suryo
	FROM update_item_jizen_bunkai_w06_4 a,
		item_jizen_bunkai_w3 b
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
SELECT ROUND(SHKOS.SURYO / T1.SURYO, 8) AS KOSERITSU, SHKOS.*
		FROM ITEM_JIZEN_BUNKAI_WEND SHKOS
		INNER JOIN ITEM_JIZEN_BUNKAI_W012 T1 ON SHKOS.ITEMCODE = T1.ITEMCODE
		WHERE SHKOS.ITEMCODE = ITEM_JIZEN_BUNKAI_WEND.ITEMCODE
			AND SHKOS.KOSECODE = ITEM_JIZEN_BUNKAI_WEND.KOSECODE
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
SELECT ROUND(S1.SA * (jp_dcl_edw.ITEM_JIZEN_BUNKAI_WEND.KOSERITSU / S1.KOSERITSUKEI), 8) as val6, s1.*
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
    left join ITEM_JIZEN_BUNKAI_W012 S2 on 
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
    update_item_jizen_bunkai_wend_2.KOSERITSU+updt_value_7.val6 as koseritsu,
    update_item_jizen_bunkai_wend_2.suryo as suryo
    from update_item_jizen_bunkai_wend_2 left join updt_value_7 on 
    updt_value_7.ITEMCODE = update_item_jizen_bunkai_wend_2.ITEMCODE
    left join ITEM_JIZEN_BUNKAI_W083 S2 on 
    S2.ITEMCODE = update_item_jizen_bunkai_wend_2.ITEMCODE
    left join ITEM_JIZEN_BUNKAI_W071 S3
	on S3.ITEMCODE = update_item_jizen_bunkai_wend_2.ITEMCODE and S3.KOSECODE = update_item_jizen_bunkai_wend_2.KOSECODE
),
item_jizen_bunkai_wend_2 as(
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
			FROM DEV_DNA_CORE.sm05_workspace.jpndcledw_integration__item_jizen_bunkai_wz w3
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
			FROM DEV_DNA_CORE.sm05_workspace.jpndcledw_integration__item_jizen_bunkai_wz w3
			WHERE w3.itemcode = m03.itemcode
			)
),
item_jizen_bunkai_tbl as(
    SELECT 
		w14.itemcode as itemcode,
		w14.kosecode as kosecode,
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
        tm14.item_cd as itemcode,
        tm14.kosei_cd as kosecode,
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
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_tz(9) as updated_date,
        null:: varchar(100) as updated_by,
        marker::number(38,0) as marker
    from item_jizen_bunkai_tbl_2
)
select * from final