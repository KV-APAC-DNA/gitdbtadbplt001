with kesai_h_data_mart_mv as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_H_DATA_MART_MV
   -- TO_DATE(CAST(date_column AS STRING), 'YYYYMMDD') 
),
cim01kokya as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM01KOKYA
),
kokyano_list_016_manual as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KOKYANO_LIST_016_MANUAL
),
kesai_m_data_mart_mv as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_M_DATA_MART_MV 
),
cld_m as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CLD_M
),
kokyano_list AS (
    SELECT kokyano
    FROM kesai_h_data_mart_mv
    WHERE insertdate >= to_char(to_date(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp()))- 3, 'YYYYMMDD')::number
        OR updatedate >= to_char(to_date(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp()))- 3, 'YYYYMMDD')::number
    
    UNION
    
    SELECT kokyano
    FROM cim01kokya
    WHERE insertdate >= to_char(to_date(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp()))- 3, 'YYYYMMDD')::number
        OR updatedate >= to_char(to_date(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp()))- 3, 'YYYYMMDD')::number
    
    UNION
    
    SELECT kokyano
    FROM kokyano_list_016_manual
),
prev_ship AS (
SELECT kokyano,
    to_date(IFF(CAST(SHUKADATE AS STRING) = 0, NULL, CAST(SHUKADATE AS STRING)),'YYYYMMDD'),
    lag(to_date(IFF(CAST(SHUKADATE AS STRING) = 0, NULL, CAST(SHUKADATE AS STRING)),'YYYYMMDD'), 1) OVER (
        PARTITION BY kokyano ORDER BY to_date(IFF(CAST(SHUKADATE AS STRING) = 0, NULL, CAST(SHUKADATE AS STRING)),'YYYYMMDD')
        ) AS prev_ship_dt
FROM (
    SELECT a.kokyano,
        a.shukadate
    FROM kesai_h_data_mart_mv a
    INNER JOIN kokyano_list ON kokyano_list.kokyano = a.kokyano
    WHERE a.juchkbn IN (0, 1, 2)
        AND EXISTS (
            SELECT 1
            FROM kesai_m_data_mart_mv k
            WHERE k.saleno_key = a.saleno_key
                AND (
                    k.MEISAINUKIKINGAKU >= 1
                    OR k.MEISAINUKIKINGAKU < 0
                    )
            )
    GROUP BY a.kokyano,
        a.shukadate
    )
),
prev_order AS (
SELECT kokyano,
    to_date(IFF(CAST(juchdate AS STRING) = 0, NULL, CAST(juchdate AS STRING)),'YYYYMMDD') AS order_dt,
    lag(to_date(IFF(CAST(juchdate AS STRING) = 0, NULL, CAST(juchdate AS STRING)),'YYYYMMDD'), 1) OVER (
        PARTITION BY kokyano ORDER BY to_date(IFF(CAST(juchdate AS STRING) = 0, NULL, CAST(juchdate AS STRING)),'YYYYMMDD')
        ) AS prev_order_dt
FROM (
    SELECT a.kokyano,
        a.juchdate
    FROM kesai_h_data_mart_mv a
    INNER JOIN kokyano_list ON kokyano_list.kokyano = a.kokyano
    WHERE a.juchkbn IN (0, 1, 2)
        AND EXISTS (
            SELECT 1
            FROM kesai_m_data_mart_mv k
            WHERE k.saleno_key = a.saleno_key
                AND (
                    k.MEISAINUKIKINGAKU >= 1
                    OR k.MEISAINUKIKINGAKU < 0
                    )
            )
    GROUP BY a.kokyano,
        a.juchdate
    )
),
transformed as(

SELECT c.kokyano,
	h.saleno_key,
	h.SALENO,
	to_date(IFF(CAST(h.juchdate AS STRING) = 0, NULL, CAST(h.juchdate AS STRING)),'YYYYMMDD') as order_dt,
    to_date(IFF(CAST(h.SHUKADATE AS STRING) = 0, NULL, CAST(h.SHUKADATE AS STRING)),'YYYYMMDD') as ship_dt,
	prev_ship.prev_ship_dt,
	prev_order.prev_order_dt,
	h.daihanrobunname channel, -- "チャネル",
	h.juchkbn,
	DENSE_RANK() OVER (
		PARTITION BY c.kokyano ORDER BY to_date(IFF(CAST(h.SHUKADATE AS STRING) = 0, NULL, CAST(h.SHUKADATE AS STRING)),'YYYYMMDD')
		) RN_SHIP445,
	DENSE_RANK() OVER (
		PARTITION BY c.kokyano ORDER BY to_date(IFF(CAST(h.juchdate as STRING) = 0, NULL, CAST(h.juchdate AS STRING)),'YYYYMMDD')
		) RN_ORDER,
	DENSE_RANK() OVER (
		PARTITION BY c.kokyano,
		x.year_445 ORDER BY to_date(IFF(CAST(h.SHUKADATE AS STRING) = 0, NULL, CAST(h.SHUKADATE AS STRING)),'YYYYMMDD')
		) RN_SHIP445_YEARWISE,
	DENSE_RANK() OVER (
		PARTITION BY c.kokyano,
		CASE 
			WHEN h.juchdate = 0
				THEN NULL
			ELSE substring(h.juchdate, 1, 4)
			END ORDER BY to_date(IFF(CAST(h.juchdate as STRING) = 0, NULL, CAST(h.juchdate AS STRING)),'YYYYMMDD')
		) RN_ORDER_YEARWISE,
	to_date(IFF(CAST(h.juchdate as STRING) = 0, NULL, CAST(h.juchdate AS STRING)),'YYYYMMDD') + interval '365 days' AS pl_order_dt,
	 to_date(IFF(CAST(h.SHUKADATE AS STRING) = 0, NULL, CAST(h.SHUKADATE AS STRING)),'YYYYMMDD') + interval '365 days' AS pl_ship_dt,
	m.itemcode kesai_itemcode,
	m.setitemnm,
	m.MEISAINUKIKINGAKU total_price,
	m.suryo AS qty,
    TO_DATE(CAST(c.birthday AS STRING), 'YYYYMMDD') "誕生日",
	c.RANK AS "顧客現在ランク",
	c.SEIBETUKBN AS "性別コード",
	c.CARRERNAME AS "職業",
	x.year_445
FROM kesai_h_data_mart_mv h
INNER JOIN kokyano_list ON kokyano_list.kokyano = h.kokyano
LEFT JOIN prev_ship ON h.kokyano = prev_ship.kokyano
	AND  to_date(IFF(CAST(h.SHUKADATE AS STRING) = 0, NULL, CAST(h.SHUKADATE AS STRING)),'YYYYMMDD') = prev_ship.prev_ship_dt --- ヘッダーテーブル
LEFT JOIN prev_order ON h.kokyano = prev_order.kokyano
	AND to_date(IFF(CAST(h.juchdate as STRING) = 0, NULL, CAST(h.juchdate AS STRING)),'YYYYMMDD') = prev_order.order_dt
INNER JOIN cim01kokya c --- 顧客テーブル
	ON h.kokyano = c.kokyano
INNER JOIN kesai_m_data_mart_mv m --- 明細テーブル
	ON h.saleno_key = m.saleno_key
LEFT JOIN cld_m x ON to_date(IFF(CAST(h.SHUKADATE AS STRING) = 0, NULL, CAST(h.SHUKADATE AS STRING)),'YYYYMMDD') = x.ymd_dt
WHERE h.juchkbn IN (0, 1, 2) --返品除く
	AND (
		m.MEISAINUKIKINGAKU >= 1
		OR -- 1円以上
		m.MEISAINUKIKINGAKU < 0
		) -- 0円未満
	AND c.TESTUSRFLG <> 'テストユーザ' ---テストユーザ除く
	AND c.kokyano <> '9999999944' ---不特定顧客除く
	AND c.kokyano <> '0000000011'
)
select * from transformed