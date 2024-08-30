with kesai_h_data_mart_mv as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_mv') }} 
   -- TO_DATE(CAST(date_column AS STRING), 'YYYYMMDD') 
),
cim01kokya as(
    select * from {{ ref('jpndcledw_integration__cim01kokya') }}
),
kokyano_list_016_manual as(
    select * from {{ source('jpdcledw_integration','kokyano_list_016_manual') }} 
),
kesai_m_data_mart_mv as(
    select * from {{ ref('jpndcledw_integration__kesai_m_data_mart_mv') }} 
),
cld_m as(
    select * from {{ ref('jpndcledw_integration__cld_m') }} 
),
kokyano_list AS (
    SELECT kokyano
    FROM kesai_h_data_mart_mv
    WHERE insertdate >= to_char(to_date(CONVERT_TIMEZONE('Asia/Tokyo', current_timestamp()))- 1, 'YYYYMMDD')::number
        OR updatedate >= to_char(to_date(CONVERT_TIMEZONE('Asia/Tokyo', current_timestamp()))- 1, 'YYYYMMDD')::number
    
    UNION
    
    SELECT kokyano
    FROM cim01kokya
    WHERE insertdate >= to_char(to_date(CONVERT_TIMEZONE('Asia/Tokyo', current_timestamp()))- 1, 'YYYYMMDD')::number
        OR updatedate >= to_char(to_date(CONVERT_TIMEZONE('Asia/Tokyo', current_timestamp()))- 1, 'YYYYMMDD')::number
    
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
),
final as(
    select
        kokyano::varchar(60) as kokyano,
        saleno_key::varchar(44) as saleno_key,
        saleno::varchar(63) as saleno,
        to_date(order_dt) as order_dt,
        to_date(ship_dt) as ship_dt,
        to_date(prev_ship_dt) as prev_ship_dt,
        to_date(prev_order_dt) as prev_order_dt,
        channel::varchar(135) as channel,
        juchkbn::varchar(3) as juchkbn,
        rn_ship445::number(38,0) as rn_ship445,
        rn_order::number(38,0) as rn_order,
        rn_ship445_yearwise::number(38,0) as rn_ship445_yearwise,
        rn_order_yearwise::number(38,0) as rn_order_yearwise,
        pl_order_dt::timestamp_ntz(9) as pl_order_dt,
        pl_ship_dt::timestamp_ntz(9) as pl_ship_dt,
        kesai_itemcode::varchar(30) as kesai_itemcode,
        setitemnm::varchar(192) as setitemnm,
        total_price::number(18,0) as total_price,
        qty::number(18,0) as qty,
        "誕生日" as "誕生日",
        "顧客現在ランク"::varchar(12000) as "顧客現在ランク",
        "性別コード"::varchar(760) as "性別コード",
        "職業"::varchar(12000) as "職業",
        year_445::varchar(256) as year_445
    from transformed

)
select * from final