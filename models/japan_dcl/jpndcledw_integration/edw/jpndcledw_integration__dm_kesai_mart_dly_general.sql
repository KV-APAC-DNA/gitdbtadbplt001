{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
    )
}}


with kesai_h_data_mart_mv_kizuna as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_mv') }}
),
kesai_m_data_mart_mv_kizuna as(
    select * from {{ ref('jpndcledw_integration__kesai_m_data_mart_mv') }}
),
cim01kokya as(
    select * from  {{ ref('jpndcledw_integration__cim01kokya') }}
),
tbpromotion as(
    select * from {{ ref('jpndclitg_integration__tbpromotion') }}
),
c_tbecclient as(
    select * from {{ ref('jpndclitg_integration__c_tbecclient') }}
),
cld_m as(
    select * from {{ ref('jpndcledw_integration__cld_m') }} 
),
TEIKIKEIYAKU_DATA_MART_UNI as(
    select * from {{ ref('jpndcledw_integration__teikikeiyaku_data_mart_uni') }}
),
SYOUHINCD_HENKAN_QV as(
    select * from  {{ ref('jpndcledw_integration__syouhincd_henkan_qv') }}
),
TM14SHKOS_QV as(
    select * from {{ ref('jpndcledw_integration__tm14shkos_qv') }}
),
CIM08SHKOS_BUNKAI_QV as(
    select * from  {{ ref('jpndcledw_integration__cim08shkos_bunkai_qv') }}
),
TM13ITEM_QV as(
    select * from  {{ ref('jpndcledw_integration__tm13item_qv') }}
),
kr_frequency_1yn_900_kizuna as(
    select * from {{ ref('jpndcledw_integration__kr_frequency_1yn_900_kizuna') }}
),
prev_ship AS (
    SELECT kokyano,
        to_date(IFF(CAST(SHUKADATE AS STRING) = 0, NULL, CAST(SHUKADATE AS STRING)),'YYYYMMDD') AS ship_dt,
        lag(to_date(IFF(CAST(SHUKADATE AS STRING) = 0, NULL, CAST(SHUKADATE AS STRING)),'YYYYMMDD'), 1) OVER (
            PARTITION BY kokyano ORDER BY to_date(IFF(CAST(SHUKADATE AS STRING) = 0, NULL, CAST(SHUKADATE AS STRING)),'YYYYMMDD')
            ) AS prev_ship_dt
    FROM (
        SELECT a.kokyano,
            a.shukadate
        FROM kesai_h_data_mart_mv_kizuna a
        WHERE a.juchkbn IN (0, 1, 2)
            AND EXISTS (
                SELECT 1
                FROM kesai_m_data_mart_mv_kizuna k
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
        FROM kesai_h_data_mart_mv_kizuna a
        WHERE a.juchkbn IN (0, 1, 2)
            AND EXISTS (
                SELECT 1
                FROM kesai_m_data_mart_mv_kizuna k
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
amount_header AS (
    SELECT saleno_key,
        saleno,
        (ditoujitsuhaisoprc + didiscountall) AS discount,
        ROUND(riyopoint::FLOAT / ((100 + ditaxrate)::FLOAT / 100::FLOAT)::FLOAT) AS point,
        ROUND((soryo + dicollectprc + ditoujitsuhaisoprc)::FLOAT / ((100 + ditaxrate)::FLOAT / 100::FLOAT)::FLOAT) AS soryo,
        (c_didiscountprc + didiscountall) AS new_discount,
        ROUND((c_didiscountprc + didiscountall)::FLOAT / ((100 + ditaxrate)::FLOAT / 100::FLOAT)::FLOAT) AS new_discount_notax
    FROM kesai_h_data_mart_mv_kizuna cn
    ),--,
    -- select * from {{ref ('jpndcledw_integration__kesai_h_data_mart_mv')}} where saleno_trm like 'T1%' limit 100;
kesai AS (
    SELECT h.kokyano,
        h.saleno_key,
        h.saleno,
        m.gyono,
        m.bun_gyono,
        to_date(IFF(CAST(h.juchdate AS STRING) = 0, NULL, CAST(h.juchdate AS STRING)),'YYYYMMDD') order_dt,
        to_date(IFF(CAST(h.SHUKADATE_P AS STRING) = 0, NULL, CAST(h.SHUKADATE_P AS STRING)),'YYYYMMDD') ship_dt,
        prev_ship.prev_ship_dt,
        prev_order.prev_order_dt,
        h.daihanrobunname AS channel, -- "チャネル",
        --    h.dipromid dipromid,-- "媒体ID",
        prom.dspromcode dspromcode, -- "媒体コード",
        prom.dspromname dspromname, -- "媒体",
        h.juchkbn,
        SUM(CASE 
                --			when m.meisaikbn in ( '商品', 'ポイント交換商品' ) then m.warimaenukikingaku
                WHEN m.meisaikbn = '商品'
                    THEN m.warimaenukikingaku
                ELSE 0
                END) OVER (PARTITION BY m.saleno) AS warimaenukigokei,
        wh.discount,
        wh.soryo,
        wh.point,
        m.meisaikbn,
        CASE 
            WHEN h.daihanrobunname = '直営・百貨店'
                THEN CONCAT (
                        client.c_dstempocode,
                        CONCAT (
                            ':',
                            client.c_dstemponame
                            )
                        )
            ELSE NULL
            END AS storename,
        DENSE_RANK() OVER (
            PARTITION BY h.kokyano ORDER BY to_date(IFF(CAST(h.SHUKADATE_P AS STRING) = 0, NULL, CAST(h.SHUKADATE_P AS STRING)),'YYYYMMDD')
            ) RN_SHIP445,
        DENSE_RANK() OVER (
            PARTITION BY h.kokyano ORDER BY to_date(IFF(CAST(h.juchdate AS STRING) = 0, NULL, CAST(h.juchdate AS STRING)),'YYYYMMDD')
            ) RN_ORDER,
        DENSE_RANK() OVER (
            PARTITION BY h.kokyano,
            x.year_445 ORDER BY to_date(IFF(CAST(h.SHUKADATE_P AS STRING) = 0, NULL, CAST(h.SHUKADATE_P AS STRING)),'YYYYMMDD')
            ) RN_SHIP445_YEARWISE,
        DENSE_RANK() OVER (
            PARTITION BY h.kokyano,
            CASE 
                WHEN h.juchdate = 0
                    THEN NULL
                ELSE substring(h.juchdate, 1, 4)
                END ORDER BY to_date(IFF(CAST(h.juchdate AS STRING) = 0, NULL, CAST(h.juchdate AS STRING)),'YYYYMMDD')
            ) RN_ORDER_YEARWISE,
        m.itemcode kesai_itemcode,
        m.setitemnm,
        m.MEISAINUKIKINGAKU,
        m.warimaenukikingaku,
        m.hensu,
        m.suryo AS qty,
        a.c_diregularcontractid,
        h.smkeiroid,
        h.uketsuketelcompanycd,
        a.diordercode,
        h.kesaiid,
        m.diorderid,
        wh.new_discount,
        wh.new_discount_notax
    FROM kesai_h_data_mart_mv_kizuna h
    INNER JOIN kesai_m_data_mart_mv_kizuna m ON h.saleno_key = m.saleno_key --- 明細テーブル
    LEFT JOIN cim01kokya c ON h.kokyano = c.kokyano -- 顧客テーブル
    LEFT JOIN tbpromotion prom ON h.dipromid = prom.dipromid
    LEFT JOIN amount_header wh ON wh.saleno_key = h.saleno_key
    LEFT JOIN prev_ship ON h.kokyano = prev_ship.kokyano
        AND to_date(IFF(CAST(h.SHUKADATE_P AS STRING) = '0', NULL, CAST(h.SHUKADATE_P AS STRING)),'YYYYMMDD') = prev_ship.ship_dt --- ヘッダーテーブル
    LEFT JOIN prev_order ON h.kokyano = prev_order.kokyano
        AND to_date(IFF(CAST(h.juchdate AS STRING) = '0', NULL, CAST(h.juchdate AS STRING)),'YYYYMMDD') = prev_order.order_dt
    LEFT JOIN c_tbecclient client ON h.tenpocode = client.c_dstempocode
    LEFT JOIN cld_m x ON to_date(IFF(CAST(h.SHUKADATE_P AS STRING) = '0', NULL, CAST(h.SHUKADATE_P AS STRING)),'YYYYMMDD') = x.ymd_dt
    LEFT JOIN TEIKIKEIYAKU_DATA_MART_UNI a ON h.kesaiid::varchar = a.c_dikesaiid::varchar
        AND h.ordercode = a.diordercode
        AND m.gyono = a.dimeisaiid
    WHERE --ship_dt < '1-jan-2012' and 
        h.juchkbn IN (0, 1, 2) --返品除く
        and (
            m.MEISAINUKIKINGAKU >= 1 -- 1円以上
            OR m.MEISAINUKIKINGAKU < 0 -- 0円未満
            --     OR m.suryo <> 0            -- 数量<>0
            --     OR m.hensu <> 0            -- 返品数量<>0
            )
        AND (
            (
                c.TESTUSRFLG <> 'テストユーザ' ---テストユーザ除く
                AND c.kokyano <> '9999999944' ---不特定顧客除く
                AND c.kokyano <> '0000000011'
                AND c.kokyano IS NOT NULL
                )
            OR h.kokyano = '0000000000'
            )
    --and h.kokyano = 6429032727 -- 9852520821
    -- and h.saleno = 'U1011235938' 
    
    UNION ALL
    
    SELECT h.kokyano,
        h.saleno_key,
        h.saleno,
        m.gyono,
        m.bun_gyono,
        to_date(IFF(CAST(h.juchdate AS STRING) = 0, NULL, CAST(h.juchdate AS STRING)),'YYYYMMDD') order_dt,
        to_date(IFF(CAST(h.SHUKADATE_P AS STRING) = 0, NULL, CAST(h.SHUKADATE_P AS STRING)),'YYYYMMDD') ship_dt,
        NULL AS prev_ship_dt,
        NULL AS prev_order_dt,
        h.daihanrobunname AS channel, -- "チャネル",
        --    h.dipromid dipromid,-- "媒体ID",
        prom.dspromcode dspromcode, -- "媒体コード",
        prom.dspromname dspromname, -- "媒体",
        h.juchkbn,
        SUM(CASE 
                WHEN m.meisaikbn = '商品'
                    THEN m.warimaenukikingaku
                ELSE 0
                END) OVER (PARTITION BY m.saleno) AS warimaenukigokei,
        wh.discount,
        wh.soryo,
        wh.point,
        m.meisaikbn,
        CASE 
            WHEN h.daihanrobunname = '直営・百貨店'
                THEN CONCAT (
                        client.c_dstempocode,
                        CONCAT (
                            ':',
                            client.c_dstemponame
                            )
                        )
            ELSE NULL
            END AS storename,
        NULL AS RN_SHIP445,
        NULL AS RN_ORDER,
        NULL AS RN_SHIP445_YEARWISE,
        NULL AS RN_ORDER_YEARWISE,
        m.itemcode kesai_itemcode,
        m.setitemnm,
        m.MEISAINUKIKINGAKU,
        m.warimaenukikingaku,
        m.hensu,
        m.suryo AS qty,
        a.c_diregularcontractid,
        h.smkeiroid,
        h.uketsuketelcompanycd,
        a.diordercode,
        h.kesaiid,
        m.diorderid,
        wh.new_discount,
        wh.new_discount_notax
    FROM kesai_h_data_mart_mv_kizuna h
    INNER JOIN kesai_m_data_mart_mv_kizuna m ON h.saleno_key = m.saleno_key --- 明細テーブル
    LEFT JOIN cim01kokya c ON h.kokyano = c.kokyano -- 顧客テーブル
    LEFT JOIN tbpromotion prom ON h.dipromid = prom.dipromid
    LEFT JOIN amount_header wh ON wh.saleno_key = h.saleno_key
    LEFT JOIN c_tbecclient client ON h.tenpocode = client.c_dstempocode
    LEFT JOIN TEIKIKEIYAKU_DATA_MART_UNI a ON h.kesaiid::varchar = a.c_dikesaiid::varchar
        AND h.ordercode = a.diordercode
        AND m.gyono = a.dimeisaiid
    WHERE --ship_dt < '1-jan-2012' and 
        (
            (
                h.juchkbn IN (0, 1, 2) --返品除く、無料サンプル
                AND m.MEISAINUKIKINGAKU = 0
                AND m.suryo <> 0
                )
            OR (
                h.juchkbn NOT IN (0, 1, 2) --返品
                AND (
                    m.MEISAINUKIKINGAKU <> 0
                    OR m.hensu <> 0
                    )
                ) -- 数量<>0
            )
        AND (
            (
                c.TESTUSRFLG <> 'テストユーザ' ---テストユーザ除く
                AND c.kokyano <> '9999999944' ---不特定顧客除く
                AND c.kokyano <> '0000000011'
                AND c.kokyano IS NOT NULL
                )
            OR h.kokyano = '0000000000'
            )
    ),
item_kose AS (
    SELECT HENKAN.ITEMCODE KOSE_ITEMCODE,
        HENKAN.KOSEIOCODE,
        TM14.KOSECODE,
        TM14.SURYO,
        TM14.KOSERITSU
    FROM SYOUHINCD_HENKAN_QV HENKAN
    LEFT JOIN TM14SHKOS_QV TM14 ON HENKAN.KOSEIOCODE = TM14.ITEMCODE
        --SELECT
        --	hen.h_itemcode AS KOSE_ITEMCODE,
        --	hen.z_itemcode AS koseiocode,
        --	jizen.kosei_cd::character varying(65535) AS kosecode,
        --	jizen.suryo::numeric(14,4) AS suryo,
        --	jizen.koseritsu::numeric(18,8) AS koseritsu
        --FROM
        --	item_z_h_hen_tbl hen
        --	left join item_jizen_bunkai_tbl jizen on jizen.item_cd = hen.z_itemcode --and jizen.marker = 1
        --WHERE
        --	hen.marker between 1 and 4
    ),
item_bunkai AS (
    SELECT itemcode,
        bunkai_itemcode,
        bunkai_kosritu,
        bunkai_kossu
    FROM CIM08SHKOS_BUNKAI_QV
        --SELECT
        --	itemcode,
        --	bunkai_itemcode::character varying(65535) AS bunkai_itemcode,
        --	bunkai_kosritu,
        --	bunkai_kossu::numeric(36,18) AS bunkai_kossu
        --FROM
        --	item_pick_bunkai_tbl
    ),
i13 AS (
    SELECT itemcode AS i13_itemcode,
        teikikeiyaku
    FROM TM13ITEM_QV
        --SELECT
        --	iht.h_itemcode AS i13_itemcode,
        --	iht.dsoption010::character varying(6000) AS teikikeiyaku
        --FROM
        --	item_hanbai_tbl iht
    ),
bs AS (
    SELECT kesai.kokyano,
        kesai.saleno_key,
        kesai.saleno,
        kesai.gyono,
        kesai.bun_gyono,
        kesai.order_dt,
        kesai.ship_dt,
        kesai.prev_ship_dt,
        kesai.prev_order_dt,
        kesai.channel, -- "チャネル",
        --    kesai.dipromid,-- "媒体ID",
        kesai.dspromcode, -- "媒体コード",
        kesai.dspromname, -- "媒体",
        kesai.juchkbn,
        kesai.warimaenukigokei,
        kesai.discount,
        kesai.soryo,
        kesai.meisaikbn,
        kesai.point,
        kesai.storename,
        kesai.RN_SHIP445,
        kesai.RN_ORDER,
        kesai.RN_SHIP445_YEARWISE,
        kesai.RN_ORDER_YEARWISE,
        kesai.kesai_itemcode,
        kesai.setitemnm,
        kesai.meisainukikingaku::float as meisainukikingaku,
        kesai.warimaenukikingaku,
        kesai.hensu,
        kesai.qty,
        item_bunkai.bunkai_itemcode,
        item_kose.koseiocode,
        item_kose.kosecode,
        item_kose.suryo,
        nvl(item_bunkai.bunkai_kosritu, 1) AS h_koseritsu,
        nvl(item_kose.koseritsu, 1) AS z_koseritsu,
        nvl(item_bunkai.bunkai_kosritu, 1) * nvl(item_kose.koseritsu, 1)::float  AS ratio,
        nvl(item_bunkai.bunkai_kossu, 1) AS h_bun_suryo,
        nvl(item_kose.suryo, 1) AS z_bun_suryo,
        nvl(item_bunkai.bunkai_kossu, 1) * nvl(item_kose.suryo, 1) AS bun_suryo,
        nvl((
                CASE 
                    WHEN (
                            kesai.warimaenukigokei = 0
                            OR kesai.meisaikbn <> '商品'
                            )
                        THEN 0
                    ELSE kesai.warimaenukikingaku / kesai.warimaenukigokei
                    END
                ), 1) AS RATIO2,
        i13.teikikeiyaku,
        kesai.c_diregularcontractid,
        kesai.smkeiroid,
        kesai.uketsuketelcompanycd,
        kesai.diordercode,
        kesai.kesaiid,
        kesai.diorderid,
        kesai.new_discount,
        kesai.new_discount_notax
    FROM kesai
    LEFT OUTER JOIN item_bunkai ON kesai.kesai_itemcode = item_bunkai.itemcode
    LEFT OUTER JOIN item_kose ON item_bunkai.bunkai_itemcode = item_kose.KOSE_ITEMCODE
    LEFT OUTER JOIN i13 ON kesai.kesai_itemcode = i13.i13_itemcode
    ),
f_order AS (
    SELECT saleno,
        kokyano,
        order_dt,
        dense_rank() OVER (
            PARTITION BY kokyano,
            date_trunc('year', order_dt) ORDER BY order_dt,
                saleno
            ) AS y_order_F
    FROM kesai
    WHERE kesai.juchkbn IN (0, 1, 2) --返品除く
        AND (
            kesai.MEISAINUKIKINGAKU >= 1 -- 1円以上
            OR kesai.MEISAINUKIKINGAKU < 0
            ) -- 0円未満
    GROUP BY 1,
        2,
        3
    ),
f_ship AS (
    -- 全額ポイント利用の出荷も1回と数える
    SELECT saleno,
        kokyano,
        ship_dt,
        c.year_445,
        dense_rank() OVER (
            PARTITION BY kokyano,
            c.year_445 ORDER BY ship_dt,
                saleno
            ) AS y_ship_F
    FROM kesai
    INNER JOIN cld_m c ON kesai.ship_dt = c.ymd_dt
    WHERE kesai.order_dt >= to_date('20210101', 'YYYYMMDD')
        AND kesai.juchkbn IN (0, 1, 2) --返品除く
        AND (
            kesai.MEISAINUKIKINGAKU >= 1 -- 1円以上
            OR kesai.MEISAINUKIKINGAKU < 0
            ) -- 0円未満
    GROUP BY 1,
        2,
        3,
        4
    ),
u_new AS (
    -- new event
    SELECT kokyano, --x.year as first_order_year, 
        TO_CHAR(first_order_dt, 'YYYY') AS first_order_year,
        y.year_445 AS first_ship_year,
        first_order_dt,
        first_ship_dt
    FROM (
        SELECT DISTINCT kokyano,
            min(order_dt) AS first_order_dt,
            min(ship_dt) AS first_ship_dt
        FROM kesai
        WHERE kesai.juchkbn IN (0, 1, 2) --返品除く
            AND (
                kesai.MEISAINUKIKINGAKU >= 1 -- 1円以上
                OR kesai.MEISAINUKIKINGAKU < 0
                ) -- 0円未満
        GROUP BY kokyano
        )
    --left join cld_m x on first_order_dt = x.ymd_dt
    LEFT JOIN cld_m y ON first_ship_dt = y.ymd_dt
    ),
u_lapsed_F1_ship AS (
    SELECT DISTINCT kesai.kokyano,
        cld_m.year_445 --, kesai.ship_dt
    FROM kesai
    INNER JOIN cld_m ON kesai.ship_dt = cld_m.ymd_dt
    WHERE datediff(day, prev_ship_dt, ship_dt) >= 365
        AND kesai.juchkbn IN (0, 1, 2) --返品除く
        AND (
            kesai.MEISAINUKIKINGAKU >= 1 -- 1円以上
            OR kesai.MEISAINUKIKINGAKU < 0
            ) -- 0円未満
    ),
u_lapsed_F1_order AS (
    SELECT DISTINCT kesai.kokyano,
        TO_CHAR(kesai.order_dt, 'YYYY') AS year --cld_m.year, kesai.order_dt
    FROM kesai --inner join cld_m on kesai.order_dt = cld_m.ymd_dt 
    WHERE datediff(day, prev_order_dt, order_dt) >= 365
        AND kesai.juchkbn IN (0, 1, 2) --返品除く
        AND (
            kesai.MEISAINUKIKINGAKU >= 1 -- 1円以上
            OR kesai.MEISAINUKIKINGAKU < 0
            ) -- 0円未満
    ),
transformed as(
    	SELECT bs.kokyano,
	bs.saleno_key,
	bs.saleno,
	bs.gyono,
	bs.bun_gyono,
	bs.order_dt,
	bs.ship_dt, --bs.pl_order_dt,bs.pl_ship_dt,
	'ダミーコード' AS tokuiname,
	bs.storename,
	CASE 
		WHEN bs.RN_ORDER = 1
			THEN '1' --new F1
		WHEN bs.RN_ORDER > 1
			AND (
				u_new2.first_order_year IS NOT NULL
				OR u_lapsed_F1_order.year IS NOT NULL
				)
			--and b.year = a.year
			AND to_char(bs.order_dt, 'YYYY') = to_char(bs.prev_order_dt, 'YYYY')
			AND datediff(day, bs.prev_order_dt, bs.order_dt) < 365
			THEN bs.RN_ORDER_YEARWISE --new F2 & lapsed F2
		WHEN bs.RN_ORDER > 1
			AND (
				u_new2.first_order_year IS NOT NULL
				OR u_lapsed_F1_order.year IS NOT NULL
				)
			AND datediff(day, bs.prev_order_dt, bs.order_dt) >= 365
			THEN '1' --lapsed F1
		WHEN bs.RN_ORDER > 1
			AND u_new2.first_order_year IS NULL
			AND u_lapsed_F1_order.year IS NULL
			AND bs.RN_ORDER_YEARWISE = 1
			THEN '1' --existing F1
		WHEN bs.RN_ORDER > 1
			AND u_new2.first_order_year IS NULL
			AND u_lapsed_F1_order.year IS NULL
			AND bs.RN_ORDER_YEARWISE > 1
			THEN bs.RN_ORDER_YEARWISE --existing F2
		END AS F_order,
	CASE 
		WHEN bs.RN_SHIP445 = 1
			THEN '1' --new F1
		WHEN bs.RN_SHIP445 > 1
			AND (
				u_new.first_ship_year IS NOT NULL
				OR u_lapsed_F1_ship.year_445 IS NOT NULL
				)
			AND y.year_445 = x.year_445
			AND datediff(day, bs.prev_ship_dt, bs.ship_dt) < 365
			THEN bs.RN_SHIP445_YEARWISE --new F2 & lapsed F2
		WHEN bs.RN_SHIP445 > 1
			AND (
				u_new.first_ship_year IS NOT NULL
				OR u_lapsed_F1_ship.year_445 IS NOT NULL
				)
			AND datediff(day, bs.prev_ship_dt, bs.ship_dt) >= 365
			THEN '1' --lapsed F1
		WHEN bs.RN_SHIP445 > 1
			AND u_new.first_ship_year IS NULL
			AND u_lapsed_F1_ship.year_445 IS NULL
			AND bs.RN_SHIP445_YEARWISE = 1
			THEN '1' --existing F1
		WHEN bs.RN_SHIP445 > 1
			AND u_new.first_ship_year IS NULL
			AND u_lapsed_F1_ship.year_445 IS NULL
			AND bs.RN_SHIP445_YEARWISE > 1 --existing F2
			THEN bs.RN_SHIP445_YEARWISE --existing F2
		END AS F_ship445,
	bs.channel,
	--	bs.dipromid,
	bs.dspromcode,
	bs.dspromname,
	bs.juchkbn,
	bs.meisaikbn,
	bs.kesai_itemcode AS h_o_item_code,
	bs.setitemnm AS h_o_item_name,
	nvl(bs.kesai_itemcode, '') || ' : ' || nvl(bs.setitemnm, '') AS h_o_item_cname,
	bs.qty * bs.ratio AS h_o_item_anbun_qty,
	bs.bunkai_itemcode AS h_item_code,
	bs.koseiocode AS z_o_item_code,
	bs.kosecode AS z_item_code,
	bs.ratio,
	CASE 
		WHEN bs.meisaikbn IN ('商品', 'ポイント交換商品')
			THEN bs.qty * bs.bun_suryo
		ELSE NULL
		END AS z_item_suryo,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN bs.hensu * bs.bun_suryo
		WHEN bs.meisaikbn = 'ポイント交換商品'
			THEN 0
		ELSE NULL
		END AS z_item_hen_suryo,
	CAST(bs.MEISAINUKIKINGAKU * bs.ratio AS NUMBER(14, 5)) AS anbun_amount_tax110_ex,
	CASE 
		WHEN bs.meisaikbn IN ('商品', 'ポイント交換商品')
			THEN bs.warimaenukikingaku * bs.ratio
		ELSE NULL
		END AS z_item_amount_tax_ex,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN bs.ratio2
		WHEN bs.meisaikbn = 'ポイント交換商品'
			THEN 0
		ELSE NULL
		END AS ratio2,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN bs.soryo * bs.ratio2 * bs.ratio
		ELSE NULL
		END AS anbun_soryo,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN bs.point * bs.ratio2 * bs.ratio
		ELSE NULL
		END AS anbun_point_tax_ex,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN bs.discount * bs.ratio2 * bs.ratio
		ELSE NULL
		END AS anbun_tokuten,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN CASE 
					WHEN juchkbn IN ('0', '1', '2')
						THEN abs(bs.warimaenukikingaku * bs.ratio) + abs(bs.soryo * bs.ratio2 * bs.ratio)
					ELSE 0
					END
		WHEN bs.meisaikbn = 'ポイント交換商品'
			THEN 0
		ELSE NULL
		END AS GTS,
	CASE 
		WHEN bs.meisaikbn IN ('商品', 'ポイント交換商品')
			THEN CASE 
					WHEN juchkbn IN ('0', '1', '2')
						THEN abs(bs.qty * bs.bun_suryo)
					ELSE 0
					END
		ELSE NULL
		END AS GTS_QTY,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN CASE 
					WHEN juchkbn IN ('0', '1', '2')
						THEN abs(bs.discount * bs.ratio2 * bs.ratio)
					ELSE 0
					END
		WHEN bs.meisaikbn = 'ポイント交換商品'
			THEN 0
		ELSE NULL
		END AS ciw_discount,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN CASE 
					WHEN juchkbn IN ('0', '1', '2')
						THEN abs(bs.point * bs.ratio2 * bs.ratio)
					ELSE abs(bs.point * bs.ratio2 * bs.ratio) * (- 1)
					END
		WHEN bs.meisaikbn = 'ポイント交換商品'
			THEN 0
		ELSE NULL
		END AS ciw_point,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN CASE 
					WHEN juchkbn IN ('90', '91', '92')
						THEN abs(bs.warimaenukikingaku * bs.ratio) * (- 1)
					ELSE 0
					END
		WHEN bs.meisaikbn = 'ポイント交換商品'
			THEN 0
		ELSE NULL
		END AS ciw_return,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN CASE 
					WHEN juchkbn IN ('90', '91', '92')
						THEN abs(bs.hensu * bs.bun_suryo) * (- 1)
					ELSE 0
					END
		WHEN bs.meisaikbn = 'ポイント交換商品'
			THEN 0
		ELSE NULL
		END AS ciw_return_qty,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN CASE 
					WHEN juchkbn IN ('0', '1', '2')
						THEN (GTS - CIW_Point - CIW_Discount)
					ELSE (CIW_RETURN - CIW_Point - CIW_Discount)
					END
		WHEN bs.meisaikbn = 'ポイント交換商品'
			THEN 0
		ELSE NULL
		END AS NTS,
	bs.teikikeiyaku,
	kr_f.now_rowno AS F,
	bs.MEISAINUKIKINGAKU,
	bs.warimaenukikingaku,
	bs.soryo,
	bs.point,
	bs.discount,
	bs.qty AS h_o_suryo,
	bs.hensu,
	fo.y_order_F,
	fs.y_ship_F,
	bs.h_koseritsu,
	bs.z_koseritsu,
	bs.h_bun_suryo,
	bs.z_bun_suryo,
	bs.c_diregularcontractid,
	bs.smkeiroid,
	bs.uketsuketelcompanycd,
	bs.diordercode,
	bs.kesaiid,
	bs.diorderid,
	bs.new_discount,
	CASE 
		WHEN bs.meisaikbn = '商品'
			THEN CASE 
					WHEN juchkbn IN ('0', '1', '2')
						THEN abs(bs.new_discount_notax * bs.ratio2 * bs.ratio)
					ELSE 0
					END
		WHEN bs.meisaikbn = 'ポイント交換商品'
			THEN 0
		ELSE NULL
		END AS ciw_discount_notax,
	CASE 
		WHEN bs.juchkbn IN (1, 2)
			AND bs.c_diregularcontractid IS NOT NULL
			THEN dense_rank() OVER (
					PARTITION BY bs.c_diregularcontractid ORDER BY bs.order_dt,
						bs.diordercode
					)
		ELSE NULL
		END AS sub_cnt FROM bs LEFT JOIN cld_m a ON a.ymd_dt = bs.order_dt LEFT JOIN cld_m x ON x.ymd_dt = bs.ship_dt LEFT JOIN cld_m b ON b.ymd_dt = bs.prev_order_dt LEFT JOIN cld_m y ON y.ymd_dt = bs.prev_ship_dt LEFT JOIN u_new ON u_new.kokyano = bs.kokyano
	AND u_new.first_ship_year = x.year_445 LEFT JOIN u_lapsed_F1_ship ON u_lapsed_F1_ship.kokyano = bs.kokyano
	AND u_lapsed_F1_ship.year_445 = x.year_445 --and bs.ship_dt = u_lapsed_F1_ship.ship_dt
	LEFT JOIN u_new AS u_new2 ON u_new2.kokyano = bs.kokyano
	AND u_new2.first_order_year = to_char(bs.order_dt, 'YYYY') --a.year
	LEFT JOIN u_lapsed_F1_order ON u_lapsed_F1_order.kokyano = bs.kokyano
	AND u_lapsed_F1_order.year = to_char(bs.order_dt, 'YYYY') --a.year --and bs.order_dt = u_lapsed_F1_order.order_dt
	LEFT OUTER JOIN kr_frequency_1yn_900_kizuna kr_f ON bs.saleno = kr_f.saleno LEFT OUTER JOIN f_order fo ON bs.saleno = fo.saleno LEFT OUTER JOIN f_ship fs ON bs.saleno = fs.saleno

),
final as(
    select 
        kokyano::varchar(68) as kokyano,
        saleno_key::varchar(83) as saleno_key,
        saleno::varchar(63) as saleno,
        gyono::number(18,0) as gyono,
        bun_gyono::number(18,0) as bun_gyono,
        to_date(order_dt) as order_dt,
        to_date(ship_dt) as ship_dt,
        tokuiname::varchar(346) as tokuiname,
        storename::varchar(131) as storename,
        f_order::varchar(5) as f_order,
        f_ship445::varchar(5) as f_ship445,
        channel::varchar(20) as channel,
        dspromcode::number(18,0) as dspromcode,
        dspromname::varchar(384) as dspromname,
        juchkbn::varchar(3) as juchkbn,
        meisaikbn::varchar(36) as meisaikbn,
        h_o_item_code::varchar(30) as h_o_item_code,
        h_o_item_name::varchar(200) as h_o_item_name,
        h_o_item_cname::varchar(240) as h_o_item_cname,
        h_o_item_anbun_qty::number(16,8) as h_o_item_anbun_qty,
        h_item_code::varchar(60) as h_item_code,
        z_o_item_code::varchar(45) as z_o_item_code,
        z_item_code::varchar(45) as z_item_code,
        ratio::number(16,8) as ratio,
        z_item_suryo::number(18,0) as z_item_suryo,
        z_item_hen_suryo::number(18,0) as z_item_hen_suryo,
        anbun_amount_tax110_ex::float as anbun_amount_tax110_ex,
        z_item_amount_tax_ex::float as z_item_amount_tax_ex,
        ratio2::number(16,8) as ratio2,
        anbun_soryo::float as anbun_soryo,
        anbun_point_tax_ex::float as anbun_point_tax_ex,
        anbun_tokuten::float as anbun_tokuten,
        gts::float as gts,
        gts_qty::float as gts_qty,
        ciw_discount::float as ciw_discount,
        ciw_point::float as ciw_point,
        ciw_return::float as ciw_return,
        ciw_return_qty::float as ciw_return_qty,
        nts::float as nts,
        teikikeiyaku::varchar(20) as teikikeiyaku,
        f::number(38,18) as f,
        meisainukikingaku::number(18,0) as meisainukikingaku,
        warimaenukikingaku::number(18,0) as warimaenukikingaku,
        soryo::number(18,0) as soryo,
        point::number(18,0) as point,
        discount::number(18,0) as discount,
        h_o_suryo::number(18,0) as h_o_suryo,
        hensu::number(18,0) as hensu,
        y_order_f::number(18,0) as y_order_f,
        y_ship_f::number(18,0) as y_ship_f,
        h_koseritsu::number(16,8) as h_koseritsu,
        z_koseritsu::number(16,8) as z_koseritsu,
        h_bun_suryo::number(18,0) as h_bun_suryo,
        z_bun_suryo::number(18,0) as z_bun_suryo,
        c_diregularcontractid::number(38,0) as c_diregularcontractid,
        smkeiroid::number(18,0) as smkeiroid,
        uketsuketelcompanycd::varchar(8) as uketsuketelcompanycd,
        diordercode::varchar(16) as diordercode,
        kesaiid::varchar(62) as kesaiid,
        diorderid::number(18,0) as diorderid,
        new_discount::float as new_discount,
        ciw_discount_notax::float as ciw_discount_notax,
        sub_cnt::number(18,0) as sub_cnt,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by ,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final
