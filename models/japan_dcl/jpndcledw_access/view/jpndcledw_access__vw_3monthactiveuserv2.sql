with kesai_h_data_mart_mv as (
    select * from {{ ref('jpndcledw_access__kesai_h_data_mart_mv') }}
),

cim01kokya as (
    select * from {{ ref('jpndcledw_access__cim01kokya') }}
),

tm22kokyasts as (
    select * from {{ ref('jpndcledw_access__tm22kokyasts') }}
),

kr_frequency_1yn_900 as (
    select * from {{ ref('jpndcledw_access__kr_frequency_1yn_900') }}
),

prekesai as
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        datediff(
            month,
            to_date(
                (
                    lag(to_char(h."juchdate")) OVER (
                        PARTITION BY h."kokyano"
                        ORDER BY to_char(h."juchdate")
                    )
                ),
                'YYYYMMDD'
            ),
            "order_dt"
        ) month_since_last_order,
        -- 前回受注日との間隔（月）
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(day, "order_dt", current_timestamp()) <= 92
        AND datediff(day, "order_dt", current_timestamp()) >= 2
),
new_user as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai as t1
    WHERE datediff(day, "order_dt", current_timestamp()) <= 32
        and datediff(day, "order_dt", current_timestamp()) >= 2
    GROUP BY t1."customer_id"
),
kesai as (
    SELECT *,
        CASE
            WHEN current_month_user = 1 then CASE
                WHEN 'nu.new_user' = 1 THEN 'new'
                WHEN 'lu.lapsed_user' = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai as "t1"
        left outer join new_user as "nu" on 't1."customer_id"' = 'nu."id_new"'
        left outer join lapsed_user as "lu" on 't1."customer_id"' = 'lu."id_lapsed"'
        left outer join current_month_user as "cmu" on 't1."customer_id"' = 'cmu."id_cmu"'
),
prekesai2 as --2
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 3
        AND datediff(month, "order_dt", current_timestamp()) >= 1
),
new_user2 as(
    SELECT t1."customer_id" id_new,
        1 as new_user
    FROM prekesai2 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user2 as (
    SELECT t1."customer_id" id_lapsed,
        1 as lapsed_user
    FROM prekesai2 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user2 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai2 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 1
    GROUP BY t1."customer_id"
),
kesai2 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai2 as t1
        left outer join new_user2 as nu on t1."customer_id" = 'nu."id_new"'
        left outer join lapsed_user2 as lu on t1."customer_id" = 'lu."id_lapsed"'
        left outer join current_month_user2 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai3 as --3
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 4
        AND datediff(month, "order_dt", current_timestamp()) >= 2
),
new_user3 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai3 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user3 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai3 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user3 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai3 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 2
    GROUP BY t1."customer_id"
),
kesai3 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai3 as t1
        left outer join new_user3 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user3 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user3 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai4 as --4
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 5
        AND datediff(month, "order_dt", current_timestamp()) >= 3
),
new_user4 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai4 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user4 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai4 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user4 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai4 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 3
    GROUP BY t1."customer_id"
),
kesai4 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai4 as t1
        left outer join new_user4 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user4 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user4 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai5 as --5
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 6
        AND datediff(month, "order_dt", current_timestamp()) >= 4
),
new_user5 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai5 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user5 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai5 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user5 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai5 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 4
    GROUP BY t1."customer_id"
),
kesai5 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai5 as t1
        left outer join new_user5 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user5 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user5 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai6 as --6
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 7
        AND datediff(month, "order_dt", current_timestamp()) >= 5
),
new_user6 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai6 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user6 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai6 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user6 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai6 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 5
    GROUP BY t1."customer_id"
),
kesai6 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai6 as t1
        left outer join new_user6 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user6 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user6 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai7 as --7
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 8
        AND datediff(month, "order_dt", current_timestamp()) >= 6
),
new_user7 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai7 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user7 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai7 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user7 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai7 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 6
    GROUP BY t1."customer_id"
),
kesai7 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai7 as t1
        left outer join new_user7 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user7 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user7 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai8 as --8
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 9
        AND datediff(month, "order_dt", current_timestamp()) >= 7
),
new_user8 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai8 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user8 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai8 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user8 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai8 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 7
    GROUP BY t1."customer_id"
),
kesai8 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai8 as t1
        left outer join new_user8 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user8 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user8 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai12 as --2
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 10
        AND datediff(month, "order_dt", current_timestamp()) >= 8
),
new_user12 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai12 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user12 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai12 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user12 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai12 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 8
    GROUP BY t1."customer_id"
),
kesai12 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai12 as t1
        left outer join new_user12 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user12 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user12 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai13 as --3
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 11
        AND datediff(month, "order_dt", current_timestamp()) >= 9
),
new_user13 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai13 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user13 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai13 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user13 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai13 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 9
    GROUP BY t1."customer_id"
),
kesai13 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai13 as t1
        left outer join new_user13 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user13 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user13 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai14 as --4
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 12
        AND datediff(month, "order_dt", current_timestamp()) >= 10
),
new_user14 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai14 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user14 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai14 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user14 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai14 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 10
    GROUP BY t1."customer_id"
),
kesai14 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai14 as t1
        left outer join new_user14 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user14 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user14 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai15 as --5
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR h."shukkasts" = '1060'
            AND (h."shukadate" IS NOT NULL)
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 13
        AND datediff(month, "order_dt", current_timestamp()) >= 11
),
new_user15 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai15 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user15 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai15 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user15 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai15 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 11
    GROUP BY t1."customer_id"
),
kesai15 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai15 as t1
        left outer join new_user15 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user15 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user15 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai16 as --6
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 14
        AND datediff(month, "order_dt", current_timestamp()) >= 12
),
new_user16 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai16 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user16 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai16 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user16 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai16 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 12
    GROUP BY t1."customer_id"
),
kesai16 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai16 as t1
        left outer join new_user16 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user16 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user16 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai17 as --7
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 15
        AND datediff(month, "order_dt", current_timestamp()) >= 13
),
new_user17 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai17 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user17 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai17 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user17 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai17 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 13
    GROUP BY t1."customer_id"
),
kesai17 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai17 as t1
        left outer join new_user17 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user17 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user17 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai18 as --8
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 16
        AND datediff(month, "order_dt", current_timestamp()) >= 14
),
new_user18 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai18 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user18 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai18 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user18 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai18 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 14
    GROUP BY t1."customer_id"
),
kesai18 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai18 as t1
        left outer join new_user18 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user18 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user18 as cmu on t1."customer_id" = cmu."id_cmu"
),
final as (
    SELECT to_char(
            dateadd(month, -2, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(dateadd(month, 0, current_timestamp()), 'YYYYMM') as "yyyymm_e",
        'as of' || to_char(dateadd(day, 0, GETDATE()), 'YYYYMMDD') as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai bs
    WHERE datediff(day, "order_dt", current_timestamp()) <= 92
        and datediff(day, "order_dt", current_timestamp()) >= 2
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -3, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -1, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -1, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai2 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -4, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -2, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -2, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai3 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -5, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -3, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -3, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai4 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -6, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -4, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -4, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai5 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -7, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -5, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -5, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai6 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -8, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -6, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -6, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai7 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -9, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -7, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -7, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai8 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -10, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -8, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -8, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai12 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -11, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -9, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -9, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai13 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -12, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -10, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -10, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai14 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -13, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -11, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -11, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai15 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -14, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -12, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -12, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai16 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -15, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -13, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -13, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai17 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -16, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -14, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -14, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai18 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
)
select *
from final