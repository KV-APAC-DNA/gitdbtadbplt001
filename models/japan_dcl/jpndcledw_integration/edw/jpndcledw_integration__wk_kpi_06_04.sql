with TEIKI_KPI4_SOUKEIYAKU_HNR_old as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TEIKI_KPI4_SOUKEIYAKU_HNR_OLD
),

CMNKOKYA_TM01_CALENDAR_JJ as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CMNKOKYA_TM01_CALENDAR_JJ
),
cte1 as 
(
      SELECT "大区分",
      CASE 
            WHEN "小区分" = 'スポット'
                  THEN '新規'
            ELSE "小区分"
            END AS "小区分",
      "年月",
      CASE "販路"
            WHEN 5
                  THEN 'ＷＥＢ'
            WHEN 7
                  THEN '店舗'
            WHEN 8
                  THEN '店舗'
            WHEN 9
                  THEN '店舗'
            ELSE '通販'
            END AS "販路",
      "総契約件数",
      "総契約金額",
      "ユニーク契約者数" FROM TEIKI_KPI4_SOUKEIYAKU_HNR_old
      WHERE "年月" >= (
            SELECT to_char(add_months(to_date(ym, 'yyyymm'), - 2), 'yyyymm')
            FROM CMNKOKYA_TM01_CALENDAR_JJ
            WHERE to_char(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp()), 'yyyymmdd') BETWEEN YM_START_DATE
                        AND YM_END_DATE
            )
),
cte2 as 
(
      SELECT "大区分",
      CASE 
            WHEN "小区分" = 'スポット'
                  THEN '解約'
            ELSE 'その他'
            END AS "小区分",
      "年月",
      CASE "販路"
            WHEN 5
                  THEN 'ＷＥＢ'
            WHEN 7
                  THEN '店舗'
            WHEN 8
                  THEN '店舗'
            WHEN 9
                  THEN '店舗'
            ELSE '通販'
            END AS "販路",
      "総契約件数",
      "総契約金額",
      "ユニーク契約者数" FROM TEIKI_KPI4_SOUKEIYAKU_HNR_old WHERE "小区分" = 'スポット'
      AND "年月" >= (
            SELECT to_char(add_months(to_date(ym, 'yyyymm'), - 2), 'yyyymm')
            FROM CMNKOKYA_TM01_CALENDAR_JJ
            WHERE to_char(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp()), 'yyyymmdd') BETWEEN YM_START_DATE
                        AND YM_END_DATE
            )
),
cte3 as 
( 
      SELECT "大区分",
      CASE 
            WHEN "小区分" = 'スポット'
                  THEN '新規'
            ELSE "小区分"
            END AS "小区分",
      "年月",
      CASE "販路"
            WHEN 7
                  THEN 'その他'
            WHEN 8
                  THEN 'その他'
            WHEN 9
                  THEN 'その他'
            ELSE 'ＤＴＣ'
            END AS "販路",
      "総契約件数",
      "総契約金額",
      "ユニーク契約者数" FROM TEIKI_KPI4_SOUKEIYAKU_HNR_old
      WHERE "年月" >= (
            SELECT to_char(add_months(to_date(ym, 'yyyymm'), - 2), 'yyyymm')
            FROM CMNKOKYA_TM01_CALENDAR_JJ
            WHERE to_char(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp()), 'yyyymmdd') BETWEEN YM_START_DATE
                        AND YM_END_DATE
            )
),
cte4 as 
(
      SELECT "大区分",
      CASE 
            WHEN "小区分" = 'スポット'
                  THEN '解約'
            ELSE 'その他'
            END AS "小区分",
      "年月",
      CASE "販路"
            WHEN 7
                  THEN 'その他'
            WHEN 8
                  THEN 'その他'
            WHEN 9
                  THEN 'その他'
            ELSE 'ＤＴＣ'
            END AS "販路",
      "総契約件数",
      "総契約金額",
      "ユニーク契約者数" FROM TEIKI_KPI4_SOUKEIYAKU_HNR_old
      WHERE "小区分" = 'スポット'
      AND "年月" >= (
            SELECT to_char(add_months(to_date(ym, 'yyyymm'), - 2), 'yyyymm')
            FROM CMNKOKYA_TM01_CALENDAR_JJ
            WHERE to_char(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', current_timestamp()), 'yyyymmdd') BETWEEN YM_START_DATE
                        AND YM_END_DATE
            )
),
transformed as 
(
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
    union all
    select * from cte4
),
final as
(
    select 
        "大区分"::VARCHAR(200) AS "大区分",
        "小区分"::VARCHAR(200) AS "小区分",
        "年月"::VARCHAR(100) AS yymm,
        "販路"::VARCHAR(200) AS "販路",
        "総契約件数"::NUMBER(38,0) AS "総契約件数",
        "総契約金額"::NUMBER(38,0) AS "総契約金額",
        "ユニーク契約者数"::NUMBER(38,0) AS "ユニーク契約者数"
    from transformed
)
select * from final