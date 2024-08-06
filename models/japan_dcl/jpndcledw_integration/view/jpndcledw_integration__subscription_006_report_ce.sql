with teikikeiyaku_data_mart_uni as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TEIKIKEIYAKU_DATA_MART_UNI
),
final as (
SELECT 
  tei.c_diusrid, 
  to_date(
    (tei.keiyakubi):: text, 
    ('YYYYMMDD' :: character varying):: text
  ) AS keiyakubi, 
  to_date(
    sho.min_keiyakubi, 
    ('YYYYMMDD' :: character varying):: text
  ) AS min_keiyakubi, 
  tei.c_diregularcourseid, 
  CASE WHEN (tei.c_diregularcourseid = 1) THEN '新毎月' :: character varying WHEN (tei.c_diregularcourseid = 2) THEN '新２ヵ月' :: character varying WHEN (tei.c_diregularcourseid = 3) THEN '新３ヵ月' :: character varying WHEN (tei.c_diregularcourseid = 4) THEN '旧毎月' :: character varying WHEN (tei.c_diregularcourseid = 5) THEN '旧２ヵ月' :: character varying WHEN (tei.c_diregularcourseid = 6) THEN '旧３ヵ月' :: character varying ELSE '該当なし' :: character varying END AS course_name, 
  CASE WHEN (
    (tei.keiyakubi):: text = sho.min_keiyakubi
  ) THEN '新規' :: character varying WHEN (
    (tei.keiyakubi):: text > sho.min_keiyakubi
  ) THEN '既存' :: character varying ELSE '異常データ' :: character varying END AS kbn, 
  count(tei.diordercode) AS cnt_order, 
  sum(tei.diitemsalesprc) AS sales_amount 
FROM 
  (
    teikikeiyaku_data_mart_uni tei 
    JOIN (
      SELECT 
        teikikeiyaku_data_mart_uni.c_diusrid, 
        min(
          (
            teikikeiyaku_data_mart_uni.keiyakubi
          ):: text
        ) AS min_keiyakubi 
      FROM 
        teikikeiyaku_data_mart_uni 
      GROUP BY 
        teikikeiyaku_data_mart_uni.c_diusrid
    ) sho ON (
      (
        (sho.c_diusrid):: text = (tei.c_diusrid):: text
      )
    )
  ) 
WHERE 
  (
    (
      (
        (tei.keiyakubi):: text >= to_char(
          date_trunc(
            'month', 
            add_months(
              sysdate(), 
              (
                - (12):: bigint
              )
            )
          ), 
          ('YYYYMMDD' :: character varying):: text
        )
      ) 
      AND (
        (tei.keiyakubi):: text <= to_char(
          sysdate(), 
          ('YYYYMMDD' :: character varying):: text
        )
      )
    ) 
    AND (
      (tei.contract_kbn):: text = ('新規' :: character varying):: text
    )
  ) 
GROUP BY 
  tei.c_diusrid, 
  to_date(
    (tei.keiyakubi):: text, 
    ('YYYYMMDD' :: character varying):: text
  ), 
  to_date(
    sho.min_keiyakubi, 
    ('YYYYMMDD' :: character varying):: text
  ), 
  tei.c_diregularcourseid, 
  CASE WHEN (tei.c_diregularcourseid = 1) THEN '新毎月' :: character varying WHEN (tei.c_diregularcourseid = 2) THEN '新２ヵ月' :: character varying WHEN (tei.c_diregularcourseid = 3) THEN '新３ヵ月' :: character varying WHEN (tei.c_diregularcourseid = 4) THEN '旧毎月' :: character varying WHEN (tei.c_diregularcourseid = 5) THEN '旧２ヵ月' :: character varying WHEN (tei.c_diregularcourseid = 6) THEN '旧３ヵ月' :: character varying ELSE '該当なし' :: character varying END, 
  CASE WHEN (
    (tei.keiyakubi):: text = sho.min_keiyakubi
  ) THEN '新規' :: character varying WHEN (
    (tei.keiyakubi):: text > sho.min_keiyakubi
  ) THEN '既存' :: character varying ELSE '異常データ' :: character varying END
)
select * from final