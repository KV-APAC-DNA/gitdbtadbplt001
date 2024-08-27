with c_tbecranksumamount as (
select * from {{ ref('jpndclitg_integration__c_tbecranksumamount') }}
),
c_tbecrankaddamountadm as (
select * from {{ ref('jpndclitg_integration__c_tbecrankaddamountadm') }}
),
final as (
SELECT 
  lpad(
    (
      (uni.diecusrid):: character varying
    ):: text, 
    10, 
    (
      (0):: character varying
    ):: text
  ) AS kokyano, 
  sum(uni.kingaku) AS ruikeikingaku 
FROM 
  (
    SELECT 
      c_tbecranksumamount.diecusrid, 
      c_tbecranksumamount.c_dsranktotalprcbymonth AS kingaku 
    FROM 
      c_tbecranksumamount c_tbecranksumamount 
    WHERE 
      (
        (
          (
            (
              c_tbecranksumamount.c_dsaggregateym
            ):: text >= (
              SELECT 
                "substring"(
                  calendar."累計金額計算_期間開始日付", 
                  1, 6
                ) AS "substring" 
              FROM 
                (
                  SELECT 
                    CASE WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0331' :: character varying):: text
                    ) THEN (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (3):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0401' :: character varying):: text
                    ) WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0930' :: character varying):: text
                    ) THEN (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (3):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('1001' :: character varying):: text
                    ) ELSE (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (2):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0401' :: character varying):: text
                    ) END AS "累計金額計算_期間開始日付", 
                    CASE WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0331' :: character varying):: text
                    ) THEN (
                      to_char(
                        current_timestamp(), 
                        ('YYYY' :: character varying):: text
                      ) || ('0331' :: character varying):: text
                    ) WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0930' :: character varying):: text
                    ) THEN (
                      to_char(
                        current_timestamp(), 
                        ('YYYY' :: character varying):: text
                      ) || ('0930' :: character varying):: text
                    ) ELSE (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) + (
                              (1):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0331' :: character varying):: text
                    ) END AS "集計金額計算_期間終了日付", 
                    to_char(
                      current_timestamp(), 
                      ('YYYYMMDD' :: character varying):: text
                    ) AS "累計金額計算_現時点終了日付"
                ) calendar
            )
          ) 
          AND (
            (
              c_tbecranksumamount.c_dsaggregateym
            ):: text <= (
              SELECT 
                "substring"(
                  calendar."累計金額計算_現時点終了日付", 
                  1, 6
                ) AS "substring" 
              FROM 
                (
                  SELECT 
                    CASE WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0331' :: character varying):: text
                    ) THEN (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (3):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0401' :: character varying):: text
                    ) WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0930' :: character varying):: text
                    ) THEN (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (3):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('1001' :: character varying):: text
                    ) ELSE (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (2):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0401' :: character varying):: text
                    ) END AS "累計金額計算_期間開始日付", 
                    CASE WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0331' :: character varying):: text
                    ) THEN (
                      to_char(
                        current_timestamp(), 
                        ('YYYY' :: character varying):: text
                      ) || ('0331' :: character varying):: text
                    ) WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0930' :: character varying):: text
                    ) THEN (
                      to_char(
                        current_timestamp(), 
                        ('YYYY' :: character varying):: text
                      ) || ('0930' :: character varying):: text
                    ) ELSE (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) + (
                              (1):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0331' :: character varying):: text
                    ) END AS "集計金額計算_期間終了日付", 
                    to_char(
                      current_timestamp(), 
                      ('YYYYMMDD' :: character varying):: text
                    ) AS "累計金額計算_現時点終了日付"
                ) calendar
            )
          )
        ) 
        AND (
          (c_tbecranksumamount.dielimflg):: text = ('0' :: character varying):: text
        )
      ) 
    UNION ALL 
    SELECT 
      c_tbecrankaddamountadm.diecusrid, 
      c_tbecrankaddamountadm.c_dsrankaddprc AS kingaku 
    FROM 
      c_tbecrankaddamountadm c_tbecrankaddamountadm 
    WHERE 
      (
        (
          (
            to_char(
              c_tbecrankaddamountadm.dsorderdt, 
              ('YYYYMMDD' :: character varying):: text
            ) >= (
              SELECT 
                calendar."累計金額計算_期間開始日付" 
              FROM 
                (
                  SELECT 
                    CASE WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0331' :: character varying):: text
                    ) THEN (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (3):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0401' :: character varying):: text
                    ) WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0930' :: character varying):: text
                    ) THEN (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (3):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('1001' :: character varying):: text
                    ) ELSE (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (2):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0401' :: character varying):: text
                    ) END AS "累計金額計算_期間開始日付", 
                    CASE WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0331' :: character varying):: text
                    ) THEN (
                      to_char(
                        current_timestamp(), 
                        ('YYYY' :: character varying):: text
                      ) || ('0331' :: character varying):: text
                    ) WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0930' :: character varying):: text
                    ) THEN (
                      to_char(
                        current_timestamp(), 
                        ('YYYY' :: character varying):: text
                      ) || ('0930' :: character varying):: text
                    ) ELSE (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) + (
                              (1):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0331' :: character varying):: text
                    ) END AS "集計金額計算_期間終了日付", 
                    to_char(
                      current_timestamp(), 
                      ('YYYYMMDD' :: character varying):: text
                    ) AS "累計金額計算_現時点終了日付"
                ) calendar
            )
          ) 
          AND (
            to_char(
              c_tbecrankaddamountadm.dsorderdt, 
              ('YYYYMMDD' :: character varying):: text
            ) <= (
              SELECT 
                calendar."累計金額計算_現時点終了日付" 
              FROM 
                (
                  SELECT 
                    CASE WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0331' :: character varying):: text
                    ) THEN (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (3):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0401' :: character varying):: text
                    ) WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0930' :: character varying):: text
                    ) THEN (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (3):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('1001' :: character varying):: text
                    ) ELSE (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) - (
                              (2):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0401' :: character varying):: text
                    ) END AS "累計金額計算_期間開始日付", 
                    CASE WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0331' :: character varying):: text
                    ) THEN (
                      to_char(
                        current_timestamp(), 
                        ('YYYY' :: character varying):: text
                      ) || ('0331' :: character varying):: text
                    ) WHEN (
                      to_char(
                        current_timestamp(), 
                        ('MMDD' :: character varying):: text
                      ) <= ('0930' :: character varying):: text
                    ) THEN (
                      to_char(
                        current_timestamp(), 
                        ('YYYY' :: character varying):: text
                      ) || ('0930' :: character varying):: text
                    ) ELSE (
                      (
                        (
                          (
                            (
                              (
                                to_char(
                                  current_timestamp(), 
                                  ('YYYY' :: character varying):: text
                                )
                              ):: numeric
                            ):: numeric(18, 0) + (
                              (1):: numeric
                            ):: numeric(18, 0)
                          )
                        ):: character varying
                      ):: text || ('0331' :: character varying):: text
                    ) END AS "集計金額計算_期間終了日付", 
                    to_char(
                      current_timestamp(), 
                      ('YYYYMMDD' :: character varying):: text
                    ) AS "累計金額計算_現時点終了日付"
                ) calendar
            )
          )
        ) 
        AND (
          (
            c_tbecrankaddamountadm.dielimflg
          ):: text = ('0' :: character varying):: text
        )
      )
  ) uni 
GROUP BY 
  lpad(
    (
      (uni.diecusrid):: character varying
    ):: text, 
    10, 
    (
      (0):: character varying
    ):: text
  ) 
HAVING 
  (
    sum(uni.kingaku) > 0
  )
)
select * from final