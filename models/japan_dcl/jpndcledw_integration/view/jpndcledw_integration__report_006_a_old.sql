with wk_kpi_06_04_old as (
select * from {{ source('jpdcledw_integration', 'wk_kpi_06_04_old') }}
),
final as (
(
  (
    (
      (
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          SELECT 
                            '通販' AS channel_name, 
                            'A-15' AS channel_id, 
                            wk_kpi_06_04_old.yymm, 
                            sum(
                              wk_kpi_06_04_old."ユニーク契約者数"
                            ) AS "ユニーク契約者数" 
                          FROM 
                            wk_kpi_06_04_old 
                          WHERE 
                            (
                              (
                                (
                                  (wk_kpi_06_04_old."販路"):: text = ('通販' :: character varying):: text
                                ) 
                                AND (
                                  (wk_kpi_06_04_old."大区分"):: text = (
                                    '01_有効契約者数' :: character varying
                                  ):: text
                                )
                              ) 
                              AND (
                                (wk_kpi_06_04_old."小区分"):: text = (
                                  '01_有効契約者数' :: character varying
                                ):: text
                              )
                            ) 
                          GROUP BY 
                            wk_kpi_06_04_old.yymm 
                          UNION ALL 
                          SELECT 
                            '通販' AS channel_name, 
                            'A-16' AS channel_id, 
                            wk_kpi_06_04_old.yymm, 
                            sum(
                              wk_kpi_06_04_old."総契約件数"
                            ) AS "ユニーク契約者数" 
                          FROM 
                            wk_kpi_06_04_old 
                          WHERE 
                            (
                              (
                                (
                                  (wk_kpi_06_04_old."販路"):: text = ('通販' :: character varying):: text
                                ) 
                                AND (
                                  (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
                                )
                              ) 
                              AND (
                                (wk_kpi_06_04_old."小区分"):: text = ('03_新規' :: character varying):: text
                              )
                            ) 
                          GROUP BY 
                            wk_kpi_06_04_old.yymm
                        ) 
                        UNION ALL 
                        SELECT 
                          '通販' AS channel_name, 
                          'A-16-C' AS channel_id, 
                          wk_kpi_06_04_old.yymm, 
                          sum(
                            wk_kpi_06_04_old."総契約件数"
                          ) AS "ユニーク契約者数" 
                        FROM 
                          wk_kpi_06_04_old 
                        WHERE 
                          (
                            (
                              (
                                (wk_kpi_06_04_old."販路"):: text = ('通販' :: character varying):: text
                              ) 
                              AND (
                                (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
                              )
                            ) 
                            AND (
                              (wk_kpi_06_04_old."小区分"):: text = ('04_解約' :: character varying):: text
                            )
                          ) 
                        GROUP BY 
                          wk_kpi_06_04_old.yymm
                      ) 
                      UNION ALL 
                      SELECT 
                        '通販' AS channel_name, 
                        'A-17' AS channel_id, 
                        wk_kpi_06_04_old.yymm, 
                        sum(
                          wk_kpi_06_04_old."総契約件数"
                        ) AS "ユニーク契約者数" 
                      FROM 
                        wk_kpi_06_04_old 
                      WHERE 
                        (
                          (
                            (
                              (wk_kpi_06_04_old."販路"):: text = ('通販' :: character varying):: text
                            ) 
                            AND (
                              (wk_kpi_06_04_old."大区分"):: text = (
                                '01_有効契約者数' :: character varying
                              ):: text
                            )
                          ) 
                          AND (
                            (wk_kpi_06_04_old."小区分"):: text = (
                              '01_有効契約者数' :: character varying
                            ):: text
                          )
                        ) 
                      GROUP BY 
                        wk_kpi_06_04_old.yymm
                    ) 
                    UNION ALL 
                    SELECT 
                      '通販' AS channel_name, 
                      'A-18' AS channel_id, 
                      kpi01.yymm, 
                      (
                        kpi01."総契約件数" - kpi02."総契約件数"
                      ) AS "ユニーク契約者数" 
                    FROM 
                      (
                        (
                          SELECT 
                            wk_kpi_06_04_old.yymm, 
                            sum(
                              wk_kpi_06_04_old."総契約件数"
                            ) AS "総契約件数" 
                          FROM 
                            wk_kpi_06_04_old 
                          WHERE 
                            (
                              (
                                (
                                  (wk_kpi_06_04_old."販路"):: text = ('通販' :: character varying):: text
                                ) 
                                AND (
                                  (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
                                )
                              ) 
                              AND (
                                (wk_kpi_06_04_old."小区分"):: text = ('03_新規' :: character varying):: text
                              )
                            ) 
                          GROUP BY 
                            wk_kpi_06_04_old.yymm
                        ) kpi01 
                        JOIN (
                          SELECT 
                            wk_kpi_06_04_old.yymm, 
                            sum(
                              wk_kpi_06_04_old."総契約件数"
                            ) AS "総契約件数" 
                          FROM 
                            wk_kpi_06_04_old 
                          WHERE 
                            (
                              (
                                (
                                  (wk_kpi_06_04_old."販路"):: text = ('通販' :: character varying):: text
                                ) 
                                AND (
                                  (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
                                )
                              ) 
                              AND (
                                (wk_kpi_06_04_old."小区分"):: text = ('04_解約' :: character varying):: text
                              )
                            ) 
                          GROUP BY 
                            wk_kpi_06_04_old.yymm
                        ) kpi02 ON (
                          (
                            (kpi01.yymm):: text = (kpi02.yymm):: text
                          )
                        )
                      )
                  ) 
                  UNION ALL 
                  SELECT 
                    'WEB' AS channel_name, 
                    'A-15' AS channel_id, 
                    wk_kpi_06_04_old.yymm, 
                    sum(
                      wk_kpi_06_04_old."ユニーク契約者数"
                    ) AS "ユニーク契約者数" 
                  FROM 
                    wk_kpi_06_04_old 
                  WHERE 
                    (
                      (
                        (
                          (wk_kpi_06_04_old."販路"):: text = ('ＷＥＢ' :: character varying):: text
                        ) 
                        AND (
                          (wk_kpi_06_04_old."大区分"):: text = (
                            '01_有効契約者数' :: character varying
                          ):: text
                        )
                      ) 
                      AND (
                        (wk_kpi_06_04_old."小区分"):: text = (
                          '01_有効契約者数' :: character varying
                        ):: text
                      )
                    ) 
                  GROUP BY 
                    wk_kpi_06_04_old.yymm
                ) 
                UNION ALL 
                SELECT 
                  'WEB' AS channel_name, 
                  'A-16' AS channel_id, 
                  wk_kpi_06_04_old.yymm, 
                  sum(
                    wk_kpi_06_04_old."総契約件数"
                  ) AS "ユニーク契約者数" 
                FROM 
                  wk_kpi_06_04_old 
                WHERE 
                  (
                    (
                      (
                        (wk_kpi_06_04_old."販路"):: text = ('ＷＥＢ' :: character varying):: text
                      ) 
                      AND (
                        (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
                      )
                    ) 
                    AND (
                      (wk_kpi_06_04_old."小区分"):: text = ('03_新規' :: character varying):: text
                    )
                  ) 
                GROUP BY 
                  wk_kpi_06_04_old.yymm
              ) 
              UNION ALL 
              SELECT 
                'WEB' AS channel_name, 
                'A-16-C' AS channel_id, 
                wk_kpi_06_04_old.yymm, 
                sum(
                  wk_kpi_06_04_old."総契約件数"
                ) AS "ユニーク契約者数" 
              FROM 
                wk_kpi_06_04_old 
              WHERE 
                (
                  (
                    (
                      (wk_kpi_06_04_old."販路"):: text = ('通販' :: character varying):: text
                    ) 
                    AND (
                      (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
                    )
                  ) 
                  AND (
                    (wk_kpi_06_04_old."小区分"):: text = ('04_解約' :: character varying):: text
                  )
                ) 
              GROUP BY 
                wk_kpi_06_04_old.yymm
            ) 
            UNION ALL 
            SELECT 
              'WEB' AS channel_name, 
              'A-17' AS channel_id, 
              wk_kpi_06_04_old.yymm, 
              sum(
                wk_kpi_06_04_old."総契約件数"
              ) AS "ユニーク契約者数" 
            FROM 
              wk_kpi_06_04_old 
            WHERE 
              (
                (
                  (
                    (wk_kpi_06_04_old."販路"):: text = ('ＷＥＢ' :: character varying):: text
                  ) 
                  AND (
                    (wk_kpi_06_04_old."大区分"):: text = (
                      '01_有効契約者数' :: character varying
                    ):: text
                  )
                ) 
                AND (
                  (wk_kpi_06_04_old."小区分"):: text = (
                    '01_有効契約者数' :: character varying
                  ):: text
                )
              ) 
            GROUP BY 
              wk_kpi_06_04_old.yymm
          ) 
          UNION ALL 
          SELECT 
            'WEB' AS channel_name, 
            'A-18' AS channel_id, 
            kpi01.yymm, 
            (
              kpi01."総契約件数" - kpi02."総契約件数"
            ) AS "ユニーク契約者数" 
          FROM 
            (
              (
                SELECT 
                  wk_kpi_06_04_old.yymm, 
                  sum(
                    wk_kpi_06_04_old."総契約件数"
                  ) AS "総契約件数" 
                FROM 
                  wk_kpi_06_04_old 
                WHERE 
                  (
                    (
                      (
                        (wk_kpi_06_04_old."販路"):: text = ('ＷＥＢ' :: character varying):: text
                      ) 
                      AND (
                        (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
                      )
                    ) 
                    AND (
                      (wk_kpi_06_04_old."小区分"):: text = ('03_新規' :: character varying):: text
                    )
                  ) 
                GROUP BY 
                  wk_kpi_06_04_old.yymm
              ) kpi01 
              JOIN (
                SELECT 
                  wk_kpi_06_04_old.yymm, 
                  sum(
                    wk_kpi_06_04_old."総契約件数"
                  ) AS "総契約件数" 
                FROM 
                  wk_kpi_06_04_old 
                WHERE 
                  (
                    (
                      (
                        (wk_kpi_06_04_old."販路"):: text = ('ＷＥＢ' :: character varying):: text
                      ) 
                      AND (
                        (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
                      )
                    ) 
                    AND (
                      (wk_kpi_06_04_old."小区分"):: text = ('04_解約' :: character varying):: text
                    )
                  ) 
                GROUP BY 
                  wk_kpi_06_04_old.yymm
              ) kpi02 ON (
                (
                  (kpi01.yymm):: text = (kpi02.yymm):: text
                )
              )
            )
        ) 
        UNION ALL 
        SELECT 
          'DTC' AS channel_name, 
          'A-15' AS channel_id, 
          wk_kpi_06_04_old.yymm, 
          sum(
            wk_kpi_06_04_old."ユニーク契約者数"
          ) AS "ユニーク契約者数" 
        FROM 
          wk_kpi_06_04_old 
        WHERE 
          (
            (
              (
                (wk_kpi_06_04_old."販路"):: text = ('ＤＴＣ' :: character varying):: text
              ) 
              AND (
                (wk_kpi_06_04_old."大区分"):: text = (
                  '01_有効契約者数' :: character varying
                ):: text
              )
            ) 
            AND (
              (wk_kpi_06_04_old."小区分"):: text = (
                '01_有効契約者数' :: character varying
              ):: text
            )
          ) 
        GROUP BY 
          wk_kpi_06_04_old.yymm
      ) 
      UNION ALL 
      SELECT 
        'DTC' AS channel_name, 
        'A-16' AS channel_id, 
        wk_kpi_06_04_old.yymm, 
        sum(
          wk_kpi_06_04_old."総契約件数"
        ) AS "ユニーク契約者数" 
      FROM 
        wk_kpi_06_04_old 
      WHERE 
        (
          (
            (
              (wk_kpi_06_04_old."販路"):: text = ('ＤＴＣ' :: character varying):: text
            ) 
            AND (
              (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
            )
          ) 
          AND (
            (wk_kpi_06_04_old."小区分"):: text = ('03_新規' :: character varying):: text
          )
        ) 
      GROUP BY 
        wk_kpi_06_04_old.yymm
    ) 
    UNION ALL 
    SELECT 
      'DTC' AS channel_name, 
      'A-16-C' AS channel_id, 
      wk_kpi_06_04_old.yymm, 
      sum(
        wk_kpi_06_04_old."総契約件数"
      ) AS "ユニーク契約者数" 
    FROM 
      wk_kpi_06_04_old 
    WHERE 
      (
        (
          (
            (wk_kpi_06_04_old."販路"):: text = ('ＤＴＣ' :: character varying):: text
          ) 
          AND (
            (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
          )
        ) 
        AND (
          (wk_kpi_06_04_old."小区分"):: text = ('04_解約' :: character varying):: text
        )
      ) 
    GROUP BY 
      wk_kpi_06_04_old.yymm
  ) 
  UNION ALL 
  SELECT 
    'DTC' AS channel_name, 
    'A-17' AS channel_id, 
    wk_kpi_06_04_old.yymm, 
    sum(
      wk_kpi_06_04_old."総契約件数"
    ) AS "ユニーク契約者数" 
  FROM 
    wk_kpi_06_04_old 
  WHERE 
    (
      (
        (
          (wk_kpi_06_04_old."販路"):: text = ('ＤＴＣ' :: character varying):: text
        ) 
        AND (
          (wk_kpi_06_04_old."大区分"):: text = (
            '01_有効契約者数' :: character varying
          ):: text
        )
      ) 
      AND (
        (wk_kpi_06_04_old."小区分"):: text = (
          '01_有効契約者数' :: character varying
        ):: text
      )
    ) 
  GROUP BY 
    wk_kpi_06_04_old.yymm
) 
UNION ALL 
SELECT 
  'DTC' AS channel_name, 
  'A-18' AS channel_id, 
  kpi01.yymm, 
  (
    kpi01."総契約件数" - kpi02."総契約件数"
  ) AS "ユニーク契約者数" 
FROM 
  (
    (
      SELECT 
        wk_kpi_06_04_old.yymm, 
        sum(
          wk_kpi_06_04_old."総契約件数"
        ) AS "総契約件数" 
      FROM 
        wk_kpi_06_04_old 
      WHERE 
        (
          (
            (
              (wk_kpi_06_04_old."販路"):: text = ('ＤＴＣ' :: character varying):: text
            ) 
            AND (
              (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
            )
          ) 
          AND (
            (wk_kpi_06_04_old."小区分"):: text = ('03_新規' :: character varying):: text
          )
        ) 
      GROUP BY 
        wk_kpi_06_04_old.yymm
    ) kpi01 
    JOIN (
      SELECT 
        wk_kpi_06_04_old.yymm, 
        sum(
          wk_kpi_06_04_old."総契約件数"
        ) AS "総契約件数" 
      FROM 
        wk_kpi_06_04_old 
      WHERE 
        (
          (
            (
              (wk_kpi_06_04_old."販路"):: text = ('ＤＴＣ' :: character varying):: text
            ) 
            AND (
              (wk_kpi_06_04_old."大区分"):: text = ('02_内訳' :: character varying):: text
            )
          ) 
          AND (
            (wk_kpi_06_04_old."小区分"):: text = ('04_解約' :: character varying):: text
          )
        ) 
      GROUP BY 
        wk_kpi_06_04_old.yymm
    ) kpi02 ON (
      (
        (kpi01.yymm):: text = (kpi02.yymm):: text
      )
    )
  )
)
select * from final

