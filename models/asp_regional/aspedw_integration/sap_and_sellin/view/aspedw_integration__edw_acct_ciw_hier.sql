with edw_account_ciw_dim as(
    select * from {{ ref('aspedw_integration__edw_account_ciw_dim') }}
),
edw_account_ciw_xref as (
    select * from {{ source('aspedw_integration', 'edw_account_ciw_xref') }}
),
tranformed as(
SELECT
  derived_table1.chrt_acct,
  derived_table1.acct_num,
  derived_table1.acct_nm,
  derived_table1.ciw_desc,
  derived_table1.ciw_code,
  derived_table1.ciw_bucket,
  derived_table1.ciw_acct_col,
  derived_table1.ciw_acct_no,
  derived_table1.ciw_acct_nm,
  derived_table1.measure_code,
  derived_table1.measure_name,
  derived_table1.multiplication_factor
FROM (
  SELECT
    a.chrt_acct,
    a.acct_num,
    a.acct_nm,
    a.ciw_desc,
    a.ciw_code,
    a.ciw_bucket,
    a.ciw_acct_col,
    a.ciw_acct_no,
    a.ciw_acct_nm,
    b.measure_code,
    b.measure_name,
    b.multiplication_factor
  FROM (
    (
      (
        (
          (
            (
              SELECT
                edw_account_ciw_dim.chrt_acct,
                edw_account_ciw_dim.acct_num,
                edw_account_ciw_dim.acct_nm,
                edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
                CAST((
                  CASE
                    WHEN (
                      (
                        (
                          (
                            (
                              (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                              )
                              OR (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                              )
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                      )
                    )
                    THEN COALESCE(
                      SPLIT_PART(
                        LTRIM(
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l5
                          ) AS TEXT), CAST('_' AS TEXT), 1),
                          CAST('JJPLAC' AS TEXT)
                        ),
                        CAST('-' AS TEXT),
                        1
                      ),
                      LTRIM(
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l4
                        ) AS TEXT), CAST('_' AS TEXT), 1),
                        CAST('JJPLAC' AS TEXT)
                      )
                    )
                    ELSE LTRIM(
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l4
                      ) AS TEXT), CAST('_' AS TEXT), 1),
                      CAST('JJPLAC' AS TEXT)
                    )
                  END
                ) AS VARCHAR) AS ciw_code,
                CASE
                  WHEN (
                    (
                      (
                        (
                          (
                            (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                    )
                  )
                  THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
                  ELSE edw_account_ciw_dim.ciw_acct_l4_txt
                END AS ciw_bucket,
                CAST('CIW_ACCT_L1' AS VARCHAR) AS ciw_acct_col,
                edw_account_ciw_dim.ciw_acct_l1 AS ciw_acct_no,
                edw_account_ciw_dim.ciw_acct_l1_txt AS ciw_acct_nm
              FROM edw_account_ciw_dim
              WHERE
                (
                  LENGTH(CAST((
                    edw_account_ciw_dim.ciw_acct_l1
                  ) AS TEXT)) <> 0
                )
              UNION ALL
              SELECT
                edw_account_ciw_dim.chrt_acct,
                edw_account_ciw_dim.acct_num,
                edw_account_ciw_dim.acct_nm,
                edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
                CAST((
                  CASE
                    WHEN (
                      (
                        (
                          (
                            (
                              (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                              )
                              OR (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                              )
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                      )
                    )
                    THEN COALESCE(
                      SPLIT_PART(
                        LTRIM(
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l5
                          ) AS TEXT), CAST('_' AS TEXT), 1),
                          CAST('JJPLAC' AS TEXT)
                        ),
                        CAST('-' AS TEXT),
                        1
                      ),
                      LTRIM(
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l4
                        ) AS TEXT), CAST('_' AS TEXT), 1),
                        CAST('JJPLAC' AS TEXT)
                      )
                    )
                    ELSE LTRIM(
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l4
                      ) AS TEXT), CAST('_' AS TEXT), 1),
                      CAST('JJPLAC' AS TEXT)
                    )
                  END
                ) AS VARCHAR) AS ciw_code,
                CASE
                  WHEN (
                    (
                      (
                        (
                          (
                            (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                    )
                  )
                  THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
                  ELSE edw_account_ciw_dim.ciw_acct_l4_txt
                END AS ciw_bucket,
                CAST('CIW_ACCT_L2' AS VARCHAR) AS ciw_acct_col,
                edw_account_ciw_dim.ciw_acct_l2 AS ciw_acct_no,
                edw_account_ciw_dim.ciw_acct_l2_txt AS ciw_acct_nm
              FROM edw_account_ciw_dim
              WHERE
                (
                  LENGTH(CAST((
                    edw_account_ciw_dim.ciw_acct_l2
                  ) AS TEXT)) <> 0
                )
            )
            UNION ALL
            SELECT
              edw_account_ciw_dim.chrt_acct,
              edw_account_ciw_dim.acct_num,
              edw_account_ciw_dim.acct_nm,
              edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
              CAST((
                CASE
                  WHEN (
                    (
                      (
                        (
                          (
                            (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                    )
                  )
                  THEN COALESCE(
                    SPLIT_PART(
                      LTRIM(
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l5
                        ) AS TEXT), CAST('_' AS TEXT), 1),
                        CAST('JJPLAC' AS TEXT)
                      ),
                      CAST('-' AS TEXT),
                      1
                    ),
                    LTRIM(
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l4
                      ) AS TEXT), CAST('_' AS TEXT), 1),
                      CAST('JJPLAC' AS TEXT)
                    )
                  )
                  ELSE LTRIM(
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l4
                    ) AS TEXT), CAST('_' AS TEXT), 1),
                    CAST('JJPLAC' AS TEXT)
                  )
                END
              ) AS VARCHAR) AS ciw_code,
              CASE
                WHEN (
                  (
                    (
                      (
                        (
                          (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                  )
                )
                THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
                ELSE edw_account_ciw_dim.ciw_acct_l4_txt
              END AS ciw_bucket,
              CAST('CIW_ACCT_L3' AS VARCHAR) AS ciw_acct_col,
              edw_account_ciw_dim.ciw_acct_l3 AS ciw_acct_no,
              edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_acct_nm
            FROM edw_account_ciw_dim
            WHERE
              (
                LENGTH(CAST((
                  edw_account_ciw_dim.ciw_acct_l3
                ) AS TEXT)) <> 0
              )
          )
          UNION ALL
          SELECT
            edw_account_ciw_dim.chrt_acct,
            edw_account_ciw_dim.acct_num,
            edw_account_ciw_dim.acct_nm,
            edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
            CAST((
              CASE
                WHEN (
                  (
                    (
                      (
                        (
                          (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                  )
                )
                THEN COALESCE(
                  SPLIT_PART(
                    LTRIM(
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l5
                      ) AS TEXT), CAST('_' AS TEXT), 1),
                      CAST('JJPLAC' AS TEXT)
                    ),
                    CAST('-' AS TEXT),
                    1
                  ),
                  LTRIM(
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l4
                    ) AS TEXT), CAST('_' AS TEXT), 1),
                    CAST('JJPLAC' AS TEXT)
                  )
                )
                ELSE LTRIM(
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l4
                  ) AS TEXT), CAST('_' AS TEXT), 1),
                  CAST('JJPLAC' AS TEXT)
                )
              END
            ) AS VARCHAR) AS ciw_code,
            CASE
              WHEN (
                (
                  (
                    (
                      (
                        (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                  )
                )
                OR (
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l3
                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                )
              )
              THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
              ELSE edw_account_ciw_dim.ciw_acct_l4_txt
            END AS ciw_bucket,
            CAST('CIW_ACCT_L4' AS VARCHAR) AS ciw_acct_col,
            edw_account_ciw_dim.ciw_acct_l4 AS ciw_acct_no,
            edw_account_ciw_dim.ciw_acct_l4_txt AS ciw_acct_nm
          FROM edw_account_ciw_dim
          WHERE
            (
              LENGTH(CAST((
                edw_account_ciw_dim.ciw_acct_l4
              ) AS TEXT)) <> 0
            )
        )
        UNION ALL
        SELECT
          edw_account_ciw_dim.chrt_acct,
          edw_account_ciw_dim.acct_num,
          edw_account_ciw_dim.acct_nm,
          edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
          CAST((
            CASE
              WHEN (
                (
                  (
                    (
                      (
                        (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                  )
                )
                OR (
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l3
                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                )
              )
              THEN COALESCE(
                SPLIT_PART(
                  LTRIM(
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l5
                    ) AS TEXT), CAST('_' AS TEXT), 1),
                    CAST('JJPLAC' AS TEXT)
                  ),
                  CAST('-' AS TEXT),
                  1
                ),
                LTRIM(
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l4
                  ) AS TEXT), CAST('_' AS TEXT), 1),
                  CAST('JJPLAC' AS TEXT)
                )
              )
              ELSE LTRIM(
                SPLIT_PART(CAST((
                  edw_account_ciw_dim.ciw_acct_l4
                ) AS TEXT), CAST('_' AS TEXT), 1),
                CAST('JJPLAC' AS TEXT)
              )
            END
          ) AS VARCHAR) AS ciw_code,
          CASE
            WHEN (
              (
                (
                  (
                    (
                      (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                  )
                )
                OR (
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l3
                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                )
              )
              OR (
                SPLIT_PART(CAST((
                  edw_account_ciw_dim.ciw_acct_l3
                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
              )
            )
            THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
            ELSE edw_account_ciw_dim.ciw_acct_l4_txt
          END AS ciw_bucket,
          CAST('CIW_ACCT_L5' AS VARCHAR) AS ciw_acct_col,
          edw_account_ciw_dim.ciw_acct_l5 AS ciw_acct_no,
          edw_account_ciw_dim.ciw_acct_l5_txt AS ciw_acct_nm
        FROM edw_account_ciw_dim
        WHERE
          (
            LENGTH(CAST((
              edw_account_ciw_dim.ciw_acct_l5
            ) AS TEXT)) <> 0
          )
      )
      UNION ALL
      SELECT
        edw_account_ciw_dim.chrt_acct,
        edw_account_ciw_dim.acct_num,
        edw_account_ciw_dim.acct_nm,
        edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
        CAST((
          CASE
            WHEN (
              (
                (
                  (
                    (
                      (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                  )
                )
                OR (
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l3
                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                )
              )
              OR (
                SPLIT_PART(CAST((
                  edw_account_ciw_dim.ciw_acct_l3
                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
              )
            )
            THEN COALESCE(
              SPLIT_PART(
                LTRIM(
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l5
                  ) AS TEXT), CAST('_' AS TEXT), 1),
                  CAST('JJPLAC' AS TEXT)
                ),
                CAST('-' AS TEXT),
                1
              ),
              LTRIM(
                SPLIT_PART(CAST((
                  edw_account_ciw_dim.ciw_acct_l4
                ) AS TEXT), CAST('_' AS TEXT), 1),
                CAST('JJPLAC' AS TEXT)
              )
            )
            ELSE LTRIM(
              SPLIT_PART(CAST((
                edw_account_ciw_dim.ciw_acct_l4
              ) AS TEXT), CAST('_' AS TEXT), 1),
              CAST('JJPLAC' AS TEXT)
            )
          END
        ) AS VARCHAR) AS ciw_code,
        CASE
          WHEN (
            (
              (
                (
                  (
                    (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                  )
                )
                OR (
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l3
                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                )
              )
              OR (
                SPLIT_PART(CAST((
                  edw_account_ciw_dim.ciw_acct_l3
                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
              )
            )
            OR (
              SPLIT_PART(CAST((
                edw_account_ciw_dim.ciw_acct_l3
              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
            )
          )
          THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
          ELSE edw_account_ciw_dim.ciw_acct_l4_txt
        END AS ciw_bucket,
        CAST('CIW_ACCT_L6' AS VARCHAR) AS ciw_acct_col,
        edw_account_ciw_dim.ciw_acct_l6 AS ciw_acct_no,
        edw_account_ciw_dim.ciw_acct_l6_txt AS ciw_acct_nm
      FROM edw_account_ciw_dim
      WHERE
        (
          LENGTH(CAST((
            edw_account_ciw_dim.ciw_acct_l6
          ) AS TEXT)) <> 0
        )
    ) AS a
    JOIN edw_account_ciw_xref AS b
      ON (
        (
          (
            UPPER(CAST((
              a.ciw_acct_col
            ) AS TEXT)) = UPPER(CAST((
              b.lookup_col_name
            ) AS TEXT))
          )
          AND (
            CAST((
              a.ciw_acct_no
            ) AS TEXT) = CAST((
              b.lookup_value
            ) AS TEXT)
          )
        )
      )
  )
) AS derived_table1

),
final as(
    select 	
    chrt_acct as chrt_acct,
    acct_num as acct_num,
    acct_nm as acct_nm,
    ciw_desc as ciw_desc,
    ciw_code as ciw_code,
    ciw_bucket as ciw_bucket,
    ciw_acct_col as ciw_acct_col,
    ciw_acct_no as ciw_acct_no,
    ciw_acct_nm as ciw_acct_nm,
    measure_code as measure_code,
    measure_name as measure_name,
    multiplication_factor as multiplication_factor 
    from tranformed
)
select * from final