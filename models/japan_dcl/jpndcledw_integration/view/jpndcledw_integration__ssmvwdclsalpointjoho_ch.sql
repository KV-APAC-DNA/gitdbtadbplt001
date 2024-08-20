
with zcmmntorisk as (
select * from {{ source('jpdclitg_integration', 'zcmmntorisk') }}
),
zcmmnsoshitaik as (
select * from {{ source('jpdclitg_integration', 'zcmmnsoshitaik') }}
),
ssmthdclsalhephedda as (
select * from {{ source('jpdclitg_integration', 'ssmthdclsalhephedda') }}
),
ssmthdclthanjuchhedda as (
select * from {{ source('jpdclitg_integration', 'ssmthdclthanjuchhedda') }}
),
ssmtndclsaljisk as (
select * from {{ source('jpdclitg_integration', 'ssmtndclsaljisk') }}
),
ssmthsalhephedda as (
select * from {{ source('jpdclitg_integration', 'ssmthsalhephedda') }}
),
ssmthshkahedda as (
select * from {{ source('jpdclitg_integration', 'ssmthshkahedda') }}
),
ssmtndclshkahedda as (
select * from {{ source('jpdclitg_integration', 'ssmtndclshkahedda') }}
),
ssmthjuchhedda as (
select * from {{ source('jpdclitg_integration', 'ssmthjuchhedda') }}
),
ssmthsalhedda as (
select * from {{ source('jpdclitg_integration', 'ssmthsalhedda') }}
),
ssctnkaisytimei as (
select * from {{ source('jpdclitg_integration', 'ssctnkaisytimei') }}
),
aartnskyden as (
select * from {{ source('jpdclitg_integration', 'aartnskyden') }}
),
union_1 as (
  SELECT 
    al001.kaisha_cd AS "会社コード", 
    al001.sal_no AS "売上番号", 
    al002.juch_no AS "受注番号", 
    NULL AS "売上返品番号", 
    al003.dclksai_id AS "決済id", 
    al003.dclsm_keiro_id AS "経路id", 
    al003.dclsm_keiro_nms AS "経路名", 
    al001.sal_keij_dt AS "売上計上日", 
    al001.tokuisk_cd AS "得意先コード", 
    al005.torisk_nmr AS "得意先名", 
    COALESCE(al009.skysk_cd, al001.skysk_cd) AS "請求先コード", 
    al006.torisk_nmr AS "請求先名", 
    al001.kais_kssai_jken_nmr AS "回収決済条件名", 
    al004.skyshime_dt AS "請求締日", 
    (
      al001.den_nf_shm_sal_kin + al001.den_naf_sal_sz_kin
    ) AS "伝票値引後売上金額_税込", 
    al001.den_nf_shm_sal_kin AS "伝票値引後売上金額_税抜", 
    al001.den_naf_sal_sz_kin AS "伝票値引後売上消費税金額", 
    CASE WHEN (
      (al003.dcljuch_siyo_pt):: text = ('*****' :: character varying):: text
    ) THEN to_number(
      (
        COALESCE(
          al003.dcljuch_siyo_point_c, '0' :: character varying
        )
      ):: text, 
      (
        '99,999,999,999' :: character varying
      ):: text
    ) ELSE to_number(
      (al003.dcljuch_siyo_pt):: text, 
      (
        '99,999,999,999' :: character varying
      ):: text
    ) END AS "使用ポイント", 
    CASE WHEN (
      (al003.dcljuch_siyo_pt_koukan):: text = ('*****' :: character varying):: text
    ) THEN to_number(
      (
        COALESCE(
          al003.dcljuch_siyo_pt_koukan_c, 
          '0' :: character varying
        )
      ):: text, 
      (
        '99,999,999,999' :: character varying
      ):: text
    ) ELSE to_number(
      (al003.dcljuch_siyo_pt_koukan):: text, 
      (
        '99,999,999,999' :: character varying
      ):: text
    ) END AS "使用ポイント＿交換", 
    CASE WHEN (
      (al003.dclfuyo_pt):: text = ('*****' :: character varying):: text
    ) THEN (
      (0):: numeric
    ):: numeric(18, 0) ELSE to_number(
      (al003.dclfuyo_pt):: text, 
      (
        '99,999,999,999' :: character varying
      ):: text
    ) END AS "付与ポイント", 
    al008.dclshka_memo AS "送り状番号", 
    al010.bmn_hyouji_cd AS "部門表示コード", 
    al010.bmn_nmr AS "部門名＿略", 
    al003.dclkokyk_id AS "顧客ｉｄ" 
  FROM 
    (
      (
        (
          (
            (
              (
                (
                  (
                    (
                      ssmthsalhedda al001 
                      LEFT JOIN ssmthjuchhedda al002 ON (
                        (
                          (
                            (al001.kaisha_cd):: text = (al002.kaisha_cd):: text
                          ) 
                          AND (
                            (al001.hassei_mt_den_no):: text = (al002.juch_no):: text
                          )
                        )
                      )
                    ) 
                    LEFT JOIN ssmthdclthanjuchhedda al003 ON (
                      (
                        (
                          (al001.kaisha_cd):: text = (al003.kaisha_cd):: text
                        ) 
                        AND (
                          (al001.hassei_mt_den_no):: text = (al003.juch_no):: text
                        )
                      )
                    )
                  ) 
                  LEFT JOIN ssctnkaisytimei al004 ON (
                    (
                      (
                        (
                          (
                            (
                              (al001.kaisha_cd):: text = (al004.kaisha_cd):: text
                            ) 
                            AND (
                              (al001.hass_mt_den_shubt):: text = (
                                al004.kaisyti_hass_mt_den_shubt
                              ):: text
                            )
                          ) 
                          AND (
                            (al001.hassei_mt_den_no):: text = (al004.kaisyti_hass_mt_den_no):: text
                          )
                        ) 
                        AND (
                          al004.kaisyti_hass_mt_den_row_no = 0
                        )
                      ) 
                      AND (al004.kaisyti_meirow_no = 1)
                    )
                  )
                ) 
                LEFT JOIN zcmmntorisk al005 ON (
                  (
                    (
                      (al001.kaisha_cd):: text = (al005.kaisha_cd):: text
                    ) 
                    AND (
                      (al001.tokuisk_cd):: text = (al005.torisk_cd):: text
                    )
                  )
                )
              ) 
              LEFT JOIN aartnskyden al009 ON (
                (
                  (
                    (
                      (al001.kaisha_cd):: text = (al009.kaisha_cd):: text
                    ) 
                    AND (
                      (al009.hassei_mt_den_shubt_kbn):: text = ('107' :: character varying):: text
                    )
                  ) 
                  AND (
                    (al009.hassei_mt_den_no):: text = (al001.sal_no):: text
                  )
                )
              )
            ) 
            LEFT JOIN zcmmntorisk al006 ON (
              (
                (
                  (al001.kaisha_cd):: text = (al006.kaisha_cd):: text
                ) 
                AND (
                  (
                    COALESCE(al009.skysk_cd, al001.skysk_cd)
                  ):: text = (al006.torisk_cd):: text
                )
              )
            )
          ) 
          LEFT JOIN ssmthshkahedda al007 ON (
            (
              (
                (
                  (
                    (al002.kaisha_cd):: text = (al007.kaisha_cd):: text
                  ) 
                  AND (
                    (al002.juch_no):: text = (al007.rend_mt_den_no):: text
                  )
                ) 
                AND (
                  (al007.rend_mt_den_shubt):: text = ('104' :: character varying):: text
                )
              ) 
              AND (
                al007.can_flg = (0):: smallint
              )
            )
          )
        ) 
        LEFT JOIN ssmtndclshkahedda al008 ON (
          (
            (
              (al007.kaisha_cd):: text = (al008.kaisha_cd):: text
            ) 
            AND (
              (al007.shka_no):: text = (al008.shka_no):: text
            )
          )
        )
      ) 
      LEFT JOIN zcmmnsoshitaik al010 ON (
        (
          (
            (al001.kaisha_cd):: text = (al010.kaisha_cd):: text
          ) 
          AND (
            (al001.sal_bmn_naibu_no):: text = (al010.bmn_naibukanri_no):: text
          )
        )
      )
    ) 
  WHERE 
    (
      (
        al001.can_flg = (0):: smallint
      ) 
      AND (
        (al001.rend_mt_den_shubt):: text = ('105' :: character varying):: text
      )
    ) 
  UNION 
  SELECT 
    al001.kaisha_cd AS "会社コード", 
    al001.sal_no AS "売上番号", 
    NULL AS "受注番号", 
    al002.sal_hep_no AS "売上返品番号", 
    al003.dclksai_id AS "決済id", 
    al003.dclsm_keiro_id AS "経路id", 
    al003.dclsm_keiro_nms AS "経路名", 
    al001.sal_keij_dt AS "売上計上日", 
    al001.tokuisk_cd AS "得意先コード", 
    al005.torisk_nmr AS "得意先名", 
    COALESCE(al009.skysk_cd, al001.skysk_cd) AS "請求先コード", 
    al006.torisk_nmr AS "請求先名", 
    al001.kais_kssai_jken_nmr AS "回収決済条件名", 
    al004.skyshime_dt AS "請求締日", 
    (
      al001.den_nf_shm_sal_kin + al001.den_naf_sal_sz_kin
    ) AS "伝票値引後売上金額_税込", 
    al001.den_nf_shm_sal_kin AS "伝票値引後売上金額_税抜", 
    al001.den_naf_sal_sz_kin AS "伝票値引後売上消費税金額", 
    CASE WHEN (
      (al001.sal_den_kbn):: text = ('7' :: character varying):: text
    ) THEN (
      COALESCE(
        al003.dclhenkyak_pt, 
        (0):: bigint
      ) * -1
    ) ELSE COALESCE(
      al003.dclhenkyak_pt, 
      (0):: bigint
    ) END AS "使用ポイント", 
    NULL AS "使用ポイント＿交換", 
    NULL AS "付与ポイント", 
    NULL AS "送り状番号", 
    al010.bmn_hyouji_cd AS "部門表示コード", 
    al010.bmn_nmr AS "部門名＿略", 
    al003.dclkokyk_id AS "顧客ｉｄ" 
  FROM 
    (
      (
        (
          (
            (
              (
                (
                  ssmthsalhedda al001 
                  LEFT JOIN ssmthsalhephedda al002 ON (
                    (
                      (
                        (al001.kaisha_cd):: text = (al002.kaisha_cd):: text
                      ) 
                      AND (
                        (al001.rend_mt_den_no):: text = (al002.sal_hep_no):: text
                      )
                    )
                  )
                ) 
                LEFT JOIN ssmthdclsalhephedda al003 ON (
                  (
                    (
                      (al001.kaisha_cd):: text = (al003.kaisha_cd):: text
                    ) 
                    AND (
                      (al001.rend_mt_den_no):: text = (al003.sal_hep_no):: text
                    )
                  )
                )
              ) 
              LEFT JOIN ssctnkaisytimei al004 ON (
                (
                  (
                    (
                      (
                        (
                          (al001.kaisha_cd):: text = (al004.kaisha_cd):: text
                        ) 
                        AND (
                          (al001.rend_mt_den_shubt):: text = (
                            al004.kaisyti_hass_mt_den_shubt
                          ):: text
                        )
                      ) 
                      AND (
                        (al001.rend_mt_den_no):: text = (al004.kaisyti_hass_mt_den_no):: text
                      )
                    ) 
                    AND (
                      al004.kaisyti_hass_mt_den_row_no = 0
                    )
                  ) 
                  AND (al004.kaisyti_meirow_no = 1)
                )
              )
            ) 
            LEFT JOIN zcmmntorisk al005 ON (
              (
                (
                  (al001.kaisha_cd):: text = (al005.kaisha_cd):: text
                ) 
                AND (
                  (al001.tokuisk_cd):: text = (al005.torisk_cd):: text
                )
              )
            )
          ) 
          LEFT JOIN aartnskyden al009 ON (
            (
              (
                (
                  (al001.kaisha_cd):: text = (al009.kaisha_cd):: text
                ) 
                AND (
                  (al009.hassei_mt_den_shubt_kbn):: text = ('107' :: character varying):: text
                )
              ) 
              AND (
                (al009.hassei_mt_den_no):: text = (al001.sal_no):: text
              )
            )
          )
        ) 
        LEFT JOIN zcmmntorisk al006 ON (
          (
            (
              (al001.kaisha_cd):: text = (al006.kaisha_cd):: text
            ) 
            AND (
              (
                COALESCE(al009.skysk_cd, al001.skysk_cd)
              ):: text = (al006.torisk_cd):: text
            )
          )
        )
      ) 
      LEFT JOIN zcmmnsoshitaik al010 ON (
        (
          (
            (al001.kaisha_cd):: text = (al010.kaisha_cd):: text
          ) 
          AND (
            (al001.sal_bmn_naibu_no):: text = (al010.bmn_naibukanri_no):: text
          )
        )
      )
    ) 
  WHERE 
    (
      (
        al001.can_flg = (0):: smallint
      ) 
      AND (
        (al001.rend_mt_den_shubt):: text = ('109' :: character varying):: text
      )
    )
) ,
union_2 as (SELECT 
  al001.kaisha_cd AS "会社コード", 
  al001.sal_no AS "売上番号", 
  NULL AS "受注番号", 
  NULL AS "売上返品番号", 
  NULL AS "決済id", 
  99 AS "経路id", 
  '店舗その他' AS "経路名", 
  al001.sal_keij_dt AS "売上計上日", 
  al001.tokuisk_cd AS "得意先コード", 
  al004.torisk_nmr AS "得意先名", 
  COALESCE(al009.skysk_cd, al001.skysk_cd) AS "請求先コード", 
  al005.torisk_nmr AS "請求先名", 
  al001.kais_kssai_jken_nmr AS "回収決済条件名", 
  al003.skyshime_dt AS "請求締日", 
  (
    al001.den_nf_shm_sal_kin + al001.den_naf_sal_sz_kin
  ) AS "伝票値引後売上金額_税込", 
  al001.den_nf_shm_sal_kin AS "伝票値引後売上金額_税抜", 
  al001.den_naf_sal_sz_kin AS "伝票値引後売上消費税金額", 
  COALESCE(al002.dclsiyo_pt, 0) AS "使用ポイント", 
  NULL AS "使用ポイント＿交換", 
  NULL AS "付与ポイント", 
  NULL AS "送り状番号", 
  al010.bmn_hyouji_cd AS "部門表示コード", 
  al010.bmn_nmr AS "部門名＿略", 
  NULL AS "顧客ｉｄ" 
FROM 
  (
    (
      (
        (
          (
            (
              ssmthsalhedda al001 
              LEFT JOIN ssmtndclsaljisk al002 ON (
                (
                  (
                    (al001.kaisha_cd):: text = (al002.kaisha_cd):: text
                  ) 
                  AND (
                    (al001.sal_no):: text = (al002.sal_no):: text
                  )
                )
              )
            ) 
            LEFT JOIN ssctnkaisytimei al003 ON (
              (
                (
                  (
                    (
                      (
                        (al001.kaisha_cd):: text = (al003.kaisha_cd):: text
                      ) 
                      AND (
                        (
                          al003.kaisyti_hass_mt_den_shubt
                        ):: text = ('107' :: character varying):: text
                      )
                    ) 
                    AND (
                      (al001.sal_no):: text = (al003.kaisyti_hass_mt_den_no):: text
                    )
                  ) 
                  AND (
                    al003.kaisyti_hass_mt_den_row_no = 0
                  )
                ) 
                AND (al003.kaisyti_meirow_no = 1)
              )
            )
          ) 
          LEFT JOIN zcmmntorisk al004 ON (
            (
              (
                (al001.kaisha_cd):: text = (al004.kaisha_cd):: text
              ) 
              AND (
                (al001.tokuisk_cd):: text = (al004.torisk_cd):: text
              )
            )
          )
        ) 
        LEFT JOIN aartnskyden al009 ON (
          (
            (
              (
                (al001.kaisha_cd):: text = (al009.kaisha_cd):: text
              ) 
              AND (
                (al009.hassei_mt_den_shubt_kbn):: text = ('107' :: character varying):: text
              )
            ) 
            AND (
              (al009.hassei_mt_den_no):: text = (al001.sal_no):: text
            )
          )
        )
      ) 
      LEFT JOIN zcmmntorisk al005 ON (
        (
          (
            (al001.kaisha_cd):: text = (al005.kaisha_cd):: text
          ) 
          AND (
            (
              COALESCE(al009.skysk_cd, al001.skysk_cd)
            ):: text = (al005.torisk_cd):: text
          )
        )
      )
    ) 
    LEFT JOIN zcmmnsoshitaik al010 ON (
      (
        (
          (al001.kaisha_cd):: text = (al010.kaisha_cd):: text
        ) 
        AND (
          (al001.sal_bmn_naibu_no):: text = (al010.bmn_naibukanri_no):: text
        )
      )
    )
  ) 
WHERE 
  (
    (
      al001.can_flg = (0):: smallint
    ) 
    AND (
      (al001.sal_shur_kbn):: text = ('2' :: character varying):: text
    )
  )),
final as (

select * from union_1
UNION
select * from union_2
)
select * from final