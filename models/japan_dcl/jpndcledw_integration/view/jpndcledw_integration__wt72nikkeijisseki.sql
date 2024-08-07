with 
cit85osalh_kaigai as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT85OSALH_KAIGAI
),
cit85osalh as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT85OSALH
),
cit80saleh as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.cit80saleh
),
cim02tokui as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.cim02tokui
),
final as (
SELECT 
  t.shukanengetu, 
  CASE WHEN (
    (
      (
        (
          (
            (t.channel):: text = ('111' :: character varying):: text
          ) 
          OR (
            (t.channel):: text = ('112' :: character varying):: text
          )
        ) 
        OR (
          (t.channel):: text = ('113' :: character varying):: text
        )
      ) 
      OR (
        (t.channel):: text = ('114' :: character varying):: text
      )
    ) 
    OR (
      (t.channel):: text = ('121' :: character varying):: text
    )
  ) THEN '通信販売' :: character varying WHEN (
    (
      (
        (
          (t.channel):: text = ('211' :: character varying):: text
        ) 
        OR (
          (t.channel):: text = ('212' :: character varying):: text
        )
      ) 
      OR (
        (t.channel):: text = ('213' :: character varying):: text
      )
    ) 
    OR (
      (t.channel):: text = ('214' :: character varying):: text
    )
  ) THEN '対面販売' :: character varying WHEN (
    (
      (
        (
          (
            (t.channel):: text = ('311' :: character varying):: text
          ) 
          OR (
            (t.channel):: text = ('312' :: character varying):: text
          )
        ) 
        OR (
          (t.channel):: text = ('313' :: character varying):: text
        )
      ) 
      OR (
        (t.channel):: text = ('314' :: character varying):: text
      )
    ) 
    OR (
      (t.channel):: text = ('321' :: character varying):: text
    )
  ) THEN '卸売' :: character varying WHEN (
    (
      (
        (t.channel):: text = ('411' :: character varying):: text
      ) 
      OR (
        (t.channel):: text = ('412' :: character varying):: text
      )
    ) 
    OR (
      (t.channel):: text = ('413' :: character varying):: text
    )
  ) THEN '海外' :: character varying WHEN (
    (
      (t.channel):: text = ('511' :: character varying):: text
    ) 
    OR (
      (t.channel):: text = ('512' :: character varying):: text
    )
  ) THEN 'その他' :: character varying ELSE NULL :: character varying END AS channel1, 
  CASE WHEN (
    (
      (
        (
          (t.channel):: text = ('111' :: character varying):: text
        ) 
        OR (
          (t.channel):: text = ('112' :: character varying):: text
        )
      ) 
      OR (
        (t.channel):: text = ('113' :: character varying):: text
      )
    ) 
    OR (
      (t.channel):: text = ('114' :: character varying):: text
    )
  ) THEN '通販' :: character varying WHEN (
    (t.channel):: text = ('121' :: character varying):: text
  ) THEN 'WEB' :: character varying WHEN (
    (
      (
        (
          (t.channel):: text = ('211' :: character varying):: text
        ) 
        OR (
          (t.channel):: text = ('212' :: character varying):: text
        )
      ) 
      OR (
        (t.channel):: text = ('213' :: character varying):: text
      )
    ) 
    OR (
      (t.channel):: text = ('214' :: character varying):: text
    )
  ) THEN '店舗' :: character varying WHEN (
    (
      (
        (
          (t.channel):: text = ('311' :: character varying):: text
        ) 
        OR (
          (t.channel):: text = ('312' :: character varying):: text
        )
      ) 
      OR (
        (t.channel):: text = ('313' :: character varying):: text
      )
    ) 
    OR (
      (t.channel):: text = ('314' :: character varying):: text
    )
  ) THEN '卸売' :: character varying WHEN (
    (t.channel):: text = ('321' :: character varying):: text
  ) THEN 'QVC' :: character varying WHEN (
    (t.channel):: text = ('411' :: character varying):: text
  ) THEN 'JJ' :: character varying WHEN (
    (
      (t.channel):: text = ('412' :: character varying):: text
    ) 
    OR (
      (t.channel):: text = ('413' :: character varying):: text
    )
  ) THEN '海外' :: character varying WHEN (
    (
      (t.channel):: text = ('511' :: character varying):: text
    ) 
    OR (
      (t.channel):: text = ('512' :: character varying):: text
    )
  ) THEN 'その他' :: character varying ELSE NULL :: character varying END AS channel2, 
  CASE WHEN (
    (t.channel):: text = ('111' :: character varying):: text
  ) THEN '通販' :: character varying WHEN (
    (t.channel):: text = ('112' :: character varying):: text
  ) THEN '社販' :: character varying WHEN (
    (t.channel):: text = ('113' :: character varying):: text
  ) THEN 'VIP' :: character varying WHEN (
    (t.channel):: text = ('114' :: character varying):: text
  ) THEN '通販' :: character varying WHEN (
    (t.channel):: text = ('121' :: character varying):: text
  ) THEN 'WEB' :: character varying WHEN (
    (t.channel):: text = ('211' :: character varying):: text
  ) THEN '買取' :: character varying WHEN (
    (t.channel):: text = ('212' :: character varying):: text
  ) THEN '直営' :: character varying WHEN (
    (t.channel):: text = ('213' :: character varying):: text
  ) THEN '消化' :: character varying WHEN (
    (t.channel):: text = ('214' :: character varying):: text
  ) THEN 'アウトレット' :: character varying WHEN (
    (t.channel):: text = ('311' :: character varying):: text
  ) THEN '代理店' :: character varying WHEN (
    (t.channel):: text = ('312' :: character varying):: text
  ) THEN '職域（特販）' :: character varying WHEN (
    (t.channel):: text = ('313' :: character varying):: text
  ) THEN '職域（代理店）' :: character varying WHEN (
    (t.channel):: text = ('314' :: character varying):: text
  ) THEN '職域（販売会）' :: character varying WHEN (
    (t.channel):: text = ('321' :: character varying):: text
  ) THEN 'QVC' :: character varying WHEN (
    (t.channel):: text = ('411' :: character varying):: text
  ) THEN 'JJ' :: character varying WHEN (
    (t.channel):: text = ('412' :: character varying):: text
  ) THEN '国内免税' :: character varying WHEN (
    (t.channel):: text = ('413' :: character varying):: text
  ) THEN '海外免税' :: character varying WHEN (
    (t.channel):: text = ('511' :: character varying):: text
  ) THEN 'FS' :: character varying WHEN (
    (t.channel):: text = ('512' :: character varying):: text
  ) THEN 'その他' :: character varying ELSE NULL :: character varying END AS channel3, 
  t.tokuicode, 
  (
    (
      (t.tokuicode):: text || (
        nvl2(
          cim02.tokuiname, ' : ' :: character varying, 
          '' :: character varying
        )
      ):: text
    ) || COALESCE(
      (
        (
          (cim02.tokuiname):: text || (' ' :: character varying):: text
        ) || (cim02.tokuiname_ryaku):: text
      ), 
      (
        (' ' :: character varying):: character varying(65535)
      ):: text
    )
  ) AS tokuiname, 
  t.pointkomikingaku, 
  t.riyopoint, 
  t.bmn_hyouji_cd, 
  t.bmn_nms 
FROM 
  (
    (
      (
        SELECT 
          cit80saleh.shukadate AS shukanengetu, 
          CASE WHEN (
            cit80saleh.kakokbn = (
              (0):: numeric
            ):: numeric(18, 0)
          ) THEN CASE WHEN (
            cit80saleh.smkeiroid = (
              (5):: numeric
            ):: numeric(18, 0)
          ) THEN '121' :: character varying ELSE CASE WHEN (
            cit80saleh.smkeiroid = (
              (6):: numeric
            ):: numeric(18, 0)
          ) THEN '112' :: character varying WHEN (
            cit80saleh.marker = (
              (4):: numeric
            ):: numeric(18, 0)
          ) THEN '511' :: character varying ELSE '111' :: character varying END END ELSE CASE WHEN (
            (cit80saleh.kaisha):: text = ('000' :: character varying):: text
          ) THEN CASE WHEN (
            (cit80saleh.daihanrobunname):: text = ('Web' :: character varying):: text
          ) THEN '121' :: character varying ELSE '111' :: character varying END WHEN (
            (cit80saleh.kaisha):: text = ('001' :: character varying):: text
          ) THEN '112' :: character varying ELSE '114' :: character varying END END AS channel, 
          'ダミーコード' :: character varying AS tokuicode, 
          sum(
            cit80saleh.pointkominukikingaku
          ) AS pointkomikingaku, 
          sum(cit80saleh.riyopoint) AS riyopoint, 
          cit80saleh.bmn_hyouji_cd, 
          cit80saleh.bmn_nms 
        FROM 
          cit80saleh 
        WHERE 
          (
            (
              cit80saleh.cancelflg = (
                (0):: numeric
              ):: numeric(18, 0)
            ) 
            AND (
              (cit80saleh.torikeikbn):: text = ('01' :: character varying):: text
            )
          ) 
        GROUP BY 
          cit80saleh.shukadate, 
          CASE WHEN (
            cit80saleh.kakokbn = (
              (0):: numeric
            ):: numeric(18, 0)
          ) THEN CASE WHEN (
            cit80saleh.smkeiroid = (
              (5):: numeric
            ):: numeric(18, 0)
          ) THEN '121' :: character varying ELSE CASE WHEN (
            cit80saleh.smkeiroid = (
              (6):: numeric
            ):: numeric(18, 0)
          ) THEN '112' :: character varying WHEN (
            cit80saleh.marker = (
              (4):: numeric
            ):: numeric(18, 0)
          ) THEN '511' :: character varying ELSE '111' :: character varying END END ELSE CASE WHEN (
            (cit80saleh.kaisha):: text = ('000' :: character varying):: text
          ) THEN CASE WHEN (
            (cit80saleh.daihanrobunname):: text = ('Web' :: character varying):: text
          ) THEN '121' :: character varying ELSE '111' :: character varying END WHEN (
            (cit80saleh.kaisha):: text = ('001' :: character varying):: text
          ) THEN '112' :: character varying ELSE '114' :: character varying END END, 
          cit80saleh.bmn_hyouji_cd, 
          cit80saleh.bmn_nms 
        UNION ALL 
        SELECT 
          cit85osalh.shukadate AS shukanengetu, 
          CASE WHEN (
            (cit85osalh.torikeikbn):: text = ('02' :: character varying):: text
          ) THEN CASE WHEN (
            (cit85osalh.tokuicode):: text = ('QVC0000001' :: character varying):: text
          ) THEN '321' :: character varying WHEN (
            (
              (cit85osalh.tokuicode):: text = ('CC00100001' :: character varying):: text
            ) 
            OR (
              (cit85osalh.tokuicode):: text = ('CC00100000' :: character varying):: text
            )
          ) THEN '111' :: character varying ELSE CASE WHEN (
            (
              cit85osalh.fskbn = (
                (1):: numeric
              ):: numeric(18, 0)
            ) 
            OR (
              cit85osalh.fskbn = (
                (0):: numeric
              ):: numeric(18, 0)
            )
          ) THEN '511' :: character varying ELSE CASE WHEN (
            (cit85osalh.shokuikibunrui):: text = ('1' :: character varying):: text
          ) THEN '312' :: character varying WHEN (
            (cit85osalh.shokuikibunrui):: text = ('2' :: character varying):: text
          ) THEN '313' :: character varying WHEN (
            (cit85osalh.shokuikibunrui):: text = ('3' :: character varying):: text
          ) THEN '314' :: character varying ELSE CASE WHEN (
            "substring"(
              (cit85osalh.tokuicode):: text, 
              2, 
              9
            ) <= ('000499999' :: character varying):: text
          ) THEN '311' :: character varying WHEN (
            (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) >= ('000600000' :: character varying):: text
            ) 
            AND (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) <= ('000799999' :: character varying):: text
            )
          ) THEN '412' :: character varying WHEN (
            (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) >= ('000500001' :: character varying):: text
            ) 
            AND (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) <= ('000599999' :: character varying):: text
            )
          ) THEN '312' :: character varying WHEN (
            (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) >= ('000800001' :: character varying):: text
            ) 
            AND (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) <= ('000899999' :: character varying):: text
            )
          ) THEN '311' :: character varying WHEN (
            "substring"(
              (cit85osalh.tokuicode):: text, 
              2, 
              9
            ) >= ('000900000' :: character varying):: text
          ) THEN CASE WHEN (
            (cit85osalh.tokuicode):: text like ('000099%' :: character varying):: text
          ) THEN '411' :: character varying WHEN (
            (cit85osalh.tokuicode):: text like ('VIP%' :: character varying):: text
          ) THEN '113' :: character varying WHEN (
            (cit85osalh.tokuicode):: text like ('CINEXT%' :: character varying):: text
          ) THEN '111' :: character varying ELSE '412' :: character varying END ELSE '' :: character varying END END END END ELSE CASE WHEN (
            (cit85osalh.torikeikbn):: text = ('03' :: character varying):: text
          ) THEN '211' :: character varying WHEN (
            (cit85osalh.torikeikbn):: text = ('04' :: character varying):: text
          ) THEN '212' :: character varying WHEN (
            (cit85osalh.torikeikbn):: text = ('05' :: character varying):: text
          ) THEN '213' :: character varying WHEN (
            (cit85osalh.torikeikbn):: text = ('06' :: character varying):: text
          ) THEN '214' :: character varying ELSE NULL :: character varying END END AS channel, 
          cit85osalh.tokuicode, 
          CASE WHEN (
            cit85osalh.kakokbn = (
              (1):: numeric
            ):: numeric(18, 0)
          ) THEN sum(
            (
              cit85osalh.sogokei - cit85osalh.tax
            )
          ) ELSE sum(cit85osalh.nukikingaku) END AS pointkomikingaku, 
          0 AS riyopoint, 
          cit85osalh.bmn_hyouji_cd, 
          cit85osalh.bmn_nms 
        FROM 
          cit85osalh 
        GROUP BY 
          cit85osalh.shukadate, 
          CASE WHEN (
            (cit85osalh.torikeikbn):: text = ('02' :: character varying):: text
          ) THEN CASE WHEN (
            (cit85osalh.tokuicode):: text = ('QVC0000001' :: character varying):: text
          ) THEN '321' :: character varying WHEN (
            (
              (cit85osalh.tokuicode):: text = ('CC00100001' :: character varying):: text
            ) 
            OR (
              (cit85osalh.tokuicode):: text = ('CC00100000' :: character varying):: text
            )
          ) THEN '111' :: character varying ELSE CASE WHEN (
            (
              cit85osalh.fskbn = (
                (1):: numeric
              ):: numeric(18, 0)
            ) 
            OR (
              cit85osalh.fskbn = (
                (0):: numeric
              ):: numeric(18, 0)
            )
          ) THEN '511' :: character varying ELSE CASE WHEN (
            (cit85osalh.shokuikibunrui):: text = ('1' :: character varying):: text
          ) THEN '312' :: character varying WHEN (
            (cit85osalh.shokuikibunrui):: text = ('2' :: character varying):: text
          ) THEN '313' :: character varying WHEN (
            (cit85osalh.shokuikibunrui):: text = ('3' :: character varying):: text
          ) THEN '314' :: character varying ELSE CASE WHEN (
            "substring"(
              (cit85osalh.tokuicode):: text, 
              2, 
              9
            ) <= ('000499999' :: character varying):: text
          ) THEN '311' :: character varying WHEN (
            (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) >= ('000600000' :: character varying):: text
            ) 
            AND (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) <= ('000799999' :: character varying):: text
            )
          ) THEN '412' :: character varying WHEN (
            (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) >= ('000500001' :: character varying):: text
            ) 
            AND (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) <= ('000599999' :: character varying):: text
            )
          ) THEN '312' :: character varying WHEN (
            (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) >= ('000800001' :: character varying):: text
            ) 
            AND (
              "substring"(
                (cit85osalh.tokuicode):: text, 
                2, 
                9
              ) <= ('000899999' :: character varying):: text
            )
          ) THEN '311' :: character varying WHEN (
            "substring"(
              (cit85osalh.tokuicode):: text, 
              2, 
              9
            ) >= ('000900000' :: character varying):: text
          ) THEN CASE WHEN (
            (cit85osalh.tokuicode):: text like ('000099%' :: character varying):: text
          ) THEN '411' :: character varying WHEN (
            (cit85osalh.tokuicode):: text like ('VIP%' :: character varying):: text
          ) THEN '113' :: character varying WHEN (
            (cit85osalh.tokuicode):: text like ('CINEXT%' :: character varying):: text
          ) THEN '111' :: character varying ELSE '412' :: character varying END ELSE '' :: character varying END END END END ELSE CASE WHEN (
            (cit85osalh.torikeikbn):: text = ('03' :: character varying):: text
          ) THEN '211' :: character varying WHEN (
            (cit85osalh.torikeikbn):: text = ('04' :: character varying):: text
          ) THEN '212' :: character varying WHEN (
            (cit85osalh.torikeikbn):: text = ('05' :: character varying):: text
          ) THEN '213' :: character varying WHEN (
            (cit85osalh.torikeikbn):: text = ('06' :: character varying):: text
          ) THEN '214' :: character varying ELSE NULL :: character varying END END, 
          cit85osalh.tokuicode, 
          cit85osalh.kakokbn, 
          cit85osalh.bmn_hyouji_cd, 
          cit85osalh.bmn_nms
      ) 
      UNION ALL 
      SELECT 
        cit85osalh_kaigai.shukadate AS shukanengetu, 
        CASE WHEN (
          (cit85osalh_kaigai.tokuicode):: text like ('000099%' :: character varying):: text
        ) THEN '411' :: character varying WHEN (
          (cit85osalh_kaigai.tokuicode):: text like ('000091%' :: character varying):: text
        ) THEN '413' :: character varying ELSE '412' :: character varying END AS channel, 
        cit85osalh_kaigai.tokuicode, 
        sum(cit85osalh_kaigai.nukikingaku) AS pointkomikingaku, 
        0 AS riyopoint, 
        cit85osalh_kaigai.bmn_hyouji_cd, 
        cit85osalh_kaigai.bmn_nms 
      FROM 
        cit85osalh_kaigai 
      WHERE 
        (
          cit85osalh_kaigai.kakokbn = (
            (0):: numeric
          ):: numeric(18, 0)
        ) 
      GROUP BY 
        cit85osalh_kaigai.shukadate, 
        CASE WHEN (
          (cit85osalh_kaigai.tokuicode):: text like ('000099%' :: character varying):: text
        ) THEN '411' :: character varying WHEN (
          (cit85osalh_kaigai.tokuicode):: text like ('000091%' :: character varying):: text
        ) THEN '413' :: character varying ELSE '412' :: character varying END, 
        cit85osalh_kaigai.tokuicode, 
        cit85osalh_kaigai.bmn_hyouji_cd, 
        cit85osalh_kaigai.bmn_nms
    ) t 
    LEFT JOIN cim02tokui cim02 ON (
      (
        (t.tokuicode):: text = (cim02.tokuicode):: text
      )
    )
  )
)
select * from final