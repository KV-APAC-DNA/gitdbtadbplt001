with tbusrpram as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBUSRPRAM
),
c_tbecregularcoursemst as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECREGULARCOURSEMST
),
c_tbecregularmeisai as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECREGULARMEISAI
),
c_tbecregularcontract as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECREGULARCONTRACT
),
tbecitem as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECITEM
),
final as (
SELECT 
  0 AS sort_key, 
  'USERID' AS usrid, 
  'EMAIL' AS email, 
  'WEBID' AS webid, 
  'LAST_NAME' AS last_name, 
  'FIRST_NAME' AS first_name, 
  'OTODOKE_DAY' AS otodoke_day, 
  'OTODOKE_TIME' AS otodoke_time, 
  'ITEMS' AS items, 
  'PAYMENT' AS payment, 
  'OTODOKE_PLACE' AS otodoke_place, 
  'STATUS' AS status, 
  'HENKOULIMIT' AS henkoulimit, 
  'OTDK_Zip' AS adrs1, 
  'OTDK_Pref' AS adrs2, 
  'OTDK_City' AS adrs3, 
  'OTDK_Addr' AS adrs4, 
  'OTDK_Tatemono' AS adrs5, 
  'OTDK_Sei' AS otodokesei, 
  'OTDK_Mei' AS otodokemei, 
  'EXEC_DATE' AS exec_time 
UNION ALL 
SELECT 
  1 AS sort_key, 
  (up.diusrid):: character varying(18) AS usrid, 
  CASE WHEN (
    (
      (up.dsdat5):: text = ('PC' :: character varying):: text
    ) 
    OR (
      (up.dsdat5):: text = ('なし' :: character varying):: text
    )
  ) THEN CASE WHEN (
    (up.dsmail):: text not like (
      'usr%@dummy.emplex.ne.jp' :: character varying
    ):: text
  ) THEN up.dsmail ELSE up.dsdat1 END WHEN (
    (up.dsdat5):: text = ('携帯' :: character varying):: text
  ) THEN CASE WHEN (up.dsdat1 IS NOT NULL) THEN up.dsdat1 ELSE up.dsmail END ELSE NULL :: character varying END AS email, 
  up.dsctrlcd AS webid, 
  up.dsname AS last_name, 
  up.dsname2 AS first_name, 
  CASE WHEN (
    DATE_PART(
      dow, 
      rm.c_dstodokedate
    ) = (0):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            rm.c_dstodokedate, 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('日' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
      rm.c_dstodokedate
    ) = (1):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            rm.c_dstodokedate, 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('月' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
      rm.c_dstodokedate
    ) = (2):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            rm.c_dstodokedate, 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('火' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
      rm.c_dstodokedate
    ) = (3):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            rm.c_dstodokedate, 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('水' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
      rm.c_dstodokedate
    ) = (4):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            rm.c_dstodokedate, 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('木' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
      rm.c_dstodokedate
    ) = (5):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            rm.c_dstodokedate, 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('金' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
      rm.c_dstodokedate
    ) = (6):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            rm.c_dstodokedate, 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('土' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying ELSE NULL :: character varying END AS otodoke_day, 
  CASE WHEN (
    (rm.c_dsdeliverytime):: text = ('0' :: character varying):: text
  ) THEN '指定なし' :: character varying WHEN (
    (rm.c_dsdeliverytime):: text = ('1' :: character varying):: text
  ) THEN '午前中' :: character varying WHEN (
    (rm.c_dsdeliverytime):: text = ('2' :: character varying):: text
  ) THEN '12:00～14:00' :: character varying WHEN (
    (rm.c_dsdeliverytime):: text = ('3' :: character varying):: text
  ) THEN '14:00～16:00' :: character varying WHEN (
    (rm.c_dsdeliverytime):: text = ('4' :: character varying):: text
  ) THEN '16:00～18:00' :: character varying WHEN (
    (rm.c_dsdeliverytime):: text = ('5' :: character varying):: text
  ) THEN '18:00～20:00' :: character varying WHEN (
    (rm.c_dsdeliverytime):: text = ('6' :: character varying):: text
  ) THEN '19:00～21:00' :: character varying WHEN (
    (rm.c_dsdeliverytime):: text = ('7' :: character varying):: text
  ) THEN '20:00～21:00' :: character varying ELSE '' :: character varying END AS otodoke_time, 
  (
    (
      (
        (
          (tbitm.dsitemname):: text || ('(' :: character varying):: text
        ) || (rcm.c_dsregularcoursenameryaku):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying AS items, 
  CASE WHEN (
    (rm.dskessaihoho):: text = ('1' :: character varying):: text
  ) THEN 'コンビニ後払い' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('2' :: character varying):: text
  ) THEN 'クレジットカード' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('3' :: character varying):: text
  ) THEN '代金引換' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('4' :: character varying):: text
  ) THEN '楽天ペイ' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('5' :: character varying):: text
  ) THEN 'NP後払い' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('6' :: character varying):: text
  ) THEN 'auかんたん決済' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('7' :: character varying):: text
  ) THEN '店舗決済' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('8' :: character varying):: text
  ) THEN '無料決済' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('9' :: character varying):: text
  ) THEN 'その他' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('10' :: character varying):: text
  ) THEN '銀行振込' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('11' :: character varying):: text
  ) THEN '旧後払い' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('12' :: character varying):: text
  ) THEN 'd払い' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('13' :: character varying):: text
  ) THEN 'SoftBankまとめて支払' :: character varying WHEN (
    (rm.dskessaihoho):: text = ('14' :: character varying):: text
  ) THEN 'GMO後払い' :: character varying ELSE '' :: character varying END AS payment, 
  '登録なし' AS otodoke_place, 
  'お届け予定' AS status, 
  CASE WHEN (
    DATE_PART(
      dow, 
      dateadd(day,-6,rm.c_dstodokedate)
    ) = (0):: double precision
  ) THEN (
    (
      (
        (
          to_char(
           dateadd(day,-6,rm.c_dstodokedate), 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('日' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
      dateadd(day,-6,rm.c_dstodokedate)
    ) = (1):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            dateadd(day,-6,rm.c_dstodokedate), 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('月' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
     dateadd(day,-6,rm.c_dstodokedate)
    ) = (2):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            dateadd(day,-6,rm.c_dstodokedate), 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('火' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
      (
        dateadd(day,-6,rm.c_dstodokedate)
      )
    ) = (3):: double precision
  ) THEN (
    (
      (
        (
          to_char(
           dateadd(day,-6,rm.c_dstodokedate), 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('水' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
     dateadd(day,-6,rm.c_dstodokedate)
    ) = (4):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            dateadd(day,-6,rm.c_dstodokedate), 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('木' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
      dateadd(day,-6,rm.c_dstodokedate)
    ) = (5):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            dateadd(day,-6,rm.c_dstodokedate), 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('金' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying WHEN (
    DATE_PART(
      dow, 
     dateadd(day,-6,rm.c_dstodokedate)
    ) = (6):: double precision
  ) THEN (
    (
      (
        (
          to_char(
            dateadd(day,-6,rm.c_dstodokedate), 
            ('YYYY/MM/DD' :: character varying):: text
          ) || ('(' :: character varying):: text
        ) || ('土' :: character varying):: text
      ) || (')' :: character varying):: text
    )
  ):: character varying ELSE NULL :: character varying END AS henkoulimit, 
  rm.dstodokezip AS adrs1, 
  rm.dstodokepref AS adrs2, 
  rm.dstodokecity AS adrs3, 
  rm.dstodokeaddr AS adrs4, 
  rm.dstodoketatemono AS adrs5, 
  rm.dstodokesei AS otodokesei, 
  rm.dstodokemei AS otodokemei, 
  (
    to_char(
      sysdate(), 
      ('YYYY/MM/DD' :: character varying):: text
    )
  ):: character varying AS exec_time 
FROM 
  (
    (
      (
        (
          c_tbecregularcontract rc 
          JOIN c_tbecregularmeisai rm ON (
            (
              (
                (
                  (
                    (
                      rc.c_diregularcontractid = rm.c_diregularcontractid
                    ) 
                    AND (
                      to_date(rm.c_dstodokedate) >= dateadd(day,16,to_date(
                        (
                          convert_timezone(
                            'Asia/Tokyo', 
                            (sysdate() :: character varying):: timestamp without time zone
                          ) )))
                    )
                  ) 
                  AND (
                    to_date(rm.c_dstodokedate) < dateadd(day,23,to_date(
                        (
                          convert_timezone(
                            'Asia/Tokyo', 
                            (sysdate() :: character varying):: timestamp without time zone
                          ) )))
                  )
                ) 
                AND (
                  (rm.c_dicancelflg):: text = (
                    (0):: character varying
                  ):: text
                )
              ) 
              AND (
                (rm.dielimflg):: text = (
                  (0):: character varying
                ):: text
              )
            )
          )
        ) 
        JOIN c_tbecregularcoursemst rcm ON (
          (
            rm.c_diregularcourseid = rcm.c_diregularcourseid
          )
        )
      ) 
      JOIN tbusrpram up ON (
        (
          (
            (
              (
                (rc.c_diusrid = up.diusrid) 
                AND (
                  (
                    (up.dsmail):: text not like (
                      'usr%@dummy.emplex.ne.jp' :: character varying
                    ):: text
                  ) 
                  OR (up.dsdat1 IS NOT NULL)
                )
              ) 
              AND (
                (up.disecessionflg):: text = (
                  (0):: character varying
                ):: text
              )
            ) 
            AND (
              (up.dielimflg):: text = (
                (0):: character varying
              ):: text
            )
          ) 
          AND (
            (up.dsdat93):: text <> (
              'テストユーザ' :: character varying
            ):: text
          )
        )
      )
    ) 
    JOIN tbecitem tbitm ON (
      (
        (
          (tbitm.dsitemid):: text = (rm.dsitemid):: text
        ) 
        AND (
          (tbitm.dielimflg):: text = (
            (0):: character varying
          ):: text
        )
      )
    )
  ) 
WHERE 
  (
    (
      (
        to_date(rc.c_diregularcontractdate) < dateadd(day,-3,to_date(
                        (
                          convert_timezone(
                            'Asia/Tokyo', 
                            (sysdate() :: character varying):: timestamp without time zone
                          ) )))
      ) 
      AND (
        (rc.dielimflg):: text = (
          (0):: character varying
        ):: text
      )
    ) 
    AND (
      (rm.c_dsordercreatekbn):: text <> (
        (3):: character varying
      ):: text
    )
  ) 
ORDER BY 
  1
)
select * from final