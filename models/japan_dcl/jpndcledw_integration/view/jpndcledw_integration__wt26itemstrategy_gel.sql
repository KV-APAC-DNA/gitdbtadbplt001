with tm14shkos as (
select * from {{ source('jpdcledw_integration', 'tm14shkos') }}
),
tm67juch_nm as (
select * from {{ ref('jpndcledw_integration__tm67juch_nm') }}
),
tm39item_strategy as (
select * from {{ ref('jpndcledw_integration__tm39item_strategy') }}
),
salem_itemstrategy as (
select * from {{ ref('jpndcledw_integration__salem_itemstrategy') }}
),
saleh_itemstrategy as (
select * from {{ ref('jpndcledw_integration__saleh_itemstrategy') }}
),
final as ((
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
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            SELECT 
                                                                                              (
                                                                                                to_char(
                                                                                                  add_months(
                                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                                    (
                                                                                                      - (24):: bigint
                                                                                                    )
                                                                                                  ), 
                                                                                                  ('yyyymm' :: character varying):: text
                                                                                                )
                                                                                              ):: integer AS nengetu, 
                                                                                              count(
                                                                                                DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                                              ) AS ninzu, 
                                                                                              view_tuhan_bunkai_shohin.channel, 
                                                                                              view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                              view_tuhan_bunkai_shohin.itembunrui, 
                                                                                              view_tuhan_bunkai_shohin.juchkbn, 
                                                                                              (
                                                                                                view_tuhan_bunkai_shohin.juchkbncname
                                                                                              ):: character varying AS juchkbncname, 
                                                                                              view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                              (
                                                                                                view_tuhan_bunkai_shohin.henreasonname
                                                                                              ):: character varying AS henreasonname 
                                                                                            FROM 
                                                                                              (
                                                                                                SELECT 
                                                                                                  "substring"(
                                                                                                    (
                                                                                                      (h.shukadate):: character varying
                                                                                                    ):: text, 
                                                                                                    0, 
                                                                                                    6
                                                                                                  ) AS shukadate, 
                                                                                                  h.kokyano, 
                                                                                                  CASE WHEN (
                                                                                                    (
                                                                                                      h.kakokbn = (
                                                                                                        (1):: numeric
                                                                                                      ):: numeric(18, 0)
                                                                                                    ) 
                                                                                                    OR (
                                                                                                      (h.kakokbn IS NULL) 
                                                                                                      AND ('1' IS NULL)
                                                                                                    )
                                                                                                  ) THEN CASE WHEN (
                                                                                                    (h.kaisha):: text = ('000' :: character varying):: text
                                                                                                  ) THEN '01:通販' :: character varying WHEN (
                                                                                                    (h.kaisha):: text = ('001' :: character varying):: text
                                                                                                  ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                                    h.smkeiroid = (
                                                                                                      (6):: numeric
                                                                                                    ):: numeric(18, 0)
                                                                                                  ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                                  h.daihanrobunname AS konyuchubuncode, 
                                                                                                  i.itembunrui, 
                                                                                                  h.juchkbn, 
                                                                                                  tm67.cname AS juchkbncname, 
                                                                                                  h.henreasoncode, 
                                                                                                  (
                                                                                                    (
                                                                                                      (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                                    ) || (h.henreasonname):: text
                                                                                                  ) AS henreasonname 
                                                                                                FROM 
                                                                                                  (
                                                                                                    (
                                                                                                      (
                                                                                                        (
                                                                                                          saleh_itemstrategy h 
                                                                                                          JOIN salem_itemstrategy m ON (
                                                                                                            (
                                                                                                              (h.saleno):: text = (m.saleno):: text
                                                                                                            )
                                                                                                          )
                                                                                                        ) 
                                                                                                        JOIN tm14shkos "k" ON (
                                                                                                          (
                                                                                                            (m.itemcode):: text = ("k".itemcode):: text
                                                                                                          )
                                                                                                        )
                                                                                                      ) 
                                                                                                      JOIN tm39item_strategy i ON (
                                                                                                        (
                                                                                                          ("k".kosecode):: text = (i.itemcode):: text
                                                                                                        )
                                                                                                      )
                                                                                                    ) 
                                                                                                    LEFT JOIN tm67juch_nm tm67 ON (
                                                                                                      (
                                                                                                        (h.juchkbn):: text = (tm67.code):: text
                                                                                                      )
                                                                                                    )
                                                                                                  ) 
                                                                                                WHERE 
                                                                                                  (
                                                                                                    (
                                                                                                      (
                                                                                                        (
                                                                                                          (
                                                                                                            (
                                                                                                              (
                                                                                                                h.shukadate >= (
                                                                                                                  (
                                                                                                                    (
                                                                                                                      (
                                                                                                                        to_char(
                                                                                                                          add_months(
                                                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                                                            (
                                                                                                                              - (36):: bigint
                                                                                                                            )
                                                                                                                          ), 
                                                                                                                          ('yyyymm' :: character varying):: text
                                                                                                                        ) || ('01' :: character varying):: text
                                                                                                                      )
                                                                                                                    ):: integer
                                                                                                                  ):: numeric
                                                                                                                ):: numeric(18, 0)
                                                                                                              ) 
                                                                                                              AND (
                                                                                                                h.shukadate <= (
                                                                                                                  (
                                                                                                                    (
                                                                                                                      to_char(
                                                                                                                        (
                                                                                                                          last_day(
                                                                                                                            add_months(
                                                                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                                                                              (
                                                                                                                                - (1):: bigint
                                                                                                                              )
                                                                                                                            )
                                                                                                                          )
                                                                                                                        ):: timestamp without time zone, 
                                                                                                                        ('yyyymmdd' :: character varying):: text
                                                                                                                      )
                                                                                                                    ):: integer
                                                                                                                  ):: numeric
                                                                                                                ):: numeric(18, 0)
                                                                                                              )
                                                                                                            ) 
                                                                                                            AND (
                                                                                                              h.cancelflg = (
                                                                                                                (0):: numeric
                                                                                                              ):: numeric(18, 0)
                                                                                                            )
                                                                                                          ) 
                                                                                                          AND (
                                                                                                            (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                                          )
                                                                                                        ) 
                                                                                                        AND (
                                                                                                          (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                                        )
                                                                                                      ) 
                                                                                                      AND (
                                                                                                        (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                                      )
                                                                                                    ) 
                                                                                                    AND (
                                                                                                      (
                                                                                                        (
                                                                                                          (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                                        ) 
                                                                                                        OR (
                                                                                                          (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                                        )
                                                                                                      ) 
                                                                                                      OR (
                                                                                                        (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                                      )
                                                                                                    )
                                                                                                  )
                                                                                              ) view_tuhan_bunkai_shohin 
                                                                                            WHERE 
                                                                                              (
                                                                                                (
                                                                                                  view_tuhan_bunkai_shohin.shukadate >= (
                                                                                                    (
                                                                                                      (
                                                                                                        to_char(
                                                                                                          add_months(
                                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                                            (
                                                                                                              - (35):: bigint
                                                                                                            )
                                                                                                          ), 
                                                                                                          ('yyyymm' :: character varying):: text
                                                                                                        )
                                                                                                      ):: integer
                                                                                                    ):: character varying
                                                                                                  ):: text
                                                                                                ) 
                                                                                                AND (
                                                                                                  view_tuhan_bunkai_shohin.shukadate <= (
                                                                                                    (
                                                                                                      (
                                                                                                        to_char(
                                                                                                          (
                                                                                                            last_day(
                                                                                                              add_months(
                                                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                                                (
                                                                                                                  - (24):: bigint
                                                                                                                )
                                                                                                              )
                                                                                                            )
                                                                                                          ):: timestamp without time zone, 
                                                                                                          ('yyyymm' :: character varying):: text
                                                                                                        )
                                                                                                      ):: integer
                                                                                                    ):: character varying
                                                                                                  ):: text
                                                                                                )
                                                                                              ) 
                                                                                            GROUP BY 
                                                                                              view_tuhan_bunkai_shohin.channel, 
                                                                                              view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                              view_tuhan_bunkai_shohin.itembunrui, 
                                                                                              view_tuhan_bunkai_shohin.juchkbn, 
                                                                                              view_tuhan_bunkai_shohin.juchkbncname, 
                                                                                              view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                              view_tuhan_bunkai_shohin.henreasonname 
                                                                                            UNION 
                                                                                            SELECT 
                                                                                              (
                                                                                                to_char(
                                                                                                  add_months(
                                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                                    (
                                                                                                      - (23):: bigint
                                                                                                    )
                                                                                                  ), 
                                                                                                  ('yyyymm' :: character varying):: text
                                                                                                )
                                                                                              ):: integer AS nengetu, 
                                                                                              count(
                                                                                                DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                                              ) AS ninzu, 
                                                                                              view_tuhan_bunkai_shohin.channel, 
                                                                                              view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                              view_tuhan_bunkai_shohin.itembunrui, 
                                                                                              view_tuhan_bunkai_shohin.juchkbn, 
                                                                                              (
                                                                                                view_tuhan_bunkai_shohin.juchkbncname
                                                                                              ):: character varying AS juchkbncname, 
                                                                                              view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                              (
                                                                                                view_tuhan_bunkai_shohin.henreasonname
                                                                                              ):: character varying AS henreasonname 
                                                                                            FROM 
                                                                                              (
                                                                                                SELECT 
                                                                                                  "substring"(
                                                                                                    (
                                                                                                      (h.shukadate):: character varying
                                                                                                    ):: text, 
                                                                                                    0, 
                                                                                                    6
                                                                                                  ) AS shukadate, 
                                                                                                  h.kokyano, 
                                                                                                  CASE WHEN (
                                                                                                    (
                                                                                                      h.kakokbn = (
                                                                                                        (1):: numeric
                                                                                                      ):: numeric(18, 0)
                                                                                                    ) 
                                                                                                    OR (
                                                                                                      (h.kakokbn IS NULL) 
                                                                                                      AND ('1' IS NULL)
                                                                                                    )
                                                                                                  ) THEN CASE WHEN (
                                                                                                    (h.kaisha):: text = ('000' :: character varying):: text
                                                                                                  ) THEN '01:通販' :: character varying WHEN (
                                                                                                    (h.kaisha):: text = ('001' :: character varying):: text
                                                                                                  ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                                    h.smkeiroid = (
                                                                                                      (6):: numeric
                                                                                                    ):: numeric(18, 0)
                                                                                                  ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                                  h.daihanrobunname AS konyuchubuncode, 
                                                                                                  i.itembunrui, 
                                                                                                  h.juchkbn, 
                                                                                                  tm67.cname AS juchkbncname, 
                                                                                                  h.henreasoncode, 
                                                                                                  (
                                                                                                    (
                                                                                                      (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                                    ) || (h.henreasonname):: text
                                                                                                  ) AS henreasonname 
                                                                                                FROM 
                                                                                                  (
                                                                                                    (
                                                                                                      (
                                                                                                        (
                                                                                                          saleh_itemstrategy h 
                                                                                                          JOIN salem_itemstrategy m ON (
                                                                                                            (
                                                                                                              (h.saleno):: text = (m.saleno):: text
                                                                                                            )
                                                                                                          )
                                                                                                        ) 
                                                                                                        JOIN tm14shkos "k" ON (
                                                                                                          (
                                                                                                            (m.itemcode):: text = ("k".itemcode):: text
                                                                                                          )
                                                                                                        )
                                                                                                      ) 
                                                                                                      JOIN tm39item_strategy i ON (
                                                                                                        (
                                                                                                          ("k".kosecode):: text = (i.itemcode):: text
                                                                                                        )
                                                                                                      )
                                                                                                    ) 
                                                                                                    LEFT JOIN tm67juch_nm tm67 ON (
                                                                                                      (
                                                                                                        (h.juchkbn):: text = (tm67.code):: text
                                                                                                      )
                                                                                                    )
                                                                                                  ) 
                                                                                                WHERE 
                                                                                                  (
                                                                                                    (
                                                                                                      (
                                                                                                        (
                                                                                                          (
                                                                                                            (
                                                                                                              (
                                                                                                                h.shukadate >= (
                                                                                                                  (
                                                                                                                    (
                                                                                                                      (
                                                                                                                        to_char(
                                                                                                                          add_months(
                                                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                                                            (
                                                                                                                              - (36):: bigint
                                                                                                                            )
                                                                                                                          ), 
                                                                                                                          ('yyyymm' :: character varying):: text
                                                                                                                        ) || ('01' :: character varying):: text
                                                                                                                      )
                                                                                                                    ):: integer
                                                                                                                  ):: numeric
                                                                                                                ):: numeric(18, 0)
                                                                                                              ) 
                                                                                                              AND (
                                                                                                                h.shukadate <= (
                                                                                                                  (
                                                                                                                    (
                                                                                                                      to_char(
                                                                                                                        (
                                                                                                                          last_day(
                                                                                                                            add_months(
                                                                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                                                                              (
                                                                                                                                - (1):: bigint
                                                                                                                              )
                                                                                                                            )
                                                                                                                          )
                                                                                                                        ):: timestamp without time zone, 
                                                                                                                        ('yyyymmdd' :: character varying):: text
                                                                                                                      )
                                                                                                                    ):: integer
                                                                                                                  ):: numeric
                                                                                                                ):: numeric(18, 0)
                                                                                                              )
                                                                                                            ) 
                                                                                                            AND (
                                                                                                              h.cancelflg = (
                                                                                                                (0):: numeric
                                                                                                              ):: numeric(18, 0)
                                                                                                            )
                                                                                                          ) 
                                                                                                          AND (
                                                                                                            (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                                          )
                                                                                                        ) 
                                                                                                        AND (
                                                                                                          (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                                        )
                                                                                                      ) 
                                                                                                      AND (
                                                                                                        (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                                      )
                                                                                                    ) 
                                                                                                    AND (
                                                                                                      (
                                                                                                        (
                                                                                                          (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                                        ) 
                                                                                                        OR (
                                                                                                          (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                                        )
                                                                                                      ) 
                                                                                                      OR (
                                                                                                        (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                                      )
                                                                                                    )
                                                                                                  )
                                                                                              ) view_tuhan_bunkai_shohin 
                                                                                            WHERE 
                                                                                              (
                                                                                                (
                                                                                                  view_tuhan_bunkai_shohin.shukadate >= (
                                                                                                    (
                                                                                                      (
                                                                                                        to_char(
                                                                                                          add_months(
                                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                                            (
                                                                                                              - (34):: bigint
                                                                                                            )
                                                                                                          ), 
                                                                                                          ('yyyymm' :: character varying):: text
                                                                                                        )
                                                                                                      ):: integer
                                                                                                    ):: character varying
                                                                                                  ):: text
                                                                                                ) 
                                                                                                AND (
                                                                                                  view_tuhan_bunkai_shohin.shukadate <= (
                                                                                                    (
                                                                                                      (
                                                                                                        to_char(
                                                                                                          (
                                                                                                            last_day(
                                                                                                              add_months(
                                                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                                                (
                                                                                                                  - (23):: bigint
                                                                                                                )
                                                                                                              )
                                                                                                            )
                                                                                                          ):: timestamp without time zone, 
                                                                                                          ('yyyymm' :: character varying):: text
                                                                                                        )
                                                                                                      ):: integer
                                                                                                    ):: character varying
                                                                                                  ):: text
                                                                                                )
                                                                                              ) 
                                                                                            GROUP BY 
                                                                                              view_tuhan_bunkai_shohin.channel, 
                                                                                              view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                              view_tuhan_bunkai_shohin.itembunrui, 
                                                                                              view_tuhan_bunkai_shohin.juchkbn, 
                                                                                              view_tuhan_bunkai_shohin.juchkbncname, 
                                                                                              view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                              view_tuhan_bunkai_shohin.henreasonname
                                                                                          ) 
                                                                                          UNION 
                                                                                          SELECT 
                                                                                            (
                                                                                              to_char(
                                                                                                add_months(
                                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                                  (
                                                                                                    - (22):: bigint
                                                                                                  )
                                                                                                ), 
                                                                                                ('yyyymm' :: character varying):: text
                                                                                              )
                                                                                            ):: integer AS nengetu, 
                                                                                            count(
                                                                                              DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                                            ) AS ninzu, 
                                                                                            view_tuhan_bunkai_shohin.channel, 
                                                                                            view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                            view_tuhan_bunkai_shohin.itembunrui, 
                                                                                            view_tuhan_bunkai_shohin.juchkbn, 
                                                                                            (
                                                                                              view_tuhan_bunkai_shohin.juchkbncname
                                                                                            ):: character varying AS juchkbncname, 
                                                                                            view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                            (
                                                                                              view_tuhan_bunkai_shohin.henreasonname
                                                                                            ):: character varying AS henreasonname 
                                                                                          FROM 
                                                                                            (
                                                                                              SELECT 
                                                                                                "substring"(
                                                                                                  (
                                                                                                    (h.shukadate):: character varying
                                                                                                  ):: text, 
                                                                                                  0, 
                                                                                                  6
                                                                                                ) AS shukadate, 
                                                                                                h.kokyano, 
                                                                                                CASE WHEN (
                                                                                                  (
                                                                                                    h.kakokbn = (
                                                                                                      (1):: numeric
                                                                                                    ):: numeric(18, 0)
                                                                                                  ) 
                                                                                                  OR (
                                                                                                    (h.kakokbn IS NULL) 
                                                                                                    AND ('1' IS NULL)
                                                                                                  )
                                                                                                ) THEN CASE WHEN (
                                                                                                  (h.kaisha):: text = ('000' :: character varying):: text
                                                                                                ) THEN '01:通販' :: character varying WHEN (
                                                                                                  (h.kaisha):: text = ('001' :: character varying):: text
                                                                                                ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                                  h.smkeiroid = (
                                                                                                    (6):: numeric
                                                                                                  ):: numeric(18, 0)
                                                                                                ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                                h.daihanrobunname AS konyuchubuncode, 
                                                                                                i.itembunrui, 
                                                                                                h.juchkbn, 
                                                                                                tm67.cname AS juchkbncname, 
                                                                                                h.henreasoncode, 
                                                                                                (
                                                                                                  (
                                                                                                    (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                                  ) || (h.henreasonname):: text
                                                                                                ) AS henreasonname 
                                                                                              FROM 
                                                                                                (
                                                                                                  (
                                                                                                    (
                                                                                                      (
                                                                                                        saleh_itemstrategy h 
                                                                                                        JOIN salem_itemstrategy m ON (
                                                                                                          (
                                                                                                            (h.saleno):: text = (m.saleno):: text
                                                                                                          )
                                                                                                        )
                                                                                                      ) 
                                                                                                      JOIN tm14shkos "k" ON (
                                                                                                        (
                                                                                                          (m.itemcode):: text = ("k".itemcode):: text
                                                                                                        )
                                                                                                      )
                                                                                                    ) 
                                                                                                    JOIN tm39item_strategy i ON (
                                                                                                      (
                                                                                                        ("k".kosecode):: text = (i.itemcode):: text
                                                                                                      )
                                                                                                    )
                                                                                                  ) 
                                                                                                  LEFT JOIN tm67juch_nm tm67 ON (
                                                                                                    (
                                                                                                      (h.juchkbn):: text = (tm67.code):: text
                                                                                                    )
                                                                                                  )
                                                                                                ) 
                                                                                              WHERE 
                                                                                                (
                                                                                                  (
                                                                                                    (
                                                                                                      (
                                                                                                        (
                                                                                                          (
                                                                                                            (
                                                                                                              h.shukadate >= (
                                                                                                                (
                                                                                                                  (
                                                                                                                    (
                                                                                                                      to_char(
                                                                                                                        add_months(
                                                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                                                          (
                                                                                                                            - (36):: bigint
                                                                                                                          )
                                                                                                                        ), 
                                                                                                                        ('yyyymm' :: character varying):: text
                                                                                                                      ) || ('01' :: character varying):: text
                                                                                                                    )
                                                                                                                  ):: integer
                                                                                                                ):: numeric
                                                                                                              ):: numeric(18, 0)
                                                                                                            ) 
                                                                                                            AND (
                                                                                                              h.shukadate <= (
                                                                                                                (
                                                                                                                  (
                                                                                                                    to_char(
                                                                                                                      (
                                                                                                                        last_day(
                                                                                                                          add_months(
                                                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                                                            (
                                                                                                                              - (1):: bigint
                                                                                                                            )
                                                                                                                          )
                                                                                                                        )
                                                                                                                      ):: timestamp without time zone, 
                                                                                                                      ('yyyymmdd' :: character varying):: text
                                                                                                                    )
                                                                                                                  ):: integer
                                                                                                                ):: numeric
                                                                                                              ):: numeric(18, 0)
                                                                                                            )
                                                                                                          ) 
                                                                                                          AND (
                                                                                                            h.cancelflg = (
                                                                                                              (0):: numeric
                                                                                                            ):: numeric(18, 0)
                                                                                                          )
                                                                                                        ) 
                                                                                                        AND (
                                                                                                          (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                                        )
                                                                                                      ) 
                                                                                                      AND (
                                                                                                        (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                                      )
                                                                                                    ) 
                                                                                                    AND (
                                                                                                      (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                                    )
                                                                                                  ) 
                                                                                                  AND (
                                                                                                    (
                                                                                                      (
                                                                                                        (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                                      ) 
                                                                                                      OR (
                                                                                                        (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                                      )
                                                                                                    ) 
                                                                                                    OR (
                                                                                                      (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                                    )
                                                                                                  )
                                                                                                )
                                                                                            ) view_tuhan_bunkai_shohin 
                                                                                          WHERE 
                                                                                            (
                                                                                              (
                                                                                                view_tuhan_bunkai_shohin.shukadate >= (
                                                                                                  (
                                                                                                    (
                                                                                                      to_char(
                                                                                                        add_months(
                                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                                          (
                                                                                                            - (33):: bigint
                                                                                                          )
                                                                                                        ), 
                                                                                                        ('yyyymm' :: character varying):: text
                                                                                                      )
                                                                                                    ):: integer
                                                                                                  ):: character varying
                                                                                                ):: text
                                                                                              ) 
                                                                                              AND (
                                                                                                view_tuhan_bunkai_shohin.shukadate <= (
                                                                                                  (
                                                                                                    (
                                                                                                      to_char(
                                                                                                        (
                                                                                                          last_day(
                                                                                                            add_months(
                                                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                                                              (
                                                                                                                - (22):: bigint
                                                                                                              )
                                                                                                            )
                                                                                                          )
                                                                                                        ):: timestamp without time zone, 
                                                                                                        ('yyyymm' :: character varying):: text
                                                                                                      )
                                                                                                    ):: integer
                                                                                                  ):: character varying
                                                                                                ):: text
                                                                                              )
                                                                                            ) 
                                                                                          GROUP BY 
                                                                                            view_tuhan_bunkai_shohin.channel, 
                                                                                            view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                            view_tuhan_bunkai_shohin.itembunrui, 
                                                                                            view_tuhan_bunkai_shohin.juchkbn, 
                                                                                            view_tuhan_bunkai_shohin.juchkbncname, 
                                                                                            view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                            view_tuhan_bunkai_shohin.henreasonname
                                                                                        ) 
                                                                                        UNION 
                                                                                        SELECT 
                                                                                          (
                                                                                            to_char(
                                                                                              add_months(
                                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                                (
                                                                                                  - (21):: bigint
                                                                                                )
                                                                                              ), 
                                                                                              ('yyyymm' :: character varying):: text
                                                                                            )
                                                                                          ):: integer AS nengetu, 
                                                                                          count(
                                                                                            DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                                          ) AS ninzu, 
                                                                                          view_tuhan_bunkai_shohin.channel, 
                                                                                          view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                          view_tuhan_bunkai_shohin.itembunrui, 
                                                                                          view_tuhan_bunkai_shohin.juchkbn, 
                                                                                          (
                                                                                            view_tuhan_bunkai_shohin.juchkbncname
                                                                                          ):: character varying AS juchkbncname, 
                                                                                          view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                          (
                                                                                            view_tuhan_bunkai_shohin.henreasonname
                                                                                          ):: character varying AS henreasonname 
                                                                                        FROM 
                                                                                          (
                                                                                            SELECT 
                                                                                              "substring"(
                                                                                                (
                                                                                                  (h.shukadate):: character varying
                                                                                                ):: text, 
                                                                                                0, 
                                                                                                6
                                                                                              ) AS shukadate, 
                                                                                              h.kokyano, 
                                                                                              CASE WHEN (
                                                                                                (
                                                                                                  h.kakokbn = (
                                                                                                    (1):: numeric
                                                                                                  ):: numeric(18, 0)
                                                                                                ) 
                                                                                                OR (
                                                                                                  (h.kakokbn IS NULL) 
                                                                                                  AND ('1' IS NULL)
                                                                                                )
                                                                                              ) THEN CASE WHEN (
                                                                                                (h.kaisha):: text = ('000' :: character varying):: text
                                                                                              ) THEN '01:通販' :: character varying WHEN (
                                                                                                (h.kaisha):: text = ('001' :: character varying):: text
                                                                                              ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                                h.smkeiroid = (
                                                                                                  (6):: numeric
                                                                                                ):: numeric(18, 0)
                                                                                              ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                              h.daihanrobunname AS konyuchubuncode, 
                                                                                              i.itembunrui, 
                                                                                              h.juchkbn, 
                                                                                              tm67.cname AS juchkbncname, 
                                                                                              h.henreasoncode, 
                                                                                              (
                                                                                                (
                                                                                                  (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                                ) || (h.henreasonname):: text
                                                                                              ) AS henreasonname 
                                                                                            FROM 
                                                                                              (
                                                                                                (
                                                                                                  (
                                                                                                    (
                                                                                                      saleh_itemstrategy h 
                                                                                                      JOIN salem_itemstrategy m ON (
                                                                                                        (
                                                                                                          (h.saleno):: text = (m.saleno):: text
                                                                                                        )
                                                                                                      )
                                                                                                    ) 
                                                                                                    JOIN tm14shkos "k" ON (
                                                                                                      (
                                                                                                        (m.itemcode):: text = ("k".itemcode):: text
                                                                                                      )
                                                                                                    )
                                                                                                  ) 
                                                                                                  JOIN tm39item_strategy i ON (
                                                                                                    (
                                                                                                      ("k".kosecode):: text = (i.itemcode):: text
                                                                                                    )
                                                                                                  )
                                                                                                ) 
                                                                                                LEFT JOIN tm67juch_nm tm67 ON (
                                                                                                  (
                                                                                                    (h.juchkbn):: text = (tm67.code):: text
                                                                                                  )
                                                                                                )
                                                                                              ) 
                                                                                            WHERE 
                                                                                              (
                                                                                                (
                                                                                                  (
                                                                                                    (
                                                                                                      (
                                                                                                        (
                                                                                                          (
                                                                                                            h.shukadate >= (
                                                                                                              (
                                                                                                                (
                                                                                                                  (
                                                                                                                    to_char(
                                                                                                                      add_months(
                                                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                                                        (
                                                                                                                          - (36):: bigint
                                                                                                                        )
                                                                                                                      ), 
                                                                                                                      ('yyyymm' :: character varying):: text
                                                                                                                    ) || ('01' :: character varying):: text
                                                                                                                  )
                                                                                                                ):: integer
                                                                                                              ):: numeric
                                                                                                            ):: numeric(18, 0)
                                                                                                          ) 
                                                                                                          AND (
                                                                                                            h.shukadate <= (
                                                                                                              (
                                                                                                                (
                                                                                                                  to_char(
                                                                                                                    (
                                                                                                                      last_day(
                                                                                                                        add_months(
                                                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                                                          (
                                                                                                                            - (1):: bigint
                                                                                                                          )
                                                                                                                        )
                                                                                                                      )
                                                                                                                    ):: timestamp without time zone, 
                                                                                                                    ('yyyymmdd' :: character varying):: text
                                                                                                                  )
                                                                                                                ):: integer
                                                                                                              ):: numeric
                                                                                                            ):: numeric(18, 0)
                                                                                                          )
                                                                                                        ) 
                                                                                                        AND (
                                                                                                          h.cancelflg = (
                                                                                                            (0):: numeric
                                                                                                          ):: numeric(18, 0)
                                                                                                        )
                                                                                                      ) 
                                                                                                      AND (
                                                                                                        (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                                      )
                                                                                                    ) 
                                                                                                    AND (
                                                                                                      (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                                    )
                                                                                                  ) 
                                                                                                  AND (
                                                                                                    (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                                  )
                                                                                                ) 
                                                                                                AND (
                                                                                                  (
                                                                                                    (
                                                                                                      (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                                    ) 
                                                                                                    OR (
                                                                                                      (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                                    )
                                                                                                  ) 
                                                                                                  OR (
                                                                                                    (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                                  )
                                                                                                )
                                                                                              )
                                                                                          ) view_tuhan_bunkai_shohin 
                                                                                        WHERE 
                                                                                          (
                                                                                            (
                                                                                              view_tuhan_bunkai_shohin.shukadate >= (
                                                                                                (
                                                                                                  (
                                                                                                    to_char(
                                                                                                      add_months(
                                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                                        (
                                                                                                          - (32):: bigint
                                                                                                        )
                                                                                                      ), 
                                                                                                      ('yyyymm' :: character varying):: text
                                                                                                    )
                                                                                                  ):: integer
                                                                                                ):: character varying
                                                                                              ):: text
                                                                                            ) 
                                                                                            AND (
                                                                                              view_tuhan_bunkai_shohin.shukadate <= (
                                                                                                (
                                                                                                  (
                                                                                                    to_char(
                                                                                                      (
                                                                                                        last_day(
                                                                                                          add_months(
                                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                                            (
                                                                                                              - (21):: bigint
                                                                                                            )
                                                                                                          )
                                                                                                        )
                                                                                                      ):: timestamp without time zone, 
                                                                                                      ('yyyymm' :: character varying):: text
                                                                                                    )
                                                                                                  ):: integer
                                                                                                ):: character varying
                                                                                              ):: text
                                                                                            )
                                                                                          ) 
                                                                                        GROUP BY 
                                                                                          view_tuhan_bunkai_shohin.channel, 
                                                                                          view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                          view_tuhan_bunkai_shohin.itembunrui, 
                                                                                          view_tuhan_bunkai_shohin.juchkbn, 
                                                                                          view_tuhan_bunkai_shohin.juchkbncname, 
                                                                                          view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                          view_tuhan_bunkai_shohin.henreasonname
                                                                                      ) 
                                                                                      UNION 
                                                                                      SELECT 
                                                                                        (
                                                                                          to_char(
                                                                                            add_months(
                                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                                              (
                                                                                                - (20):: bigint
                                                                                              )
                                                                                            ), 
                                                                                            ('yyyymm' :: character varying):: text
                                                                                          )
                                                                                        ):: integer AS nengetu, 
                                                                                        count(
                                                                                          DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                                        ) AS ninzu, 
                                                                                        view_tuhan_bunkai_shohin.channel, 
                                                                                        view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                        view_tuhan_bunkai_shohin.itembunrui, 
                                                                                        view_tuhan_bunkai_shohin.juchkbn, 
                                                                                        (
                                                                                          view_tuhan_bunkai_shohin.juchkbncname
                                                                                        ):: character varying AS juchkbncname, 
                                                                                        view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                        (
                                                                                          view_tuhan_bunkai_shohin.henreasonname
                                                                                        ):: character varying AS henreasonname 
                                                                                      FROM 
                                                                                        (
                                                                                          SELECT 
                                                                                            "substring"(
                                                                                              (
                                                                                                (h.shukadate):: character varying
                                                                                              ):: text, 
                                                                                              0, 
                                                                                              6
                                                                                            ) AS shukadate, 
                                                                                            h.kokyano, 
                                                                                            CASE WHEN (
                                                                                              (
                                                                                                h.kakokbn = (
                                                                                                  (1):: numeric
                                                                                                ):: numeric(18, 0)
                                                                                              ) 
                                                                                              OR (
                                                                                                (h.kakokbn IS NULL) 
                                                                                                AND ('1' IS NULL)
                                                                                              )
                                                                                            ) THEN CASE WHEN (
                                                                                              (h.kaisha):: text = ('000' :: character varying):: text
                                                                                            ) THEN '01:通販' :: character varying WHEN (
                                                                                              (h.kaisha):: text = ('001' :: character varying):: text
                                                                                            ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                              h.smkeiroid = (
                                                                                                (6):: numeric
                                                                                              ):: numeric(18, 0)
                                                                                            ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                            h.daihanrobunname AS konyuchubuncode, 
                                                                                            i.itembunrui, 
                                                                                            h.juchkbn, 
                                                                                            tm67.cname AS juchkbncname, 
                                                                                            h.henreasoncode, 
                                                                                            (
                                                                                              (
                                                                                                (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                              ) || (h.henreasonname):: text
                                                                                            ) AS henreasonname 
                                                                                          FROM 
                                                                                            (
                                                                                              (
                                                                                                (
                                                                                                  (
                                                                                                    saleh_itemstrategy h 
                                                                                                    JOIN salem_itemstrategy m ON (
                                                                                                      (
                                                                                                        (h.saleno):: text = (m.saleno):: text
                                                                                                      )
                                                                                                    )
                                                                                                  ) 
                                                                                                  JOIN tm14shkos "k" ON (
                                                                                                    (
                                                                                                      (m.itemcode):: text = ("k".itemcode):: text
                                                                                                    )
                                                                                                  )
                                                                                                ) 
                                                                                                JOIN tm39item_strategy i ON (
                                                                                                  (
                                                                                                    ("k".kosecode):: text = (i.itemcode):: text
                                                                                                  )
                                                                                                )
                                                                                              ) 
                                                                                              LEFT JOIN tm67juch_nm tm67 ON (
                                                                                                (
                                                                                                  (h.juchkbn):: text = (tm67.code):: text
                                                                                                )
                                                                                              )
                                                                                            ) 
                                                                                          WHERE 
                                                                                            (
                                                                                              (
                                                                                                (
                                                                                                  (
                                                                                                    (
                                                                                                      (
                                                                                                        (
                                                                                                          h.shukadate >= (
                                                                                                            (
                                                                                                              (
                                                                                                                (
                                                                                                                  to_char(
                                                                                                                    add_months(
                                                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                                                      (
                                                                                                                        - (36):: bigint
                                                                                                                      )
                                                                                                                    ), 
                                                                                                                    ('yyyymm' :: character varying):: text
                                                                                                                  ) || ('01' :: character varying):: text
                                                                                                                )
                                                                                                              ):: integer
                                                                                                            ):: numeric
                                                                                                          ):: numeric(18, 0)
                                                                                                        ) 
                                                                                                        AND (
                                                                                                          h.shukadate <= (
                                                                                                            (
                                                                                                              (
                                                                                                                to_char(
                                                                                                                  (
                                                                                                                    last_day(
                                                                                                                      add_months(
                                                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                                                        (
                                                                                                                          - (1):: bigint
                                                                                                                        )
                                                                                                                      )
                                                                                                                    )
                                                                                                                  ):: timestamp without time zone, 
                                                                                                                  ('yyyymmdd' :: character varying):: text
                                                                                                                )
                                                                                                              ):: integer
                                                                                                            ):: numeric
                                                                                                          ):: numeric(18, 0)
                                                                                                        )
                                                                                                      ) 
                                                                                                      AND (
                                                                                                        h.cancelflg = (
                                                                                                          (0):: numeric
                                                                                                        ):: numeric(18, 0)
                                                                                                      )
                                                                                                    ) 
                                                                                                    AND (
                                                                                                      (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                                    )
                                                                                                  ) 
                                                                                                  AND (
                                                                                                    (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                                  )
                                                                                                ) 
                                                                                                AND (
                                                                                                  (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                                )
                                                                                              ) 
                                                                                              AND (
                                                                                                (
                                                                                                  (
                                                                                                    (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                                  ) 
                                                                                                  OR (
                                                                                                    (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                                  )
                                                                                                ) 
                                                                                                OR (
                                                                                                  (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                                )
                                                                                              )
                                                                                            )
                                                                                        ) view_tuhan_bunkai_shohin 
                                                                                      WHERE 
                                                                                        (
                                                                                          (
                                                                                            view_tuhan_bunkai_shohin.shukadate >= (
                                                                                              (
                                                                                                (
                                                                                                  to_char(
                                                                                                    add_months(
                                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                                      (
                                                                                                        - (31):: bigint
                                                                                                      )
                                                                                                    ), 
                                                                                                    ('yyyymm' :: character varying):: text
                                                                                                  )
                                                                                                ):: integer
                                                                                              ):: character varying
                                                                                            ):: text
                                                                                          ) 
                                                                                          AND (
                                                                                            view_tuhan_bunkai_shohin.shukadate <= (
                                                                                              (
                                                                                                (
                                                                                                  to_char(
                                                                                                    (
                                                                                                      last_day(
                                                                                                        add_months(
                                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                                          (
                                                                                                            - (20):: bigint
                                                                                                          )
                                                                                                        )
                                                                                                      )
                                                                                                    ):: timestamp without time zone, 
                                                                                                    ('yyyymm' :: character varying):: text
                                                                                                  )
                                                                                                ):: integer
                                                                                              ):: character varying
                                                                                            ):: text
                                                                                          )
                                                                                        ) 
                                                                                      GROUP BY 
                                                                                        view_tuhan_bunkai_shohin.channel, 
                                                                                        view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                        view_tuhan_bunkai_shohin.itembunrui, 
                                                                                        view_tuhan_bunkai_shohin.juchkbn, 
                                                                                        view_tuhan_bunkai_shohin.juchkbncname, 
                                                                                        view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                        view_tuhan_bunkai_shohin.henreasonname
                                                                                    ) 
                                                                                    UNION 
                                                                                    SELECT 
                                                                                      (
                                                                                        to_char(
                                                                                          add_months(
                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                            (
                                                                                              - (19):: bigint
                                                                                            )
                                                                                          ), 
                                                                                          ('yyyymm' :: character varying):: text
                                                                                        )
                                                                                      ):: integer AS nengetu, 
                                                                                      count(
                                                                                        DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                                      ) AS ninzu, 
                                                                                      view_tuhan_bunkai_shohin.channel, 
                                                                                      view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                      view_tuhan_bunkai_shohin.itembunrui, 
                                                                                      view_tuhan_bunkai_shohin.juchkbn, 
                                                                                      (
                                                                                        view_tuhan_bunkai_shohin.juchkbncname
                                                                                      ):: character varying AS juchkbncname, 
                                                                                      view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                      (
                                                                                        view_tuhan_bunkai_shohin.henreasonname
                                                                                      ):: character varying AS henreasonname 
                                                                                    FROM 
                                                                                      (
                                                                                        SELECT 
                                                                                          "substring"(
                                                                                            (
                                                                                              (h.shukadate):: character varying
                                                                                            ):: text, 
                                                                                            0, 
                                                                                            6
                                                                                          ) AS shukadate, 
                                                                                          h.kokyano, 
                                                                                          CASE WHEN (
                                                                                            (
                                                                                              h.kakokbn = (
                                                                                                (1):: numeric
                                                                                              ):: numeric(18, 0)
                                                                                            ) 
                                                                                            OR (
                                                                                              (h.kakokbn IS NULL) 
                                                                                              AND ('1' IS NULL)
                                                                                            )
                                                                                          ) THEN CASE WHEN (
                                                                                            (h.kaisha):: text = ('000' :: character varying):: text
                                                                                          ) THEN '01:通販' :: character varying WHEN (
                                                                                            (h.kaisha):: text = ('001' :: character varying):: text
                                                                                          ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                            h.smkeiroid = (
                                                                                              (6):: numeric
                                                                                            ):: numeric(18, 0)
                                                                                          ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                          h.daihanrobunname AS konyuchubuncode, 
                                                                                          i.itembunrui, 
                                                                                          h.juchkbn, 
                                                                                          tm67.cname AS juchkbncname, 
                                                                                          h.henreasoncode, 
                                                                                          (
                                                                                            (
                                                                                              (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                            ) || (h.henreasonname):: text
                                                                                          ) AS henreasonname 
                                                                                        FROM 
                                                                                          (
                                                                                            (
                                                                                              (
                                                                                                (
                                                                                                  saleh_itemstrategy h 
                                                                                                  JOIN salem_itemstrategy m ON (
                                                                                                    (
                                                                                                      (h.saleno):: text = (m.saleno):: text
                                                                                                    )
                                                                                                  )
                                                                                                ) 
                                                                                                JOIN tm14shkos "k" ON (
                                                                                                  (
                                                                                                    (m.itemcode):: text = ("k".itemcode):: text
                                                                                                  )
                                                                                                )
                                                                                              ) 
                                                                                              JOIN tm39item_strategy i ON (
                                                                                                (
                                                                                                  ("k".kosecode):: text = (i.itemcode):: text
                                                                                                )
                                                                                              )
                                                                                            ) 
                                                                                            LEFT JOIN tm67juch_nm tm67 ON (
                                                                                              (
                                                                                                (h.juchkbn):: text = (tm67.code):: text
                                                                                              )
                                                                                            )
                                                                                          ) 
                                                                                        WHERE 
                                                                                          (
                                                                                            (
                                                                                              (
                                                                                                (
                                                                                                  (
                                                                                                    (
                                                                                                      (
                                                                                                        h.shukadate >= (
                                                                                                          (
                                                                                                            (
                                                                                                              (
                                                                                                                to_char(
                                                                                                                  add_months(
                                                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                                                    (
                                                                                                                      - (36):: bigint
                                                                                                                    )
                                                                                                                  ), 
                                                                                                                  ('yyyymm' :: character varying):: text
                                                                                                                ) || ('01' :: character varying):: text
                                                                                                              )
                                                                                                            ):: integer
                                                                                                          ):: numeric
                                                                                                        ):: numeric(18, 0)
                                                                                                      ) 
                                                                                                      AND (
                                                                                                        h.shukadate <= (
                                                                                                          (
                                                                                                            (
                                                                                                              to_char(
                                                                                                                (
                                                                                                                  last_day(
                                                                                                                    add_months(
                                                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                                                      (
                                                                                                                        - (1):: bigint
                                                                                                                      )
                                                                                                                    )
                                                                                                                  )
                                                                                                                ):: timestamp without time zone, 
                                                                                                                ('yyyymmdd' :: character varying):: text
                                                                                                              )
                                                                                                            ):: integer
                                                                                                          ):: numeric
                                                                                                        ):: numeric(18, 0)
                                                                                                      )
                                                                                                    ) 
                                                                                                    AND (
                                                                                                      h.cancelflg = (
                                                                                                        (0):: numeric
                                                                                                      ):: numeric(18, 0)
                                                                                                    )
                                                                                                  ) 
                                                                                                  AND (
                                                                                                    (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                                  )
                                                                                                ) 
                                                                                                AND (
                                                                                                  (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                                )
                                                                                              ) 
                                                                                              AND (
                                                                                                (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                              )
                                                                                            ) 
                                                                                            AND (
                                                                                              (
                                                                                                (
                                                                                                  (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                                ) 
                                                                                                OR (
                                                                                                  (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                                )
                                                                                              ) 
                                                                                              OR (
                                                                                                (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                              )
                                                                                            )
                                                                                          )
                                                                                      ) view_tuhan_bunkai_shohin 
                                                                                    WHERE 
                                                                                      (
                                                                                        (
                                                                                          view_tuhan_bunkai_shohin.shukadate >= (
                                                                                            (
                                                                                              (
                                                                                                to_char(
                                                                                                  add_months(
                                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                                    (
                                                                                                      - (30):: bigint
                                                                                                    )
                                                                                                  ), 
                                                                                                  ('yyyymm' :: character varying):: text
                                                                                                )
                                                                                              ):: integer
                                                                                            ):: character varying
                                                                                          ):: text
                                                                                        ) 
                                                                                        AND (
                                                                                          view_tuhan_bunkai_shohin.shukadate <= (
                                                                                            (
                                                                                              (
                                                                                                to_char(
                                                                                                  (
                                                                                                    last_day(
                                                                                                      add_months(
                                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                                        (
                                                                                                          - (19):: bigint
                                                                                                        )
                                                                                                      )
                                                                                                    )
                                                                                                  ):: timestamp without time zone, 
                                                                                                  ('yyyymm' :: character varying):: text
                                                                                                )
                                                                                              ):: integer
                                                                                            ):: character varying
                                                                                          ):: text
                                                                                        )
                                                                                      ) 
                                                                                    GROUP BY 
                                                                                      view_tuhan_bunkai_shohin.channel, 
                                                                                      view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                      view_tuhan_bunkai_shohin.itembunrui, 
                                                                                      view_tuhan_bunkai_shohin.juchkbn, 
                                                                                      view_tuhan_bunkai_shohin.juchkbncname, 
                                                                                      view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                      view_tuhan_bunkai_shohin.henreasonname
                                                                                  ) 
                                                                                  UNION 
                                                                                  SELECT 
                                                                                    (
                                                                                      to_char(
                                                                                        add_months(
                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                          (
                                                                                            - (18):: bigint
                                                                                          )
                                                                                        ), 
                                                                                        ('yyyymm' :: character varying):: text
                                                                                      )
                                                                                    ):: integer AS nengetu, 
                                                                                    count(
                                                                                      DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                                    ) AS ninzu, 
                                                                                    view_tuhan_bunkai_shohin.channel, 
                                                                                    view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                    view_tuhan_bunkai_shohin.itembunrui, 
                                                                                    view_tuhan_bunkai_shohin.juchkbn, 
                                                                                    (
                                                                                      view_tuhan_bunkai_shohin.juchkbncname
                                                                                    ):: character varying AS juchkbncname, 
                                                                                    view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                    (
                                                                                      view_tuhan_bunkai_shohin.henreasonname
                                                                                    ):: character varying AS henreasonname 
                                                                                  FROM 
                                                                                    (
                                                                                      SELECT 
                                                                                        "substring"(
                                                                                          (
                                                                                            (h.shukadate):: character varying
                                                                                          ):: text, 
                                                                                          0, 
                                                                                          6
                                                                                        ) AS shukadate, 
                                                                                        h.kokyano, 
                                                                                        CASE WHEN (
                                                                                          (
                                                                                            h.kakokbn = (
                                                                                              (1):: numeric
                                                                                            ):: numeric(18, 0)
                                                                                          ) 
                                                                                          OR (
                                                                                            (h.kakokbn IS NULL) 
                                                                                            AND ('1' IS NULL)
                                                                                          )
                                                                                        ) THEN CASE WHEN (
                                                                                          (h.kaisha):: text = ('000' :: character varying):: text
                                                                                        ) THEN '01:通販' :: character varying WHEN (
                                                                                          (h.kaisha):: text = ('001' :: character varying):: text
                                                                                        ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                          h.smkeiroid = (
                                                                                            (6):: numeric
                                                                                          ):: numeric(18, 0)
                                                                                        ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                        h.daihanrobunname AS konyuchubuncode, 
                                                                                        i.itembunrui, 
                                                                                        h.juchkbn, 
                                                                                        tm67.cname AS juchkbncname, 
                                                                                        h.henreasoncode, 
                                                                                        (
                                                                                          (
                                                                                            (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                          ) || (h.henreasonname):: text
                                                                                        ) AS henreasonname 
                                                                                      FROM 
                                                                                        (
                                                                                          (
                                                                                            (
                                                                                              (
                                                                                                saleh_itemstrategy h 
                                                                                                JOIN salem_itemstrategy m ON (
                                                                                                  (
                                                                                                    (h.saleno):: text = (m.saleno):: text
                                                                                                  )
                                                                                                )
                                                                                              ) 
                                                                                              JOIN tm14shkos "k" ON (
                                                                                                (
                                                                                                  (m.itemcode):: text = ("k".itemcode):: text
                                                                                                )
                                                                                              )
                                                                                            ) 
                                                                                            JOIN tm39item_strategy i ON (
                                                                                              (
                                                                                                ("k".kosecode):: text = (i.itemcode):: text
                                                                                              )
                                                                                            )
                                                                                          ) 
                                                                                          LEFT JOIN tm67juch_nm tm67 ON (
                                                                                            (
                                                                                              (h.juchkbn):: text = (tm67.code):: text
                                                                                            )
                                                                                          )
                                                                                        ) 
                                                                                      WHERE 
                                                                                        (
                                                                                          (
                                                                                            (
                                                                                              (
                                                                                                (
                                                                                                  (
                                                                                                    (
                                                                                                      h.shukadate >= (
                                                                                                        (
                                                                                                          (
                                                                                                            (
                                                                                                              to_char(
                                                                                                                add_months(
                                                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                                                  (
                                                                                                                    - (36):: bigint
                                                                                                                  )
                                                                                                                ), 
                                                                                                                ('yyyymm' :: character varying):: text
                                                                                                              ) || ('01' :: character varying):: text
                                                                                                            )
                                                                                                          ):: integer
                                                                                                        ):: numeric
                                                                                                      ):: numeric(18, 0)
                                                                                                    ) 
                                                                                                    AND (
                                                                                                      h.shukadate <= (
                                                                                                        (
                                                                                                          (
                                                                                                            to_char(
                                                                                                              (
                                                                                                                last_day(
                                                                                                                  add_months(
                                                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                                                    (
                                                                                                                      - (1):: bigint
                                                                                                                    )
                                                                                                                  )
                                                                                                                )
                                                                                                              ):: timestamp without time zone, 
                                                                                                              ('yyyymmdd' :: character varying):: text
                                                                                                            )
                                                                                                          ):: integer
                                                                                                        ):: numeric
                                                                                                      ):: numeric(18, 0)
                                                                                                    )
                                                                                                  ) 
                                                                                                  AND (
                                                                                                    h.cancelflg = (
                                                                                                      (0):: numeric
                                                                                                    ):: numeric(18, 0)
                                                                                                  )
                                                                                                ) 
                                                                                                AND (
                                                                                                  (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                                )
                                                                                              ) 
                                                                                              AND (
                                                                                                (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                              )
                                                                                            ) 
                                                                                            AND (
                                                                                              (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                            )
                                                                                          ) 
                                                                                          AND (
                                                                                            (
                                                                                              (
                                                                                                (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                              ) 
                                                                                              OR (
                                                                                                (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                              )
                                                                                            ) 
                                                                                            OR (
                                                                                              (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                            )
                                                                                          )
                                                                                        )
                                                                                    ) view_tuhan_bunkai_shohin 
                                                                                  WHERE 
                                                                                    (
                                                                                      (
                                                                                        view_tuhan_bunkai_shohin.shukadate >= (
                                                                                          (
                                                                                            (
                                                                                              to_char(
                                                                                                add_months(
                                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                                  (
                                                                                                    - (29):: bigint
                                                                                                  )
                                                                                                ), 
                                                                                                ('yyyymm' :: character varying):: text
                                                                                              )
                                                                                            ):: integer
                                                                                          ):: character varying
                                                                                        ):: text
                                                                                      ) 
                                                                                      AND (
                                                                                        view_tuhan_bunkai_shohin.shukadate <= (
                                                                                          (
                                                                                            (
                                                                                              to_char(
                                                                                                (
                                                                                                  last_day(
                                                                                                    add_months(
                                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                                      (
                                                                                                        - (18):: bigint
                                                                                                      )
                                                                                                    )
                                                                                                  )
                                                                                                ):: timestamp without time zone, 
                                                                                                ('yyyymm' :: character varying):: text
                                                                                              )
                                                                                            ):: integer
                                                                                          ):: character varying
                                                                                        ):: text
                                                                                      )
                                                                                    ) 
                                                                                  GROUP BY 
                                                                                    view_tuhan_bunkai_shohin.channel, 
                                                                                    view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                    view_tuhan_bunkai_shohin.itembunrui, 
                                                                                    view_tuhan_bunkai_shohin.juchkbn, 
                                                                                    view_tuhan_bunkai_shohin.juchkbncname, 
                                                                                    view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                    view_tuhan_bunkai_shohin.henreasonname
                                                                                ) 
                                                                                UNION 
                                                                                SELECT 
                                                                                  (
                                                                                    to_char(
                                                                                      add_months(
                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                        (
                                                                                          - (17):: bigint
                                                                                        )
                                                                                      ), 
                                                                                      ('yyyymm' :: character varying):: text
                                                                                    )
                                                                                  ):: integer AS nengetu, 
                                                                                  count(
                                                                                    DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                                  ) AS ninzu, 
                                                                                  view_tuhan_bunkai_shohin.channel, 
                                                                                  view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                  view_tuhan_bunkai_shohin.itembunrui, 
                                                                                  view_tuhan_bunkai_shohin.juchkbn, 
                                                                                  (
                                                                                    view_tuhan_bunkai_shohin.juchkbncname
                                                                                  ):: character varying AS juchkbncname, 
                                                                                  view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                  (
                                                                                    view_tuhan_bunkai_shohin.henreasonname
                                                                                  ):: character varying AS henreasonname 
                                                                                FROM 
                                                                                  (
                                                                                    SELECT 
                                                                                      "substring"(
                                                                                        (
                                                                                          (h.shukadate):: character varying
                                                                                        ):: text, 
                                                                                        0, 
                                                                                        6
                                                                                      ) AS shukadate, 
                                                                                      h.kokyano, 
                                                                                      CASE WHEN (
                                                                                        (
                                                                                          h.kakokbn = (
                                                                                            (1):: numeric
                                                                                          ):: numeric(18, 0)
                                                                                        ) 
                                                                                        OR (
                                                                                          (h.kakokbn IS NULL) 
                                                                                          AND ('1' IS NULL)
                                                                                        )
                                                                                      ) THEN CASE WHEN (
                                                                                        (h.kaisha):: text = ('000' :: character varying):: text
                                                                                      ) THEN '01:通販' :: character varying WHEN (
                                                                                        (h.kaisha):: text = ('001' :: character varying):: text
                                                                                      ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                        h.smkeiroid = (
                                                                                          (6):: numeric
                                                                                        ):: numeric(18, 0)
                                                                                      ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                      h.daihanrobunname AS konyuchubuncode, 
                                                                                      i.itembunrui, 
                                                                                      h.juchkbn, 
                                                                                      tm67.cname AS juchkbncname, 
                                                                                      h.henreasoncode, 
                                                                                      (
                                                                                        (
                                                                                          (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                        ) || (h.henreasonname):: text
                                                                                      ) AS henreasonname 
                                                                                    FROM 
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            (
                                                                                              saleh_itemstrategy h 
                                                                                              JOIN salem_itemstrategy m ON (
                                                                                                (
                                                                                                  (h.saleno):: text = (m.saleno):: text
                                                                                                )
                                                                                              )
                                                                                            ) 
                                                                                            JOIN tm14shkos "k" ON (
                                                                                              (
                                                                                                (m.itemcode):: text = ("k".itemcode):: text
                                                                                              )
                                                                                            )
                                                                                          ) 
                                                                                          JOIN tm39item_strategy i ON (
                                                                                            (
                                                                                              ("k".kosecode):: text = (i.itemcode):: text
                                                                                            )
                                                                                          )
                                                                                        ) 
                                                                                        LEFT JOIN tm67juch_nm tm67 ON (
                                                                                          (
                                                                                            (h.juchkbn):: text = (tm67.code):: text
                                                                                          )
                                                                                        )
                                                                                      ) 
                                                                                    WHERE 
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            (
                                                                                              (
                                                                                                (
                                                                                                  (
                                                                                                    h.shukadate >= (
                                                                                                      (
                                                                                                        (
                                                                                                          (
                                                                                                            to_char(
                                                                                                              add_months(
                                                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                                                (
                                                                                                                  - (36):: bigint
                                                                                                                )
                                                                                                              ), 
                                                                                                              ('yyyymm' :: character varying):: text
                                                                                                            ) || ('01' :: character varying):: text
                                                                                                          )
                                                                                                        ):: integer
                                                                                                      ):: numeric
                                                                                                    ):: numeric(18, 0)
                                                                                                  ) 
                                                                                                  AND (
                                                                                                    h.shukadate <= (
                                                                                                      (
                                                                                                        (
                                                                                                          to_char(
                                                                                                            (
                                                                                                              last_day(
                                                                                                                add_months(
                                                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                                                  (
                                                                                                                    - (1):: bigint
                                                                                                                  )
                                                                                                                )
                                                                                                              )
                                                                                                            ):: timestamp without time zone, 
                                                                                                            ('yyyymmdd' :: character varying):: text
                                                                                                          )
                                                                                                        ):: integer
                                                                                                      ):: numeric
                                                                                                    ):: numeric(18, 0)
                                                                                                  )
                                                                                                ) 
                                                                                                AND (
                                                                                                  h.cancelflg = (
                                                                                                    (0):: numeric
                                                                                                  ):: numeric(18, 0)
                                                                                                )
                                                                                              ) 
                                                                                              AND (
                                                                                                (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                              )
                                                                                            ) 
                                                                                            AND (
                                                                                              (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                            )
                                                                                          ) 
                                                                                          AND (
                                                                                            (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                          )
                                                                                        ) 
                                                                                        AND (
                                                                                          (
                                                                                            (
                                                                                              (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                            ) 
                                                                                            OR (
                                                                                              (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                            )
                                                                                          ) 
                                                                                          OR (
                                                                                            (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                          )
                                                                                        )
                                                                                      )
                                                                                  ) view_tuhan_bunkai_shohin 
                                                                                WHERE 
                                                                                  (
                                                                                    (
                                                                                      view_tuhan_bunkai_shohin.shukadate >= (
                                                                                        (
                                                                                          (
                                                                                            to_char(
                                                                                              add_months(
                                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                                (
                                                                                                  - (28):: bigint
                                                                                                )
                                                                                              ), 
                                                                                              ('yyyymm' :: character varying):: text
                                                                                            )
                                                                                          ):: integer
                                                                                        ):: character varying
                                                                                      ):: text
                                                                                    ) 
                                                                                    AND (
                                                                                      view_tuhan_bunkai_shohin.shukadate <= (
                                                                                        (
                                                                                          (
                                                                                            to_char(
                                                                                              (
                                                                                                last_day(
                                                                                                  add_months(
                                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                                    (
                                                                                                      - (17):: bigint
                                                                                                    )
                                                                                                  )
                                                                                                )
                                                                                              ):: timestamp without time zone, 
                                                                                              ('yyyymm' :: character varying):: text
                                                                                            )
                                                                                          ):: integer
                                                                                        ):: character varying
                                                                                      ):: text
                                                                                    )
                                                                                  ) 
                                                                                GROUP BY 
                                                                                  view_tuhan_bunkai_shohin.channel, 
                                                                                  view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                  view_tuhan_bunkai_shohin.itembunrui, 
                                                                                  view_tuhan_bunkai_shohin.juchkbn, 
                                                                                  view_tuhan_bunkai_shohin.juchkbncname, 
                                                                                  view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                  view_tuhan_bunkai_shohin.henreasonname
                                                                              ) 
                                                                              UNION 
                                                                              SELECT 
                                                                                (
                                                                                  to_char(
                                                                                    add_months(
                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                      (
                                                                                        - (16):: bigint
                                                                                      )
                                                                                    ), 
                                                                                    ('yyyymm' :: character varying):: text
                                                                                  )
                                                                                ):: integer AS nengetu, 
                                                                                count(
                                                                                  DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                                ) AS ninzu, 
                                                                                view_tuhan_bunkai_shohin.channel, 
                                                                                view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                view_tuhan_bunkai_shohin.itembunrui, 
                                                                                view_tuhan_bunkai_shohin.juchkbn, 
                                                                                (
                                                                                  view_tuhan_bunkai_shohin.juchkbncname
                                                                                ):: character varying AS juchkbncname, 
                                                                                view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                (
                                                                                  view_tuhan_bunkai_shohin.henreasonname
                                                                                ):: character varying AS henreasonname 
                                                                              FROM 
                                                                                (
                                                                                  SELECT 
                                                                                    "substring"(
                                                                                      (
                                                                                        (h.shukadate):: character varying
                                                                                      ):: text, 
                                                                                      0, 
                                                                                      6
                                                                                    ) AS shukadate, 
                                                                                    h.kokyano, 
                                                                                    CASE WHEN (
                                                                                      (
                                                                                        h.kakokbn = (
                                                                                          (1):: numeric
                                                                                        ):: numeric(18, 0)
                                                                                      ) 
                                                                                      OR (
                                                                                        (h.kakokbn IS NULL) 
                                                                                        AND ('1' IS NULL)
                                                                                      )
                                                                                    ) THEN CASE WHEN (
                                                                                      (h.kaisha):: text = ('000' :: character varying):: text
                                                                                    ) THEN '01:通販' :: character varying WHEN (
                                                                                      (h.kaisha):: text = ('001' :: character varying):: text
                                                                                    ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                      h.smkeiroid = (
                                                                                        (6):: numeric
                                                                                      ):: numeric(18, 0)
                                                                                    ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                    h.daihanrobunname AS konyuchubuncode, 
                                                                                    i.itembunrui, 
                                                                                    h.juchkbn, 
                                                                                    tm67.cname AS juchkbncname, 
                                                                                    h.henreasoncode, 
                                                                                    (
                                                                                      (
                                                                                        (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                      ) || (h.henreasonname):: text
                                                                                    ) AS henreasonname 
                                                                                  FROM 
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            saleh_itemstrategy h 
                                                                                            JOIN salem_itemstrategy m ON (
                                                                                              (
                                                                                                (h.saleno):: text = (m.saleno):: text
                                                                                              )
                                                                                            )
                                                                                          ) 
                                                                                          JOIN tm14shkos "k" ON (
                                                                                            (
                                                                                              (m.itemcode):: text = ("k".itemcode):: text
                                                                                            )
                                                                                          )
                                                                                        ) 
                                                                                        JOIN tm39item_strategy i ON (
                                                                                          (
                                                                                            ("k".kosecode):: text = (i.itemcode):: text
                                                                                          )
                                                                                        )
                                                                                      ) 
                                                                                      LEFT JOIN tm67juch_nm tm67 ON (
                                                                                        (
                                                                                          (h.juchkbn):: text = (tm67.code):: text
                                                                                        )
                                                                                      )
                                                                                    ) 
                                                                                  WHERE 
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            (
                                                                                              (
                                                                                                (
                                                                                                  h.shukadate >= (
                                                                                                    (
                                                                                                      (
                                                                                                        (
                                                                                                          to_char(
                                                                                                            add_months(
                                                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                                                              (
                                                                                                                - (36):: bigint
                                                                                                              )
                                                                                                            ), 
                                                                                                            ('yyyymm' :: character varying):: text
                                                                                                          ) || ('01' :: character varying):: text
                                                                                                        )
                                                                                                      ):: integer
                                                                                                    ):: numeric
                                                                                                  ):: numeric(18, 0)
                                                                                                ) 
                                                                                                AND (
                                                                                                  h.shukadate <= (
                                                                                                    (
                                                                                                      (
                                                                                                        to_char(
                                                                                                          (
                                                                                                            last_day(
                                                                                                              add_months(
                                                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                                                (
                                                                                                                  - (1):: bigint
                                                                                                                )
                                                                                                              )
                                                                                                            )
                                                                                                          ):: timestamp without time zone, 
                                                                                                          ('yyyymmdd' :: character varying):: text
                                                                                                        )
                                                                                                      ):: integer
                                                                                                    ):: numeric
                                                                                                  ):: numeric(18, 0)
                                                                                                )
                                                                                              ) 
                                                                                              AND (
                                                                                                h.cancelflg = (
                                                                                                  (0):: numeric
                                                                                                ):: numeric(18, 0)
                                                                                              )
                                                                                            ) 
                                                                                            AND (
                                                                                              (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                            )
                                                                                          ) 
                                                                                          AND (
                                                                                            (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                          )
                                                                                        ) 
                                                                                        AND (
                                                                                          (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                        )
                                                                                      ) 
                                                                                      AND (
                                                                                        (
                                                                                          (
                                                                                            (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                          ) 
                                                                                          OR (
                                                                                            (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                          )
                                                                                        ) 
                                                                                        OR (
                                                                                          (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                        )
                                                                                      )
                                                                                    )
                                                                                ) view_tuhan_bunkai_shohin 
                                                                              WHERE 
                                                                                (
                                                                                  (
                                                                                    view_tuhan_bunkai_shohin.shukadate >= (
                                                                                      (
                                                                                        (
                                                                                          to_char(
                                                                                            add_months(
                                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                                              (
                                                                                                - (27):: bigint
                                                                                              )
                                                                                            ), 
                                                                                            ('yyyymm' :: character varying):: text
                                                                                          )
                                                                                        ):: integer
                                                                                      ):: character varying
                                                                                    ):: text
                                                                                  ) 
                                                                                  AND (
                                                                                    view_tuhan_bunkai_shohin.shukadate <= (
                                                                                      (
                                                                                        (
                                                                                          to_char(
                                                                                            (
                                                                                              last_day(
                                                                                                add_months(
                                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                                  (
                                                                                                    - (16):: bigint
                                                                                                  )
                                                                                                )
                                                                                              )
                                                                                            ):: timestamp without time zone, 
                                                                                            ('yyyymm' :: character varying):: text
                                                                                          )
                                                                                        ):: integer
                                                                                      ):: character varying
                                                                                    ):: text
                                                                                  )
                                                                                ) 
                                                                              GROUP BY 
                                                                                view_tuhan_bunkai_shohin.channel, 
                                                                                view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                                view_tuhan_bunkai_shohin.itembunrui, 
                                                                                view_tuhan_bunkai_shohin.juchkbn, 
                                                                                view_tuhan_bunkai_shohin.juchkbncname, 
                                                                                view_tuhan_bunkai_shohin.henreasoncode, 
                                                                                view_tuhan_bunkai_shohin.henreasonname
                                                                            ) 
                                                                            UNION 
                                                                            SELECT 
                                                                              (
                                                                                to_char(
                                                                                  add_months(
                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                    (
                                                                                      - (15):: bigint
                                                                                    )
                                                                                  ), 
                                                                                  ('yyyymm' :: character varying):: text
                                                                                )
                                                                              ):: integer AS nengetu, 
                                                                              count(
                                                                                DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                              ) AS ninzu, 
                                                                              view_tuhan_bunkai_shohin.channel, 
                                                                              view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                              view_tuhan_bunkai_shohin.itembunrui, 
                                                                              view_tuhan_bunkai_shohin.juchkbn, 
                                                                              (
                                                                                view_tuhan_bunkai_shohin.juchkbncname
                                                                              ):: character varying AS juchkbncname, 
                                                                              view_tuhan_bunkai_shohin.henreasoncode, 
                                                                              (
                                                                                view_tuhan_bunkai_shohin.henreasonname
                                                                              ):: character varying AS henreasonname 
                                                                            FROM 
                                                                              (
                                                                                SELECT 
                                                                                  "substring"(
                                                                                    (
                                                                                      (h.shukadate):: character varying
                                                                                    ):: text, 
                                                                                    0, 
                                                                                    6
                                                                                  ) AS shukadate, 
                                                                                  h.kokyano, 
                                                                                  CASE WHEN (
                                                                                    (
                                                                                      h.kakokbn = (
                                                                                        (1):: numeric
                                                                                      ):: numeric(18, 0)
                                                                                    ) 
                                                                                    OR (
                                                                                      (h.kakokbn IS NULL) 
                                                                                      AND ('1' IS NULL)
                                                                                    )
                                                                                  ) THEN CASE WHEN (
                                                                                    (h.kaisha):: text = ('000' :: character varying):: text
                                                                                  ) THEN '01:通販' :: character varying WHEN (
                                                                                    (h.kaisha):: text = ('001' :: character varying):: text
                                                                                  ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                    h.smkeiroid = (
                                                                                      (6):: numeric
                                                                                    ):: numeric(18, 0)
                                                                                  ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                  h.daihanrobunname AS konyuchubuncode, 
                                                                                  i.itembunrui, 
                                                                                  h.juchkbn, 
                                                                                  tm67.cname AS juchkbncname, 
                                                                                  h.henreasoncode, 
                                                                                  (
                                                                                    (
                                                                                      (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                    ) || (h.henreasonname):: text
                                                                                  ) AS henreasonname 
                                                                                FROM 
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          saleh_itemstrategy h 
                                                                                          JOIN salem_itemstrategy m ON (
                                                                                            (
                                                                                              (h.saleno):: text = (m.saleno):: text
                                                                                            )
                                                                                          )
                                                                                        ) 
                                                                                        JOIN tm14shkos "k" ON (
                                                                                          (
                                                                                            (m.itemcode):: text = ("k".itemcode):: text
                                                                                          )
                                                                                        )
                                                                                      ) 
                                                                                      JOIN tm39item_strategy i ON (
                                                                                        (
                                                                                          ("k".kosecode):: text = (i.itemcode):: text
                                                                                        )
                                                                                      )
                                                                                    ) 
                                                                                    LEFT JOIN tm67juch_nm tm67 ON (
                                                                                      (
                                                                                        (h.juchkbn):: text = (tm67.code):: text
                                                                                      )
                                                                                    )
                                                                                  ) 
                                                                                WHERE 
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            (
                                                                                              (
                                                                                                h.shukadate >= (
                                                                                                  (
                                                                                                    (
                                                                                                      (
                                                                                                        to_char(
                                                                                                          add_months(
                                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                                            (
                                                                                                              - (36):: bigint
                                                                                                            )
                                                                                                          ), 
                                                                                                          ('yyyymm' :: character varying):: text
                                                                                                        ) || ('01' :: character varying):: text
                                                                                                      )
                                                                                                    ):: integer
                                                                                                  ):: numeric
                                                                                                ):: numeric(18, 0)
                                                                                              ) 
                                                                                              AND (
                                                                                                h.shukadate <= (
                                                                                                  (
                                                                                                    (
                                                                                                      to_char(
                                                                                                        (
                                                                                                          last_day(
                                                                                                            add_months(
                                                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                                                              (
                                                                                                                - (1):: bigint
                                                                                                              )
                                                                                                            )
                                                                                                          )
                                                                                                        ):: timestamp without time zone, 
                                                                                                        ('yyyymmdd' :: character varying):: text
                                                                                                      )
                                                                                                    ):: integer
                                                                                                  ):: numeric
                                                                                                ):: numeric(18, 0)
                                                                                              )
                                                                                            ) 
                                                                                            AND (
                                                                                              h.cancelflg = (
                                                                                                (0):: numeric
                                                                                              ):: numeric(18, 0)
                                                                                            )
                                                                                          ) 
                                                                                          AND (
                                                                                            (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                          )
                                                                                        ) 
                                                                                        AND (
                                                                                          (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                        )
                                                                                      ) 
                                                                                      AND (
                                                                                        (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                      )
                                                                                    ) 
                                                                                    AND (
                                                                                      (
                                                                                        (
                                                                                          (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                        ) 
                                                                                        OR (
                                                                                          (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                        )
                                                                                      ) 
                                                                                      OR (
                                                                                        (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                      )
                                                                                    )
                                                                                  )
                                                                              ) view_tuhan_bunkai_shohin 
                                                                            WHERE 
                                                                              (
                                                                                (
                                                                                  view_tuhan_bunkai_shohin.shukadate >= (
                                                                                    (
                                                                                      (
                                                                                        to_char(
                                                                                          add_months(
                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                            (
                                                                                              - (26):: bigint
                                                                                            )
                                                                                          ), 
                                                                                          ('yyyymm' :: character varying):: text
                                                                                        )
                                                                                      ):: integer
                                                                                    ):: character varying
                                                                                  ):: text
                                                                                ) 
                                                                                AND (
                                                                                  view_tuhan_bunkai_shohin.shukadate <= (
                                                                                    (
                                                                                      (
                                                                                        to_char(
                                                                                          (
                                                                                            last_day(
                                                                                              add_months(
                                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                                (
                                                                                                  - (15):: bigint
                                                                                                )
                                                                                              )
                                                                                            )
                                                                                          ):: timestamp without time zone, 
                                                                                          ('yyyymm' :: character varying):: text
                                                                                        )
                                                                                      ):: integer
                                                                                    ):: character varying
                                                                                  ):: text
                                                                                )
                                                                              ) 
                                                                            GROUP BY 
                                                                              view_tuhan_bunkai_shohin.channel, 
                                                                              view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                              view_tuhan_bunkai_shohin.itembunrui, 
                                                                              view_tuhan_bunkai_shohin.juchkbn, 
                                                                              view_tuhan_bunkai_shohin.juchkbncname, 
                                                                              view_tuhan_bunkai_shohin.henreasoncode, 
                                                                              view_tuhan_bunkai_shohin.henreasonname
                                                                          ) 
                                                                          UNION 
                                                                          SELECT 
                                                                            (
                                                                              to_char(
                                                                                add_months(
                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                  (
                                                                                    - (14):: bigint
                                                                                  )
                                                                                ), 
                                                                                ('yyyymm' :: character varying):: text
                                                                              )
                                                                            ):: integer AS nengetu, 
                                                                            count(
                                                                              DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                            ) AS ninzu, 
                                                                            view_tuhan_bunkai_shohin.channel, 
                                                                            view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                            view_tuhan_bunkai_shohin.itembunrui, 
                                                                            view_tuhan_bunkai_shohin.juchkbn, 
                                                                            (
                                                                              view_tuhan_bunkai_shohin.juchkbncname
                                                                            ):: character varying AS juchkbncname, 
                                                                            view_tuhan_bunkai_shohin.henreasoncode, 
                                                                            (
                                                                              view_tuhan_bunkai_shohin.henreasonname
                                                                            ):: character varying AS henreasonname 
                                                                          FROM 
                                                                            (
                                                                              SELECT 
                                                                                "substring"(
                                                                                  (
                                                                                    (h.shukadate):: character varying
                                                                                  ):: text, 
                                                                                  0, 
                                                                                  6
                                                                                ) AS shukadate, 
                                                                                h.kokyano, 
                                                                                CASE WHEN (
                                                                                  (
                                                                                    h.kakokbn = (
                                                                                      (1):: numeric
                                                                                    ):: numeric(18, 0)
                                                                                  ) 
                                                                                  OR (
                                                                                    (h.kakokbn IS NULL) 
                                                                                    AND ('1' IS NULL)
                                                                                  )
                                                                                ) THEN CASE WHEN (
                                                                                  (h.kaisha):: text = ('000' :: character varying):: text
                                                                                ) THEN '01:通販' :: character varying WHEN (
                                                                                  (h.kaisha):: text = ('001' :: character varying):: text
                                                                                ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                  h.smkeiroid = (
                                                                                    (6):: numeric
                                                                                  ):: numeric(18, 0)
                                                                                ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                                h.daihanrobunname AS konyuchubuncode, 
                                                                                i.itembunrui, 
                                                                                h.juchkbn, 
                                                                                tm67.cname AS juchkbncname, 
                                                                                h.henreasoncode, 
                                                                                (
                                                                                  (
                                                                                    (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                  ) || (h.henreasonname):: text
                                                                                ) AS henreasonname 
                                                                              FROM 
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        saleh_itemstrategy h 
                                                                                        JOIN salem_itemstrategy m ON (
                                                                                          (
                                                                                            (h.saleno):: text = (m.saleno):: text
                                                                                          )
                                                                                        )
                                                                                      ) 
                                                                                      JOIN tm14shkos "k" ON (
                                                                                        (
                                                                                          (m.itemcode):: text = ("k".itemcode):: text
                                                                                        )
                                                                                      )
                                                                                    ) 
                                                                                    JOIN tm39item_strategy i ON (
                                                                                      (
                                                                                        ("k".kosecode):: text = (i.itemcode):: text
                                                                                      )
                                                                                    )
                                                                                  ) 
                                                                                  LEFT JOIN tm67juch_nm tm67 ON (
                                                                                    (
                                                                                      (h.juchkbn):: text = (tm67.code):: text
                                                                                    )
                                                                                  )
                                                                                ) 
                                                                              WHERE 
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            (
                                                                                              h.shukadate >= (
                                                                                                (
                                                                                                  (
                                                                                                    (
                                                                                                      to_char(
                                                                                                        add_months(
                                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                                          (
                                                                                                            - (36):: bigint
                                                                                                          )
                                                                                                        ), 
                                                                                                        ('yyyymm' :: character varying):: text
                                                                                                      ) || ('01' :: character varying):: text
                                                                                                    )
                                                                                                  ):: integer
                                                                                                ):: numeric
                                                                                              ):: numeric(18, 0)
                                                                                            ) 
                                                                                            AND (
                                                                                              h.shukadate <= (
                                                                                                (
                                                                                                  (
                                                                                                    to_char(
                                                                                                      (
                                                                                                        last_day(
                                                                                                          add_months(
                                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                                            (
                                                                                                              - (1):: bigint
                                                                                                            )
                                                                                                          )
                                                                                                        )
                                                                                                      ):: timestamp without time zone, 
                                                                                                      ('yyyymmdd' :: character varying):: text
                                                                                                    )
                                                                                                  ):: integer
                                                                                                ):: numeric
                                                                                              ):: numeric(18, 0)
                                                                                            )
                                                                                          ) 
                                                                                          AND (
                                                                                            h.cancelflg = (
                                                                                              (0):: numeric
                                                                                            ):: numeric(18, 0)
                                                                                          )
                                                                                        ) 
                                                                                        AND (
                                                                                          (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                        )
                                                                                      ) 
                                                                                      AND (
                                                                                        (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                      )
                                                                                    ) 
                                                                                    AND (
                                                                                      (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                    )
                                                                                  ) 
                                                                                  AND (
                                                                                    (
                                                                                      (
                                                                                        (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                      ) 
                                                                                      OR (
                                                                                        (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                      )
                                                                                    ) 
                                                                                    OR (
                                                                                      (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                    )
                                                                                  )
                                                                                )
                                                                            ) view_tuhan_bunkai_shohin 
                                                                          WHERE 
                                                                            (
                                                                              (
                                                                                view_tuhan_bunkai_shohin.shukadate >= (
                                                                                  (
                                                                                    (
                                                                                      to_char(
                                                                                        add_months(
                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                          (
                                                                                            - (25):: bigint
                                                                                          )
                                                                                        ), 
                                                                                        ('yyyymm' :: character varying):: text
                                                                                      )
                                                                                    ):: integer
                                                                                  ):: character varying
                                                                                ):: text
                                                                              ) 
                                                                              AND (
                                                                                view_tuhan_bunkai_shohin.shukadate <= (
                                                                                  (
                                                                                    (
                                                                                      to_char(
                                                                                        (
                                                                                          last_day(
                                                                                            add_months(
                                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                                              (
                                                                                                - (14):: bigint
                                                                                              )
                                                                                            )
                                                                                          )
                                                                                        ):: timestamp without time zone, 
                                                                                        ('yyyymm' :: character varying):: text
                                                                                      )
                                                                                    ):: integer
                                                                                  ):: character varying
                                                                                ):: text
                                                                              )
                                                                            ) 
                                                                          GROUP BY 
                                                                            view_tuhan_bunkai_shohin.channel, 
                                                                            view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                            view_tuhan_bunkai_shohin.itembunrui, 
                                                                            view_tuhan_bunkai_shohin.juchkbn, 
                                                                            view_tuhan_bunkai_shohin.juchkbncname, 
                                                                            view_tuhan_bunkai_shohin.henreasoncode, 
                                                                            view_tuhan_bunkai_shohin.henreasonname
                                                                        ) 
                                                                        UNION 
                                                                        SELECT 
                                                                          (
                                                                            to_char(
                                                                              add_months(
                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                (
                                                                                  - (13):: bigint
                                                                                )
                                                                              ), 
                                                                              ('yyyymm' :: character varying):: text
                                                                            )
                                                                          ):: integer AS nengetu, 
                                                                          count(
                                                                            DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                          ) AS ninzu, 
                                                                          view_tuhan_bunkai_shohin.channel, 
                                                                          view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                          view_tuhan_bunkai_shohin.itembunrui, 
                                                                          view_tuhan_bunkai_shohin.juchkbn, 
                                                                          (
                                                                            view_tuhan_bunkai_shohin.juchkbncname
                                                                          ):: character varying AS juchkbncname, 
                                                                          view_tuhan_bunkai_shohin.henreasoncode, 
                                                                          (
                                                                            view_tuhan_bunkai_shohin.henreasonname
                                                                          ):: character varying AS henreasonname 
                                                                        FROM 
                                                                          (
                                                                            SELECT 
                                                                              "substring"(
                                                                                (
                                                                                  (h.shukadate):: character varying
                                                                                ):: text, 
                                                                                0, 
                                                                                6
                                                                              ) AS shukadate, 
                                                                              h.kokyano, 
                                                                              CASE WHEN (
                                                                                (
                                                                                  h.kakokbn = (
                                                                                    (1):: numeric
                                                                                  ):: numeric(18, 0)
                                                                                ) 
                                                                                OR (
                                                                                  (h.kakokbn IS NULL) 
                                                                                  AND ('1' IS NULL)
                                                                                )
                                                                              ) THEN CASE WHEN (
                                                                                (h.kaisha):: text = ('000' :: character varying):: text
                                                                              ) THEN '01:通販' :: character varying WHEN (
                                                                                (h.kaisha):: text = ('001' :: character varying):: text
                                                                              ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                                h.smkeiroid = (
                                                                                  (6):: numeric
                                                                                ):: numeric(18, 0)
                                                                              ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                              h.daihanrobunname AS konyuchubuncode, 
                                                                              i.itembunrui, 
                                                                              h.juchkbn, 
                                                                              tm67.cname AS juchkbncname, 
                                                                              h.henreasoncode, 
                                                                              (
                                                                                (
                                                                                  (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                                ) || (h.henreasonname):: text
                                                                              ) AS henreasonname 
                                                                            FROM 
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      saleh_itemstrategy h 
                                                                                      JOIN salem_itemstrategy m ON (
                                                                                        (
                                                                                          (h.saleno):: text = (m.saleno):: text
                                                                                        )
                                                                                      )
                                                                                    ) 
                                                                                    JOIN tm14shkos "k" ON (
                                                                                      (
                                                                                        (m.itemcode):: text = ("k".itemcode):: text
                                                                                      )
                                                                                    )
                                                                                  ) 
                                                                                  JOIN tm39item_strategy i ON (
                                                                                    (
                                                                                      ("k".kosecode):: text = (i.itemcode):: text
                                                                                    )
                                                                                  )
                                                                                ) 
                                                                                LEFT JOIN tm67juch_nm tm67 ON (
                                                                                  (
                                                                                    (h.juchkbn):: text = (tm67.code):: text
                                                                                  )
                                                                                )
                                                                              ) 
                                                                            WHERE 
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            h.shukadate >= (
                                                                                              (
                                                                                                (
                                                                                                  (
                                                                                                    to_char(
                                                                                                      add_months(
                                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                                        (
                                                                                                          - (36):: bigint
                                                                                                        )
                                                                                                      ), 
                                                                                                      ('yyyymm' :: character varying):: text
                                                                                                    ) || ('01' :: character varying):: text
                                                                                                  )
                                                                                                ):: integer
                                                                                              ):: numeric
                                                                                            ):: numeric(18, 0)
                                                                                          ) 
                                                                                          AND (
                                                                                            h.shukadate <= (
                                                                                              (
                                                                                                (
                                                                                                  to_char(
                                                                                                    (
                                                                                                      last_day(
                                                                                                        add_months(
                                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                                          (
                                                                                                            - (1):: bigint
                                                                                                          )
                                                                                                        )
                                                                                                      )
                                                                                                    ):: timestamp without time zone, 
                                                                                                    ('yyyymmdd' :: character varying):: text
                                                                                                  )
                                                                                                ):: integer
                                                                                              ):: numeric
                                                                                            ):: numeric(18, 0)
                                                                                          )
                                                                                        ) 
                                                                                        AND (
                                                                                          h.cancelflg = (
                                                                                            (0):: numeric
                                                                                          ):: numeric(18, 0)
                                                                                        )
                                                                                      ) 
                                                                                      AND (
                                                                                        (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                      )
                                                                                    ) 
                                                                                    AND (
                                                                                      (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                    )
                                                                                  ) 
                                                                                  AND (
                                                                                    (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                  )
                                                                                ) 
                                                                                AND (
                                                                                  (
                                                                                    (
                                                                                      (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                    ) 
                                                                                    OR (
                                                                                      (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                    )
                                                                                  ) 
                                                                                  OR (
                                                                                    (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                  )
                                                                                )
                                                                              )
                                                                          ) view_tuhan_bunkai_shohin 
                                                                        WHERE 
                                                                          (
                                                                            (
                                                                              view_tuhan_bunkai_shohin.shukadate >= (
                                                                                (
                                                                                  (
                                                                                    to_char(
                                                                                      add_months(
                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                        (
                                                                                          - (24):: bigint
                                                                                        )
                                                                                      ), 
                                                                                      ('yyyymm' :: character varying):: text
                                                                                    )
                                                                                  ):: integer
                                                                                ):: character varying
                                                                              ):: text
                                                                            ) 
                                                                            AND (
                                                                              view_tuhan_bunkai_shohin.shukadate <= (
                                                                                (
                                                                                  (
                                                                                    to_char(
                                                                                      (
                                                                                        last_day(
                                                                                          add_months(
                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                            (
                                                                                              - (13):: bigint
                                                                                            )
                                                                                          )
                                                                                        )
                                                                                      ):: timestamp without time zone, 
                                                                                      ('yyyymm' :: character varying):: text
                                                                                    )
                                                                                  ):: integer
                                                                                ):: character varying
                                                                              ):: text
                                                                            )
                                                                          ) 
                                                                        GROUP BY 
                                                                          view_tuhan_bunkai_shohin.channel, 
                                                                          view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                          view_tuhan_bunkai_shohin.itembunrui, 
                                                                          view_tuhan_bunkai_shohin.juchkbn, 
                                                                          view_tuhan_bunkai_shohin.juchkbncname, 
                                                                          view_tuhan_bunkai_shohin.henreasoncode, 
                                                                          view_tuhan_bunkai_shohin.henreasonname
                                                                      ) 
                                                                      UNION 
                                                                      SELECT 
                                                                        (
                                                                          to_char(
                                                                            add_months(
                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                              (
                                                                                - (12):: bigint
                                                                              )
                                                                            ), 
                                                                            ('yyyymm' :: character varying):: text
                                                                          )
                                                                        ):: integer AS nengetu, 
                                                                        count(
                                                                          DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                        ) AS ninzu, 
                                                                        view_tuhan_bunkai_shohin.channel, 
                                                                        view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                        view_tuhan_bunkai_shohin.itembunrui, 
                                                                        view_tuhan_bunkai_shohin.juchkbn, 
                                                                        (
                                                                          view_tuhan_bunkai_shohin.juchkbncname
                                                                        ):: character varying AS juchkbncname, 
                                                                        view_tuhan_bunkai_shohin.henreasoncode, 
                                                                        (
                                                                          view_tuhan_bunkai_shohin.henreasonname
                                                                        ):: character varying AS henreasonname 
                                                                      FROM 
                                                                        (
                                                                          SELECT 
                                                                            "substring"(
                                                                              (
                                                                                (h.shukadate):: character varying
                                                                              ):: text, 
                                                                              0, 
                                                                              6
                                                                            ) AS shukadate, 
                                                                            h.kokyano, 
                                                                            CASE WHEN (
                                                                              (
                                                                                h.kakokbn = (
                                                                                  (1):: numeric
                                                                                ):: numeric(18, 0)
                                                                              ) 
                                                                              OR (
                                                                                (h.kakokbn IS NULL) 
                                                                                AND ('1' IS NULL)
                                                                              )
                                                                            ) THEN CASE WHEN (
                                                                              (h.kaisha):: text = ('000' :: character varying):: text
                                                                            ) THEN '01:通販' :: character varying WHEN (
                                                                              (h.kaisha):: text = ('001' :: character varying):: text
                                                                            ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                              h.smkeiroid = (
                                                                                (6):: numeric
                                                                              ):: numeric(18, 0)
                                                                            ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                            h.daihanrobunname AS konyuchubuncode, 
                                                                            i.itembunrui, 
                                                                            h.juchkbn, 
                                                                            tm67.cname AS juchkbncname, 
                                                                            h.henreasoncode, 
                                                                            (
                                                                              (
                                                                                (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                              ) || (h.henreasonname):: text
                                                                            ) AS henreasonname 
                                                                          FROM 
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    saleh_itemstrategy h 
                                                                                    JOIN salem_itemstrategy m ON (
                                                                                      (
                                                                                        (h.saleno):: text = (m.saleno):: text
                                                                                      )
                                                                                    )
                                                                                  ) 
                                                                                  JOIN tm14shkos "k" ON (
                                                                                    (
                                                                                      (m.itemcode):: text = ("k".itemcode):: text
                                                                                    )
                                                                                  )
                                                                                ) 
                                                                                JOIN tm39item_strategy i ON (
                                                                                  (
                                                                                    ("k".kosecode):: text = (i.itemcode):: text
                                                                                  )
                                                                                )
                                                                              ) 
                                                                              LEFT JOIN tm67juch_nm tm67 ON (
                                                                                (
                                                                                  (h.juchkbn):: text = (tm67.code):: text
                                                                                )
                                                                              )
                                                                            ) 
                                                                          WHERE 
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          h.shukadate >= (
                                                                                            (
                                                                                              (
                                                                                                (
                                                                                                  to_char(
                                                                                                    add_months(
                                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                                      (
                                                                                                        - (36):: bigint
                                                                                                      )
                                                                                                    ), 
                                                                                                    ('yyyymm' :: character varying):: text
                                                                                                  ) || ('01' :: character varying):: text
                                                                                                )
                                                                                              ):: integer
                                                                                            ):: numeric
                                                                                          ):: numeric(18, 0)
                                                                                        ) 
                                                                                        AND (
                                                                                          h.shukadate <= (
                                                                                            (
                                                                                              (
                                                                                                to_char(
                                                                                                  (
                                                                                                    last_day(
                                                                                                      add_months(
                                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                                        (
                                                                                                          - (1):: bigint
                                                                                                        )
                                                                                                      )
                                                                                                    )
                                                                                                  ):: timestamp without time zone, 
                                                                                                  ('yyyymmdd' :: character varying):: text
                                                                                                )
                                                                                              ):: integer
                                                                                            ):: numeric
                                                                                          ):: numeric(18, 0)
                                                                                        )
                                                                                      ) 
                                                                                      AND (
                                                                                        h.cancelflg = (
                                                                                          (0):: numeric
                                                                                        ):: numeric(18, 0)
                                                                                      )
                                                                                    ) 
                                                                                    AND (
                                                                                      (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                    )
                                                                                  ) 
                                                                                  AND (
                                                                                    (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                  )
                                                                                ) 
                                                                                AND (
                                                                                  (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                                )
                                                                              ) 
                                                                              AND (
                                                                                (
                                                                                  (
                                                                                    (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                  ) 
                                                                                  OR (
                                                                                    (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                  )
                                                                                ) 
                                                                                OR (
                                                                                  (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                                )
                                                                              )
                                                                            )
                                                                        ) view_tuhan_bunkai_shohin 
                                                                      WHERE 
                                                                        (
                                                                          (
                                                                            view_tuhan_bunkai_shohin.shukadate >= (
                                                                              (
                                                                                (
                                                                                  to_char(
                                                                                    add_months(
                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                      (
                                                                                        - (23):: bigint
                                                                                      )
                                                                                    ), 
                                                                                    ('yyyymm' :: character varying):: text
                                                                                  )
                                                                                ):: integer
                                                                              ):: character varying
                                                                            ):: text
                                                                          ) 
                                                                          AND (
                                                                            view_tuhan_bunkai_shohin.shukadate <= (
                                                                              (
                                                                                (
                                                                                  to_char(
                                                                                    (
                                                                                      last_day(
                                                                                        add_months(
                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                          (
                                                                                            - (12):: bigint
                                                                                          )
                                                                                        )
                                                                                      )
                                                                                    ):: timestamp without time zone, 
                                                                                    ('yyyymm' :: character varying):: text
                                                                                  )
                                                                                ):: integer
                                                                              ):: character varying
                                                                            ):: text
                                                                          )
                                                                        ) 
                                                                      GROUP BY 
                                                                        view_tuhan_bunkai_shohin.channel, 
                                                                        view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                        view_tuhan_bunkai_shohin.itembunrui, 
                                                                        view_tuhan_bunkai_shohin.juchkbn, 
                                                                        view_tuhan_bunkai_shohin.juchkbncname, 
                                                                        view_tuhan_bunkai_shohin.henreasoncode, 
                                                                        view_tuhan_bunkai_shohin.henreasonname
                                                                    ) 
                                                                    UNION 
                                                                    SELECT 
                                                                      (
                                                                        to_char(
                                                                          add_months(
                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                            (
                                                                              - (11):: bigint
                                                                            )
                                                                          ), 
                                                                          ('yyyymm' :: character varying):: text
                                                                        )
                                                                      ):: integer AS nengetu, 
                                                                      count(
                                                                        DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                      ) AS ninzu, 
                                                                      view_tuhan_bunkai_shohin.channel, 
                                                                      view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                      view_tuhan_bunkai_shohin.itembunrui, 
                                                                      view_tuhan_bunkai_shohin.juchkbn, 
                                                                      (
                                                                        view_tuhan_bunkai_shohin.juchkbncname
                                                                      ):: character varying AS juchkbncname, 
                                                                      view_tuhan_bunkai_shohin.henreasoncode, 
                                                                      (
                                                                        view_tuhan_bunkai_shohin.henreasonname
                                                                      ):: character varying AS henreasonname 
                                                                    FROM 
                                                                      (
                                                                        SELECT 
                                                                          "substring"(
                                                                            (
                                                                              (h.shukadate):: character varying
                                                                            ):: text, 
                                                                            0, 
                                                                            6
                                                                          ) AS shukadate, 
                                                                          h.kokyano, 
                                                                          CASE WHEN (
                                                                            (
                                                                              h.kakokbn = (
                                                                                (1):: numeric
                                                                              ):: numeric(18, 0)
                                                                            ) 
                                                                            OR (
                                                                              (h.kakokbn IS NULL) 
                                                                              AND ('1' IS NULL)
                                                                            )
                                                                          ) THEN CASE WHEN (
                                                                            (h.kaisha):: text = ('000' :: character varying):: text
                                                                          ) THEN '01:通販' :: character varying WHEN (
                                                                            (h.kaisha):: text = ('001' :: character varying):: text
                                                                          ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                            h.smkeiroid = (
                                                                              (6):: numeric
                                                                            ):: numeric(18, 0)
                                                                          ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                          h.daihanrobunname AS konyuchubuncode, 
                                                                          i.itembunrui, 
                                                                          h.juchkbn, 
                                                                          tm67.cname AS juchkbncname, 
                                                                          h.henreasoncode, 
                                                                          (
                                                                            (
                                                                              (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                            ) || (h.henreasonname):: text
                                                                          ) AS henreasonname 
                                                                        FROM 
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  saleh_itemstrategy h 
                                                                                  JOIN salem_itemstrategy m ON (
                                                                                    (
                                                                                      (h.saleno):: text = (m.saleno):: text
                                                                                    )
                                                                                  )
                                                                                ) 
                                                                                JOIN tm14shkos "k" ON (
                                                                                  (
                                                                                    (m.itemcode):: text = ("k".itemcode):: text
                                                                                  )
                                                                                )
                                                                              ) 
                                                                              JOIN tm39item_strategy i ON (
                                                                                (
                                                                                  ("k".kosecode):: text = (i.itemcode):: text
                                                                                )
                                                                              )
                                                                            ) 
                                                                            LEFT JOIN tm67juch_nm tm67 ON (
                                                                              (
                                                                                (h.juchkbn):: text = (tm67.code):: text
                                                                              )
                                                                            )
                                                                          ) 
                                                                        WHERE 
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        h.shukadate >= (
                                                                                          (
                                                                                            (
                                                                                              (
                                                                                                to_char(
                                                                                                  add_months(
                                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                                    (
                                                                                                      - (36):: bigint
                                                                                                    )
                                                                                                  ), 
                                                                                                  ('yyyymm' :: character varying):: text
                                                                                                ) || ('01' :: character varying):: text
                                                                                              )
                                                                                            ):: integer
                                                                                          ):: numeric
                                                                                        ):: numeric(18, 0)
                                                                                      ) 
                                                                                      AND (
                                                                                        h.shukadate <= (
                                                                                          (
                                                                                            (
                                                                                              to_char(
                                                                                                (
                                                                                                  last_day(
                                                                                                    add_months(
                                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                                      (
                                                                                                        - (1):: bigint
                                                                                                      )
                                                                                                    )
                                                                                                  )
                                                                                                ):: timestamp without time zone, 
                                                                                                ('yyyymmdd' :: character varying):: text
                                                                                              )
                                                                                            ):: integer
                                                                                          ):: numeric
                                                                                        ):: numeric(18, 0)
                                                                                      )
                                                                                    ) 
                                                                                    AND (
                                                                                      h.cancelflg = (
                                                                                        (0):: numeric
                                                                                      ):: numeric(18, 0)
                                                                                    )
                                                                                  ) 
                                                                                  AND (
                                                                                    (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                  )
                                                                                ) 
                                                                                AND (
                                                                                  (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                                )
                                                                              ) 
                                                                              AND (
                                                                                (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                              )
                                                                            ) 
                                                                            AND (
                                                                              (
                                                                                (
                                                                                  (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                                ) 
                                                                                OR (
                                                                                  (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                                )
                                                                              ) 
                                                                              OR (
                                                                                (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                              )
                                                                            )
                                                                          )
                                                                      ) view_tuhan_bunkai_shohin 
                                                                    WHERE 
                                                                      (
                                                                        (
                                                                          view_tuhan_bunkai_shohin.shukadate >= (
                                                                            (
                                                                              (
                                                                                to_char(
                                                                                  add_months(
                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                    (
                                                                                      - (22):: bigint
                                                                                    )
                                                                                  ), 
                                                                                  ('yyyymm' :: character varying):: text
                                                                                )
                                                                              ):: integer
                                                                            ):: character varying
                                                                          ):: text
                                                                        ) 
                                                                        AND (
                                                                          view_tuhan_bunkai_shohin.shukadate <= (
                                                                            (
                                                                              (
                                                                                to_char(
                                                                                  (
                                                                                    last_day(
                                                                                      add_months(
                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                        (
                                                                                          - (11):: bigint
                                                                                        )
                                                                                      )
                                                                                    )
                                                                                  ):: timestamp without time zone, 
                                                                                  ('yyyymm' :: character varying):: text
                                                                                )
                                                                              ):: integer
                                                                            ):: character varying
                                                                          ):: text
                                                                        )
                                                                      ) 
                                                                    GROUP BY 
                                                                      view_tuhan_bunkai_shohin.channel, 
                                                                      view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                      view_tuhan_bunkai_shohin.itembunrui, 
                                                                      view_tuhan_bunkai_shohin.juchkbn, 
                                                                      view_tuhan_bunkai_shohin.juchkbncname, 
                                                                      view_tuhan_bunkai_shohin.henreasoncode, 
                                                                      view_tuhan_bunkai_shohin.henreasonname
                                                                  ) 
                                                                  UNION 
                                                                  SELECT 
                                                                    (
                                                                      to_char(
                                                                        add_months(
                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                          (
                                                                            - (10):: bigint
                                                                          )
                                                                        ), 
                                                                        ('yyyymm' :: character varying):: text
                                                                      )
                                                                    ):: integer AS nengetu, 
                                                                    count(
                                                                      DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                    ) AS ninzu, 
                                                                    view_tuhan_bunkai_shohin.channel, 
                                                                    view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                    view_tuhan_bunkai_shohin.itembunrui, 
                                                                    view_tuhan_bunkai_shohin.juchkbn, 
                                                                    (
                                                                      view_tuhan_bunkai_shohin.juchkbncname
                                                                    ):: character varying AS juchkbncname, 
                                                                    view_tuhan_bunkai_shohin.henreasoncode, 
                                                                    (
                                                                      view_tuhan_bunkai_shohin.henreasonname
                                                                    ):: character varying AS henreasonname 
                                                                  FROM 
                                                                    (
                                                                      SELECT 
                                                                        "substring"(
                                                                          (
                                                                            (h.shukadate):: character varying
                                                                          ):: text, 
                                                                          0, 
                                                                          6
                                                                        ) AS shukadate, 
                                                                        h.kokyano, 
                                                                        CASE WHEN (
                                                                          (
                                                                            h.kakokbn = (
                                                                              (1):: numeric
                                                                            ):: numeric(18, 0)
                                                                          ) 
                                                                          OR (
                                                                            (h.kakokbn IS NULL) 
                                                                            AND ('1' IS NULL)
                                                                          )
                                                                        ) THEN CASE WHEN (
                                                                          (h.kaisha):: text = ('000' :: character varying):: text
                                                                        ) THEN '01:通販' :: character varying WHEN (
                                                                          (h.kaisha):: text = ('001' :: character varying):: text
                                                                        ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                          h.smkeiroid = (
                                                                            (6):: numeric
                                                                          ):: numeric(18, 0)
                                                                        ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                        h.daihanrobunname AS konyuchubuncode, 
                                                                        i.itembunrui, 
                                                                        h.juchkbn, 
                                                                        tm67.cname AS juchkbncname, 
                                                                        h.henreasoncode, 
                                                                        (
                                                                          (
                                                                            (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                          ) || (h.henreasonname):: text
                                                                        ) AS henreasonname 
                                                                      FROM 
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                saleh_itemstrategy h 
                                                                                JOIN salem_itemstrategy m ON (
                                                                                  (
                                                                                    (h.saleno):: text = (m.saleno):: text
                                                                                  )
                                                                                )
                                                                              ) 
                                                                              JOIN tm14shkos "k" ON (
                                                                                (
                                                                                  (m.itemcode):: text = ("k".itemcode):: text
                                                                                )
                                                                              )
                                                                            ) 
                                                                            JOIN tm39item_strategy i ON (
                                                                              (
                                                                                ("k".kosecode):: text = (i.itemcode):: text
                                                                              )
                                                                            )
                                                                          ) 
                                                                          LEFT JOIN tm67juch_nm tm67 ON (
                                                                            (
                                                                              (h.juchkbn):: text = (tm67.code):: text
                                                                            )
                                                                          )
                                                                        ) 
                                                                      WHERE 
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      h.shukadate >= (
                                                                                        (
                                                                                          (
                                                                                            (
                                                                                              to_char(
                                                                                                add_months(
                                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                                  (
                                                                                                    - (36):: bigint
                                                                                                  )
                                                                                                ), 
                                                                                                ('yyyymm' :: character varying):: text
                                                                                              ) || ('01' :: character varying):: text
                                                                                            )
                                                                                          ):: integer
                                                                                        ):: numeric
                                                                                      ):: numeric(18, 0)
                                                                                    ) 
                                                                                    AND (
                                                                                      h.shukadate <= (
                                                                                        (
                                                                                          (
                                                                                            to_char(
                                                                                              (
                                                                                                last_day(
                                                                                                  add_months(
                                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                                    (
                                                                                                      - (1):: bigint
                                                                                                    )
                                                                                                  )
                                                                                                )
                                                                                              ):: timestamp without time zone, 
                                                                                              ('yyyymmdd' :: character varying):: text
                                                                                            )
                                                                                          ):: integer
                                                                                        ):: numeric
                                                                                      ):: numeric(18, 0)
                                                                                    )
                                                                                  ) 
                                                                                  AND (
                                                                                    h.cancelflg = (
                                                                                      (0):: numeric
                                                                                    ):: numeric(18, 0)
                                                                                  )
                                                                                ) 
                                                                                AND (
                                                                                  (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                                )
                                                                              ) 
                                                                              AND (
                                                                                (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                              )
                                                                            ) 
                                                                            AND (
                                                                              (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                            )
                                                                          ) 
                                                                          AND (
                                                                            (
                                                                              (
                                                                                (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                              ) 
                                                                              OR (
                                                                                (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                              )
                                                                            ) 
                                                                            OR (
                                                                              (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                            )
                                                                          )
                                                                        )
                                                                    ) view_tuhan_bunkai_shohin 
                                                                  WHERE 
                                                                    (
                                                                      (
                                                                        view_tuhan_bunkai_shohin.shukadate >= (
                                                                          (
                                                                            (
                                                                              to_char(
                                                                                add_months(
                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                  (
                                                                                    - (21):: bigint
                                                                                  )
                                                                                ), 
                                                                                ('yyyymm' :: character varying):: text
                                                                              )
                                                                            ):: integer
                                                                          ):: character varying
                                                                        ):: text
                                                                      ) 
                                                                      AND (
                                                                        view_tuhan_bunkai_shohin.shukadate <= (
                                                                          (
                                                                            (
                                                                              to_char(
                                                                                (
                                                                                  last_day(
                                                                                    add_months(
                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                      (
                                                                                        - (10):: bigint
                                                                                      )
                                                                                    )
                                                                                  )
                                                                                ):: timestamp without time zone, 
                                                                                ('yyyymm' :: character varying):: text
                                                                              )
                                                                            ):: integer
                                                                          ):: character varying
                                                                        ):: text
                                                                      )
                                                                    ) 
                                                                  GROUP BY 
                                                                    view_tuhan_bunkai_shohin.channel, 
                                                                    view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                    view_tuhan_bunkai_shohin.itembunrui, 
                                                                    view_tuhan_bunkai_shohin.juchkbn, 
                                                                    view_tuhan_bunkai_shohin.juchkbncname, 
                                                                    view_tuhan_bunkai_shohin.henreasoncode, 
                                                                    view_tuhan_bunkai_shohin.henreasonname
                                                                ) 
                                                                UNION 
                                                                SELECT 
                                                                  (
                                                                    to_char(
                                                                      add_months(
                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                        (
                                                                          - (9):: bigint
                                                                        )
                                                                      ), 
                                                                      ('yyyymm' :: character varying):: text
                                                                    )
                                                                  ):: integer AS nengetu, 
                                                                  count(
                                                                    DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                  ) AS ninzu, 
                                                                  view_tuhan_bunkai_shohin.channel, 
                                                                  view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                  view_tuhan_bunkai_shohin.itembunrui, 
                                                                  view_tuhan_bunkai_shohin.juchkbn, 
                                                                  (
                                                                    view_tuhan_bunkai_shohin.juchkbncname
                                                                  ):: character varying AS juchkbncname, 
                                                                  view_tuhan_bunkai_shohin.henreasoncode, 
                                                                  (
                                                                    view_tuhan_bunkai_shohin.henreasonname
                                                                  ):: character varying AS henreasonname 
                                                                FROM 
                                                                  (
                                                                    SELECT 
                                                                      "substring"(
                                                                        (
                                                                          (h.shukadate):: character varying
                                                                        ):: text, 
                                                                        0, 
                                                                        6
                                                                      ) AS shukadate, 
                                                                      h.kokyano, 
                                                                      CASE WHEN (
                                                                        (
                                                                          h.kakokbn = (
                                                                            (1):: numeric
                                                                          ):: numeric(18, 0)
                                                                        ) 
                                                                        OR (
                                                                          (h.kakokbn IS NULL) 
                                                                          AND ('1' IS NULL)
                                                                        )
                                                                      ) THEN CASE WHEN (
                                                                        (h.kaisha):: text = ('000' :: character varying):: text
                                                                      ) THEN '01:通販' :: character varying WHEN (
                                                                        (h.kaisha):: text = ('001' :: character varying):: text
                                                                      ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                        h.smkeiroid = (
                                                                          (6):: numeric
                                                                        ):: numeric(18, 0)
                                                                      ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                      h.daihanrobunname AS konyuchubuncode, 
                                                                      i.itembunrui, 
                                                                      h.juchkbn, 
                                                                      tm67.cname AS juchkbncname, 
                                                                      h.henreasoncode, 
                                                                      (
                                                                        (
                                                                          (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                        ) || (h.henreasonname):: text
                                                                      ) AS henreasonname 
                                                                    FROM 
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              saleh_itemstrategy h 
                                                                              JOIN salem_itemstrategy m ON (
                                                                                (
                                                                                  (h.saleno):: text = (m.saleno):: text
                                                                                )
                                                                              )
                                                                            ) 
                                                                            JOIN tm14shkos "k" ON (
                                                                              (
                                                                                (m.itemcode):: text = ("k".itemcode):: text
                                                                              )
                                                                            )
                                                                          ) 
                                                                          JOIN tm39item_strategy i ON (
                                                                            (
                                                                              ("k".kosecode):: text = (i.itemcode):: text
                                                                            )
                                                                          )
                                                                        ) 
                                                                        LEFT JOIN tm67juch_nm tm67 ON (
                                                                          (
                                                                            (h.juchkbn):: text = (tm67.code):: text
                                                                          )
                                                                        )
                                                                      ) 
                                                                    WHERE 
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    h.shukadate >= (
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            to_char(
                                                                                              add_months(
                                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                                (
                                                                                                  - (36):: bigint
                                                                                                )
                                                                                              ), 
                                                                                              ('yyyymm' :: character varying):: text
                                                                                            ) || ('01' :: character varying):: text
                                                                                          )
                                                                                        ):: integer
                                                                                      ):: numeric
                                                                                    ):: numeric(18, 0)
                                                                                  ) 
                                                                                  AND (
                                                                                    h.shukadate <= (
                                                                                      (
                                                                                        (
                                                                                          to_char(
                                                                                            (
                                                                                              last_day(
                                                                                                add_months(
                                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                                  (
                                                                                                    - (1):: bigint
                                                                                                  )
                                                                                                )
                                                                                              )
                                                                                            ):: timestamp without time zone, 
                                                                                            ('yyyymmdd' :: character varying):: text
                                                                                          )
                                                                                        ):: integer
                                                                                      ):: numeric
                                                                                    ):: numeric(18, 0)
                                                                                  )
                                                                                ) 
                                                                                AND (
                                                                                  h.cancelflg = (
                                                                                    (0):: numeric
                                                                                  ):: numeric(18, 0)
                                                                                )
                                                                              ) 
                                                                              AND (
                                                                                (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                              )
                                                                            ) 
                                                                            AND (
                                                                              (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                            )
                                                                          ) 
                                                                          AND (
                                                                            (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                          )
                                                                        ) 
                                                                        AND (
                                                                          (
                                                                            (
                                                                              (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                            ) 
                                                                            OR (
                                                                              (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                            )
                                                                          ) 
                                                                          OR (
                                                                            (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                          )
                                                                        )
                                                                      )
                                                                  ) view_tuhan_bunkai_shohin 
                                                                WHERE 
                                                                  (
                                                                    (
                                                                      view_tuhan_bunkai_shohin.shukadate >= (
                                                                        (
                                                                          (
                                                                            to_char(
                                                                              add_months(
                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                (
                                                                                  - (20):: bigint
                                                                                )
                                                                              ), 
                                                                              ('yyyymm' :: character varying):: text
                                                                            )
                                                                          ):: integer
                                                                        ):: character varying
                                                                      ):: text
                                                                    ) 
                                                                    AND (
                                                                      view_tuhan_bunkai_shohin.shukadate <= (
                                                                        (
                                                                          (
                                                                            to_char(
                                                                              (
                                                                                last_day(
                                                                                  add_months(
                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                    (
                                                                                      - (9):: bigint
                                                                                    )
                                                                                  )
                                                                                )
                                                                              ):: timestamp without time zone, 
                                                                              ('yyyymm' :: character varying):: text
                                                                            )
                                                                          ):: integer
                                                                        ):: character varying
                                                                      ):: text
                                                                    )
                                                                  ) 
                                                                GROUP BY 
                                                                  view_tuhan_bunkai_shohin.channel, 
                                                                  view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                  view_tuhan_bunkai_shohin.itembunrui, 
                                                                  view_tuhan_bunkai_shohin.juchkbn, 
                                                                  view_tuhan_bunkai_shohin.juchkbncname, 
                                                                  view_tuhan_bunkai_shohin.henreasoncode, 
                                                                  view_tuhan_bunkai_shohin.henreasonname
                                                              ) 
                                                              UNION 
                                                              SELECT 
                                                                (
                                                                  to_char(
                                                                    add_months(
                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                      (
                                                                        - (8):: bigint
                                                                      )
                                                                    ), 
                                                                    ('yyyymm' :: character varying):: text
                                                                  )
                                                                ):: integer AS nengetu, 
                                                                count(
                                                                  DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                                ) AS ninzu, 
                                                                view_tuhan_bunkai_shohin.channel, 
                                                                view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                view_tuhan_bunkai_shohin.itembunrui, 
                                                                view_tuhan_bunkai_shohin.juchkbn, 
                                                                (
                                                                  view_tuhan_bunkai_shohin.juchkbncname
                                                                ):: character varying AS juchkbncname, 
                                                                view_tuhan_bunkai_shohin.henreasoncode, 
                                                                (
                                                                  view_tuhan_bunkai_shohin.henreasonname
                                                                ):: character varying AS henreasonname 
                                                              FROM 
                                                                (
                                                                  SELECT 
                                                                    "substring"(
                                                                      (
                                                                        (h.shukadate):: character varying
                                                                      ):: text, 
                                                                      0, 
                                                                      6
                                                                    ) AS shukadate, 
                                                                    h.kokyano, 
                                                                    CASE WHEN (
                                                                      (
                                                                        h.kakokbn = (
                                                                          (1):: numeric
                                                                        ):: numeric(18, 0)
                                                                      ) 
                                                                      OR (
                                                                        (h.kakokbn IS NULL) 
                                                                        AND ('1' IS NULL)
                                                                      )
                                                                    ) THEN CASE WHEN (
                                                                      (h.kaisha):: text = ('000' :: character varying):: text
                                                                    ) THEN '01:通販' :: character varying WHEN (
                                                                      (h.kaisha):: text = ('001' :: character varying):: text
                                                                    ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                      h.smkeiroid = (
                                                                        (6):: numeric
                                                                      ):: numeric(18, 0)
                                                                    ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                    h.daihanrobunname AS konyuchubuncode, 
                                                                    i.itembunrui, 
                                                                    h.juchkbn, 
                                                                    tm67.cname AS juchkbncname, 
                                                                    h.henreasoncode, 
                                                                    (
                                                                      (
                                                                        (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                      ) || (h.henreasonname):: text
                                                                    ) AS henreasonname 
                                                                  FROM 
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            saleh_itemstrategy h 
                                                                            JOIN salem_itemstrategy m ON (
                                                                              (
                                                                                (h.saleno):: text = (m.saleno):: text
                                                                              )
                                                                            )
                                                                          ) 
                                                                          JOIN tm14shkos "k" ON (
                                                                            (
                                                                              (m.itemcode):: text = ("k".itemcode):: text
                                                                            )
                                                                          )
                                                                        ) 
                                                                        JOIN tm39item_strategy i ON (
                                                                          (
                                                                            ("k".kosecode):: text = (i.itemcode):: text
                                                                          )
                                                                        )
                                                                      ) 
                                                                      LEFT JOIN tm67juch_nm tm67 ON (
                                                                        (
                                                                          (h.juchkbn):: text = (tm67.code):: text
                                                                        )
                                                                      )
                                                                    ) 
                                                                  WHERE 
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  h.shukadate >= (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          to_char(
                                                                                            add_months(
                                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                                              (
                                                                                                - (36):: bigint
                                                                                              )
                                                                                            ), 
                                                                                            ('yyyymm' :: character varying):: text
                                                                                          ) || ('01' :: character varying):: text
                                                                                        )
                                                                                      ):: integer
                                                                                    ):: numeric
                                                                                  ):: numeric(18, 0)
                                                                                ) 
                                                                                AND (
                                                                                  h.shukadate <= (
                                                                                    (
                                                                                      (
                                                                                        to_char(
                                                                                          (
                                                                                            last_day(
                                                                                              add_months(
                                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                                (
                                                                                                  - (1):: bigint
                                                                                                )
                                                                                              )
                                                                                            )
                                                                                          ):: timestamp without time zone, 
                                                                                          ('yyyymmdd' :: character varying):: text
                                                                                        )
                                                                                      ):: integer
                                                                                    ):: numeric
                                                                                  ):: numeric(18, 0)
                                                                                )
                                                                              ) 
                                                                              AND (
                                                                                h.cancelflg = (
                                                                                  (0):: numeric
                                                                                ):: numeric(18, 0)
                                                                              )
                                                                            ) 
                                                                            AND (
                                                                              (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                            )
                                                                          ) 
                                                                          AND (
                                                                            (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                          )
                                                                        ) 
                                                                        AND (
                                                                          (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                        )
                                                                      ) 
                                                                      AND (
                                                                        (
                                                                          (
                                                                            (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                          ) 
                                                                          OR (
                                                                            (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                          )
                                                                        ) 
                                                                        OR (
                                                                          (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                        )
                                                                      )
                                                                    )
                                                                ) view_tuhan_bunkai_shohin 
                                                              WHERE 
                                                                (
                                                                  (
                                                                    view_tuhan_bunkai_shohin.shukadate >= (
                                                                      (
                                                                        (
                                                                          to_char(
                                                                            add_months(
                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                              (
                                                                                - (19):: bigint
                                                                              )
                                                                            ), 
                                                                            ('yyyymm' :: character varying):: text
                                                                          )
                                                                        ):: integer
                                                                      ):: character varying
                                                                    ):: text
                                                                  ) 
                                                                  AND (
                                                                    view_tuhan_bunkai_shohin.shukadate <= (
                                                                      (
                                                                        (
                                                                          to_char(
                                                                            (
                                                                              last_day(
                                                                                add_months(
                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                  (
                                                                                    - (8):: bigint
                                                                                  )
                                                                                )
                                                                              )
                                                                            ):: timestamp without time zone, 
                                                                            ('yyyymm' :: character varying):: text
                                                                          )
                                                                        ):: integer
                                                                      ):: character varying
                                                                    ):: text
                                                                  )
                                                                ) 
                                                              GROUP BY 
                                                                view_tuhan_bunkai_shohin.channel, 
                                                                view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                                view_tuhan_bunkai_shohin.itembunrui, 
                                                                view_tuhan_bunkai_shohin.juchkbn, 
                                                                view_tuhan_bunkai_shohin.juchkbncname, 
                                                                view_tuhan_bunkai_shohin.henreasoncode, 
                                                                view_tuhan_bunkai_shohin.henreasonname
                                                            ) 
                                                            UNION 
                                                            SELECT 
                                                              (
                                                                to_char(
                                                                  add_months(
                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                    (
                                                                      - (7):: bigint
                                                                    )
                                                                  ), 
                                                                  ('yyyymm' :: character varying):: text
                                                                )
                                                              ):: integer AS nengetu, 
                                                              count(
                                                                DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                              ) AS ninzu, 
                                                              view_tuhan_bunkai_shohin.channel, 
                                                              view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                              view_tuhan_bunkai_shohin.itembunrui, 
                                                              view_tuhan_bunkai_shohin.juchkbn, 
                                                              (
                                                                view_tuhan_bunkai_shohin.juchkbncname
                                                              ):: character varying AS juchkbncname, 
                                                              view_tuhan_bunkai_shohin.henreasoncode, 
                                                              (
                                                                view_tuhan_bunkai_shohin.henreasonname
                                                              ):: character varying AS henreasonname 
                                                            FROM 
                                                              (
                                                                SELECT 
                                                                  "substring"(
                                                                    (
                                                                      (h.shukadate):: character varying
                                                                    ):: text, 
                                                                    0, 
                                                                    6
                                                                  ) AS shukadate, 
                                                                  h.kokyano, 
                                                                  CASE WHEN (
                                                                    (
                                                                      h.kakokbn = (
                                                                        (1):: numeric
                                                                      ):: numeric(18, 0)
                                                                    ) 
                                                                    OR (
                                                                      (h.kakokbn IS NULL) 
                                                                      AND ('1' IS NULL)
                                                                    )
                                                                  ) THEN CASE WHEN (
                                                                    (h.kaisha):: text = ('000' :: character varying):: text
                                                                  ) THEN '01:通販' :: character varying WHEN (
                                                                    (h.kaisha):: text = ('001' :: character varying):: text
                                                                  ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                    h.smkeiroid = (
                                                                      (6):: numeric
                                                                    ):: numeric(18, 0)
                                                                  ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                  h.daihanrobunname AS konyuchubuncode, 
                                                                  i.itembunrui, 
                                                                  h.juchkbn, 
                                                                  tm67.cname AS juchkbncname, 
                                                                  h.henreasoncode, 
                                                                  (
                                                                    (
                                                                      (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                    ) || (h.henreasonname):: text
                                                                  ) AS henreasonname 
                                                                FROM 
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          saleh_itemstrategy h 
                                                                          JOIN salem_itemstrategy m ON (
                                                                            (
                                                                              (h.saleno):: text = (m.saleno):: text
                                                                            )
                                                                          )
                                                                        ) 
                                                                        JOIN tm14shkos "k" ON (
                                                                          (
                                                                            (m.itemcode):: text = ("k".itemcode):: text
                                                                          )
                                                                        )
                                                                      ) 
                                                                      JOIN tm39item_strategy i ON (
                                                                        (
                                                                          ("k".kosecode):: text = (i.itemcode):: text
                                                                        )
                                                                      )
                                                                    ) 
                                                                    LEFT JOIN tm67juch_nm tm67 ON (
                                                                      (
                                                                        (h.juchkbn):: text = (tm67.code):: text
                                                                      )
                                                                    )
                                                                  ) 
                                                                WHERE 
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                h.shukadate >= (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        to_char(
                                                                                          add_months(
                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                            (
                                                                                              - (36):: bigint
                                                                                            )
                                                                                          ), 
                                                                                          ('yyyymm' :: character varying):: text
                                                                                        ) || ('01' :: character varying):: text
                                                                                      )
                                                                                    ):: integer
                                                                                  ):: numeric
                                                                                ):: numeric(18, 0)
                                                                              ) 
                                                                              AND (
                                                                                h.shukadate <= (
                                                                                  (
                                                                                    (
                                                                                      to_char(
                                                                                        (
                                                                                          last_day(
                                                                                            add_months(
                                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                                              (
                                                                                                - (1):: bigint
                                                                                              )
                                                                                            )
                                                                                          )
                                                                                        ):: timestamp without time zone, 
                                                                                        ('yyyymmdd' :: character varying):: text
                                                                                      )
                                                                                    ):: integer
                                                                                  ):: numeric
                                                                                ):: numeric(18, 0)
                                                                              )
                                                                            ) 
                                                                            AND (
                                                                              h.cancelflg = (
                                                                                (0):: numeric
                                                                              ):: numeric(18, 0)
                                                                            )
                                                                          ) 
                                                                          AND (
                                                                            (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                          )
                                                                        ) 
                                                                        AND (
                                                                          (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                        )
                                                                      ) 
                                                                      AND (
                                                                        (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                      )
                                                                    ) 
                                                                    AND (
                                                                      (
                                                                        (
                                                                          (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                        ) 
                                                                        OR (
                                                                          (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                        )
                                                                      ) 
                                                                      OR (
                                                                        (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                      )
                                                                    )
                                                                  )
                                                              ) view_tuhan_bunkai_shohin 
                                                            WHERE 
                                                              (
                                                                (
                                                                  view_tuhan_bunkai_shohin.shukadate >= (
                                                                    (
                                                                      (
                                                                        to_char(
                                                                          add_months(
                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                            (
                                                                              - (18):: bigint
                                                                            )
                                                                          ), 
                                                                          ('yyyymm' :: character varying):: text
                                                                        )
                                                                      ):: integer
                                                                    ):: character varying
                                                                  ):: text
                                                                ) 
                                                                AND (
                                                                  view_tuhan_bunkai_shohin.shukadate <= (
                                                                    (
                                                                      (
                                                                        to_char(
                                                                          (
                                                                            last_day(
                                                                              add_months(
                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                (
                                                                                  - (7):: bigint
                                                                                )
                                                                              )
                                                                            )
                                                                          ):: timestamp without time zone, 
                                                                          ('yyyymm' :: character varying):: text
                                                                        )
                                                                      ):: integer
                                                                    ):: character varying
                                                                  ):: text
                                                                )
                                                              ) 
                                                            GROUP BY 
                                                              view_tuhan_bunkai_shohin.channel, 
                                                              view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                              view_tuhan_bunkai_shohin.itembunrui, 
                                                              view_tuhan_bunkai_shohin.juchkbn, 
                                                              view_tuhan_bunkai_shohin.juchkbncname, 
                                                              view_tuhan_bunkai_shohin.henreasoncode, 
                                                              view_tuhan_bunkai_shohin.henreasonname
                                                          ) 
                                                          UNION 
                                                          SELECT 
                                                            (
                                                              to_char(
                                                                add_months(
                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                  (
                                                                    - (6):: bigint
                                                                  )
                                                                ), 
                                                                ('yyyymm' :: character varying):: text
                                                              )
                                                            ):: integer AS nengetu, 
                                                            count(
                                                              DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                            ) AS ninzu, 
                                                            view_tuhan_bunkai_shohin.channel, 
                                                            view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                            view_tuhan_bunkai_shohin.itembunrui, 
                                                            view_tuhan_bunkai_shohin.juchkbn, 
                                                            (
                                                              view_tuhan_bunkai_shohin.juchkbncname
                                                            ):: character varying AS juchkbncname, 
                                                            view_tuhan_bunkai_shohin.henreasoncode, 
                                                            (
                                                              view_tuhan_bunkai_shohin.henreasonname
                                                            ):: character varying AS henreasonname 
                                                          FROM 
                                                            (
                                                              SELECT 
                                                                "substring"(
                                                                  (
                                                                    (h.shukadate):: character varying
                                                                  ):: text, 
                                                                  0, 
                                                                  6
                                                                ) AS shukadate, 
                                                                h.kokyano, 
                                                                CASE WHEN (
                                                                  (
                                                                    h.kakokbn = (
                                                                      (1):: numeric
                                                                    ):: numeric(18, 0)
                                                                  ) 
                                                                  OR (
                                                                    (h.kakokbn IS NULL) 
                                                                    AND ('1' IS NULL)
                                                                  )
                                                                ) THEN CASE WHEN (
                                                                  (h.kaisha):: text = ('000' :: character varying):: text
                                                                ) THEN '01:通販' :: character varying WHEN (
                                                                  (h.kaisha):: text = ('001' :: character varying):: text
                                                                ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                  h.smkeiroid = (
                                                                    (6):: numeric
                                                                  ):: numeric(18, 0)
                                                                ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                                h.daihanrobunname AS konyuchubuncode, 
                                                                i.itembunrui, 
                                                                h.juchkbn, 
                                                                tm67.cname AS juchkbncname, 
                                                                h.henreasoncode, 
                                                                (
                                                                  (
                                                                    (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                  ) || (h.henreasonname):: text
                                                                ) AS henreasonname 
                                                              FROM 
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        saleh_itemstrategy h 
                                                                        JOIN salem_itemstrategy m ON (
                                                                          (
                                                                            (h.saleno):: text = (m.saleno):: text
                                                                          )
                                                                        )
                                                                      ) 
                                                                      JOIN tm14shkos "k" ON (
                                                                        (
                                                                          (m.itemcode):: text = ("k".itemcode):: text
                                                                        )
                                                                      )
                                                                    ) 
                                                                    JOIN tm39item_strategy i ON (
                                                                      (
                                                                        ("k".kosecode):: text = (i.itemcode):: text
                                                                      )
                                                                    )
                                                                  ) 
                                                                  LEFT JOIN tm67juch_nm tm67 ON (
                                                                    (
                                                                      (h.juchkbn):: text = (tm67.code):: text
                                                                    )
                                                                  )
                                                                ) 
                                                              WHERE 
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              h.shukadate >= (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      to_char(
                                                                                        add_months(
                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                          (
                                                                                            - (36):: bigint
                                                                                          )
                                                                                        ), 
                                                                                        ('yyyymm' :: character varying):: text
                                                                                      ) || ('01' :: character varying):: text
                                                                                    )
                                                                                  ):: integer
                                                                                ):: numeric
                                                                              ):: numeric(18, 0)
                                                                            ) 
                                                                            AND (
                                                                              h.shukadate <= (
                                                                                (
                                                                                  (
                                                                                    to_char(
                                                                                      (
                                                                                        last_day(
                                                                                          add_months(
                                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                                            (
                                                                                              - (1):: bigint
                                                                                            )
                                                                                          )
                                                                                        )
                                                                                      ):: timestamp without time zone, 
                                                                                      ('yyyymmdd' :: character varying):: text
                                                                                    )
                                                                                  ):: integer
                                                                                ):: numeric
                                                                              ):: numeric(18, 0)
                                                                            )
                                                                          ) 
                                                                          AND (
                                                                            h.cancelflg = (
                                                                              (0):: numeric
                                                                            ):: numeric(18, 0)
                                                                          )
                                                                        ) 
                                                                        AND (
                                                                          (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                        )
                                                                      ) 
                                                                      AND (
                                                                        (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                      )
                                                                    ) 
                                                                    AND (
                                                                      (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                    )
                                                                  ) 
                                                                  AND (
                                                                    (
                                                                      (
                                                                        (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                      ) 
                                                                      OR (
                                                                        (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                      )
                                                                    ) 
                                                                    OR (
                                                                      (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                    )
                                                                  )
                                                                )
                                                            ) view_tuhan_bunkai_shohin 
                                                          WHERE 
                                                            (
                                                              (
                                                                view_tuhan_bunkai_shohin.shukadate >= (
                                                                  (
                                                                    (
                                                                      to_char(
                                                                        add_months(
                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                          (
                                                                            - (17):: bigint
                                                                          )
                                                                        ), 
                                                                        ('yyyymm' :: character varying):: text
                                                                      )
                                                                    ):: integer
                                                                  ):: character varying
                                                                ):: text
                                                              ) 
                                                              AND (
                                                                view_tuhan_bunkai_shohin.shukadate <= (
                                                                  (
                                                                    (
                                                                      to_char(
                                                                        (
                                                                          last_day(
                                                                            add_months(
                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                              (
                                                                                - (6):: bigint
                                                                              )
                                                                            )
                                                                          )
                                                                        ):: timestamp without time zone, 
                                                                        ('yyyymm' :: character varying):: text
                                                                      )
                                                                    ):: integer
                                                                  ):: character varying
                                                                ):: text
                                                              )
                                                            ) 
                                                          GROUP BY 
                                                            view_tuhan_bunkai_shohin.channel, 
                                                            view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                            view_tuhan_bunkai_shohin.itembunrui, 
                                                            view_tuhan_bunkai_shohin.juchkbn, 
                                                            view_tuhan_bunkai_shohin.juchkbncname, 
                                                            view_tuhan_bunkai_shohin.henreasoncode, 
                                                            view_tuhan_bunkai_shohin.henreasonname
                                                        ) 
                                                        UNION 
                                                        SELECT 
                                                          (
                                                            to_char(
                                                              add_months(
                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                (
                                                                  - (5):: bigint
                                                                )
                                                              ), 
                                                              ('yyyymm' :: character varying):: text
                                                            )
                                                          ):: integer AS nengetu, 
                                                          count(
                                                            DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                          ) AS ninzu, 
                                                          view_tuhan_bunkai_shohin.channel, 
                                                          view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                          view_tuhan_bunkai_shohin.itembunrui, 
                                                          view_tuhan_bunkai_shohin.juchkbn, 
                                                          (
                                                            view_tuhan_bunkai_shohin.juchkbncname
                                                          ):: character varying AS juchkbncname, 
                                                          view_tuhan_bunkai_shohin.henreasoncode, 
                                                          (
                                                            view_tuhan_bunkai_shohin.henreasonname
                                                          ):: character varying AS henreasonname 
                                                        FROM 
                                                          (
                                                            SELECT 
                                                              "substring"(
                                                                (
                                                                  (h.shukadate):: character varying
                                                                ):: text, 
                                                                0, 
                                                                6
                                                              ) AS shukadate, 
                                                              h.kokyano, 
                                                              CASE WHEN (
                                                                (
                                                                  h.kakokbn = (
                                                                    (1):: numeric
                                                                  ):: numeric(18, 0)
                                                                ) 
                                                                OR (
                                                                  (h.kakokbn IS NULL) 
                                                                  AND ('1' IS NULL)
                                                                )
                                                              ) THEN CASE WHEN (
                                                                (h.kaisha):: text = ('000' :: character varying):: text
                                                              ) THEN '01:通販' :: character varying WHEN (
                                                                (h.kaisha):: text = ('001' :: character varying):: text
                                                              ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                                h.smkeiroid = (
                                                                  (6):: numeric
                                                                ):: numeric(18, 0)
                                                              ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                              h.daihanrobunname AS konyuchubuncode, 
                                                              i.itembunrui, 
                                                              h.juchkbn, 
                                                              tm67.cname AS juchkbncname, 
                                                              h.henreasoncode, 
                                                              (
                                                                (
                                                                  (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                                ) || (h.henreasonname):: text
                                                              ) AS henreasonname 
                                                            FROM 
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      saleh_itemstrategy h 
                                                                      JOIN salem_itemstrategy m ON (
                                                                        (
                                                                          (h.saleno):: text = (m.saleno):: text
                                                                        )
                                                                      )
                                                                    ) 
                                                                    JOIN tm14shkos "k" ON (
                                                                      (
                                                                        (m.itemcode):: text = ("k".itemcode):: text
                                                                      )
                                                                    )
                                                                  ) 
                                                                  JOIN tm39item_strategy i ON (
                                                                    (
                                                                      ("k".kosecode):: text = (i.itemcode):: text
                                                                    )
                                                                  )
                                                                ) 
                                                                LEFT JOIN tm67juch_nm tm67 ON (
                                                                  (
                                                                    (h.juchkbn):: text = (tm67.code):: text
                                                                  )
                                                                )
                                                              ) 
                                                            WHERE 
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            h.shukadate >= (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    to_char(
                                                                                      add_months(
                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                        (
                                                                                          - (36):: bigint
                                                                                        )
                                                                                      ), 
                                                                                      ('yyyymm' :: character varying):: text
                                                                                    ) || ('01' :: character varying):: text
                                                                                  )
                                                                                ):: integer
                                                                              ):: numeric
                                                                            ):: numeric(18, 0)
                                                                          ) 
                                                                          AND (
                                                                            h.shukadate <= (
                                                                              (
                                                                                (
                                                                                  to_char(
                                                                                    (
                                                                                      last_day(
                                                                                        add_months(
                                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                                          (
                                                                                            - (1):: bigint
                                                                                          )
                                                                                        )
                                                                                      )
                                                                                    ):: timestamp without time zone, 
                                                                                    ('yyyymmdd' :: character varying):: text
                                                                                  )
                                                                                ):: integer
                                                                              ):: numeric
                                                                            ):: numeric(18, 0)
                                                                          )
                                                                        ) 
                                                                        AND (
                                                                          h.cancelflg = (
                                                                            (0):: numeric
                                                                          ):: numeric(18, 0)
                                                                        )
                                                                      ) 
                                                                      AND (
                                                                        (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                      )
                                                                    ) 
                                                                    AND (
                                                                      (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                    )
                                                                  ) 
                                                                  AND (
                                                                    (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                  )
                                                                ) 
                                                                AND (
                                                                  (
                                                                    (
                                                                      (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                    ) 
                                                                    OR (
                                                                      (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                    )
                                                                  ) 
                                                                  OR (
                                                                    (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                  )
                                                                )
                                                              )
                                                          ) view_tuhan_bunkai_shohin 
                                                        WHERE 
                                                          (
                                                            (
                                                              view_tuhan_bunkai_shohin.shukadate >= (
                                                                (
                                                                  (
                                                                    to_char(
                                                                      add_months(
                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                        (
                                                                          - (16):: bigint
                                                                        )
                                                                      ), 
                                                                      ('yyyymm' :: character varying):: text
                                                                    )
                                                                  ):: integer
                                                                ):: character varying
                                                              ):: text
                                                            ) 
                                                            AND (
                                                              view_tuhan_bunkai_shohin.shukadate <= (
                                                                (
                                                                  (
                                                                    to_char(
                                                                      (
                                                                        last_day(
                                                                          add_months(
                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                            (
                                                                              - (5):: bigint
                                                                            )
                                                                          )
                                                                        )
                                                                      ):: timestamp without time zone, 
                                                                      ('yyyymm' :: character varying):: text
                                                                    )
                                                                  ):: integer
                                                                ):: character varying
                                                              ):: text
                                                            )
                                                          ) 
                                                        GROUP BY 
                                                          view_tuhan_bunkai_shohin.channel, 
                                                          view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                          view_tuhan_bunkai_shohin.itembunrui, 
                                                          view_tuhan_bunkai_shohin.juchkbn, 
                                                          view_tuhan_bunkai_shohin.juchkbncname, 
                                                          view_tuhan_bunkai_shohin.henreasoncode, 
                                                          view_tuhan_bunkai_shohin.henreasonname
                                                      ) 
                                                      UNION 
                                                      SELECT 
                                                        (
                                                          to_char(
                                                            add_months(
                                                              ('now' :: character varying):: timestamp without time zone, 
                                                              (
                                                                - (4):: bigint
                                                              )
                                                            ), 
                                                            ('yyyymm' :: character varying):: text
                                                          )
                                                        ):: integer AS nengetu, 
                                                        count(
                                                          DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                        ) AS ninzu, 
                                                        view_tuhan_bunkai_shohin.channel, 
                                                        view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                        view_tuhan_bunkai_shohin.itembunrui, 
                                                        view_tuhan_bunkai_shohin.juchkbn, 
                                                        (
                                                          view_tuhan_bunkai_shohin.juchkbncname
                                                        ):: character varying AS juchkbncname, 
                                                        view_tuhan_bunkai_shohin.henreasoncode, 
                                                        (
                                                          view_tuhan_bunkai_shohin.henreasonname
                                                        ):: character varying AS henreasonname 
                                                      FROM 
                                                        (
                                                          SELECT 
                                                            "substring"(
                                                              (
                                                                (h.shukadate):: character varying
                                                              ):: text, 
                                                              0, 
                                                              6
                                                            ) AS shukadate, 
                                                            h.kokyano, 
                                                            CASE WHEN (
                                                              (
                                                                h.kakokbn = (
                                                                  (1):: numeric
                                                                ):: numeric(18, 0)
                                                              ) 
                                                              OR (
                                                                (h.kakokbn IS NULL) 
                                                                AND ('1' IS NULL)
                                                              )
                                                            ) THEN CASE WHEN (
                                                              (h.kaisha):: text = ('000' :: character varying):: text
                                                            ) THEN '01:通販' :: character varying WHEN (
                                                              (h.kaisha):: text = ('001' :: character varying):: text
                                                            ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                              h.smkeiroid = (
                                                                (6):: numeric
                                                              ):: numeric(18, 0)
                                                            ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                            h.daihanrobunname AS konyuchubuncode, 
                                                            i.itembunrui, 
                                                            h.juchkbn, 
                                                            tm67.cname AS juchkbncname, 
                                                            h.henreasoncode, 
                                                            (
                                                              (
                                                                (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                              ) || (h.henreasonname):: text
                                                            ) AS henreasonname 
                                                          FROM 
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    saleh_itemstrategy h 
                                                                    JOIN salem_itemstrategy m ON (
                                                                      (
                                                                        (h.saleno):: text = (m.saleno):: text
                                                                      )
                                                                    )
                                                                  ) 
                                                                  JOIN tm14shkos "k" ON (
                                                                    (
                                                                      (m.itemcode):: text = ("k".itemcode):: text
                                                                    )
                                                                  )
                                                                ) 
                                                                JOIN tm39item_strategy i ON (
                                                                  (
                                                                    ("k".kosecode):: text = (i.itemcode):: text
                                                                  )
                                                                )
                                                              ) 
                                                              LEFT JOIN tm67juch_nm tm67 ON (
                                                                (
                                                                  (h.juchkbn):: text = (tm67.code):: text
                                                                )
                                                              )
                                                            ) 
                                                          WHERE 
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          h.shukadate >= (
                                                                            (
                                                                              (
                                                                                (
                                                                                  to_char(
                                                                                    add_months(
                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                      (
                                                                                        - (36):: bigint
                                                                                      )
                                                                                    ), 
                                                                                    ('yyyymm' :: character varying):: text
                                                                                  ) || ('01' :: character varying):: text
                                                                                )
                                                                              ):: integer
                                                                            ):: numeric
                                                                          ):: numeric(18, 0)
                                                                        ) 
                                                                        AND (
                                                                          h.shukadate <= (
                                                                            (
                                                                              (
                                                                                to_char(
                                                                                  (
                                                                                    last_day(
                                                                                      add_months(
                                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                                        (
                                                                                          - (1):: bigint
                                                                                        )
                                                                                      )
                                                                                    )
                                                                                  ):: timestamp without time zone, 
                                                                                  ('yyyymmdd' :: character varying):: text
                                                                                )
                                                                              ):: integer
                                                                            ):: numeric
                                                                          ):: numeric(18, 0)
                                                                        )
                                                                      ) 
                                                                      AND (
                                                                        h.cancelflg = (
                                                                          (0):: numeric
                                                                        ):: numeric(18, 0)
                                                                      )
                                                                    ) 
                                                                    AND (
                                                                      (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                    )
                                                                  ) 
                                                                  AND (
                                                                    (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                  )
                                                                ) 
                                                                AND (
                                                                  (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                                )
                                                              ) 
                                                              AND (
                                                                (
                                                                  (
                                                                    (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                  ) 
                                                                  OR (
                                                                    (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                  )
                                                                ) 
                                                                OR (
                                                                  (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                                )
                                                              )
                                                            )
                                                        ) view_tuhan_bunkai_shohin 
                                                      WHERE 
                                                        (
                                                          (
                                                            view_tuhan_bunkai_shohin.shukadate >= (
                                                              (
                                                                (
                                                                  to_char(
                                                                    add_months(
                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                      (
                                                                        - (15):: bigint
                                                                      )
                                                                    ), 
                                                                    ('yyyymm' :: character varying):: text
                                                                  )
                                                                ):: integer
                                                              ):: character varying
                                                            ):: text
                                                          ) 
                                                          AND (
                                                            view_tuhan_bunkai_shohin.shukadate <= (
                                                              (
                                                                (
                                                                  to_char(
                                                                    (
                                                                      last_day(
                                                                        add_months(
                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                          (
                                                                            - (4):: bigint
                                                                          )
                                                                        )
                                                                      )
                                                                    ):: timestamp without time zone, 
                                                                    ('yyyymm' :: character varying):: text
                                                                  )
                                                                ):: integer
                                                              ):: character varying
                                                            ):: text
                                                          )
                                                        ) 
                                                      GROUP BY 
                                                        view_tuhan_bunkai_shohin.channel, 
                                                        view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                        view_tuhan_bunkai_shohin.itembunrui, 
                                                        view_tuhan_bunkai_shohin.juchkbn, 
                                                        view_tuhan_bunkai_shohin.juchkbncname, 
                                                        view_tuhan_bunkai_shohin.henreasoncode, 
                                                        view_tuhan_bunkai_shohin.henreasonname
                                                    ) 
                                                    UNION 
                                                    SELECT 
                                                      (
                                                        to_char(
                                                          add_months(
                                                            ('now' :: character varying):: timestamp without time zone, 
                                                            (
                                                              - (3):: bigint
                                                            )
                                                          ), 
                                                          ('yyyymm' :: character varying):: text
                                                        )
                                                      ):: integer AS nengetu, 
                                                      count(
                                                        DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                      ) AS ninzu, 
                                                      view_tuhan_bunkai_shohin.channel, 
                                                      view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                      view_tuhan_bunkai_shohin.itembunrui, 
                                                      view_tuhan_bunkai_shohin.juchkbn, 
                                                      (
                                                        view_tuhan_bunkai_shohin.juchkbncname
                                                      ):: character varying AS juchkbncname, 
                                                      view_tuhan_bunkai_shohin.henreasoncode, 
                                                      (
                                                        view_tuhan_bunkai_shohin.henreasonname
                                                      ):: character varying AS henreasonname 
                                                    FROM 
                                                      (
                                                        SELECT 
                                                          "substring"(
                                                            (
                                                              (h.shukadate):: character varying
                                                            ):: text, 
                                                            0, 
                                                            6
                                                          ) AS shukadate, 
                                                          h.kokyano, 
                                                          CASE WHEN (
                                                            (
                                                              h.kakokbn = (
                                                                (1):: numeric
                                                              ):: numeric(18, 0)
                                                            ) 
                                                            OR (
                                                              (h.kakokbn IS NULL) 
                                                              AND ('1' IS NULL)
                                                            )
                                                          ) THEN CASE WHEN (
                                                            (h.kaisha):: text = ('000' :: character varying):: text
                                                          ) THEN '01:通販' :: character varying WHEN (
                                                            (h.kaisha):: text = ('001' :: character varying):: text
                                                          ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                            h.smkeiroid = (
                                                              (6):: numeric
                                                            ):: numeric(18, 0)
                                                          ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                          h.daihanrobunname AS konyuchubuncode, 
                                                          i.itembunrui, 
                                                          h.juchkbn, 
                                                          tm67.cname AS juchkbncname, 
                                                          h.henreasoncode, 
                                                          (
                                                            (
                                                              (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                            ) || (h.henreasonname):: text
                                                          ) AS henreasonname 
                                                        FROM 
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  saleh_itemstrategy h 
                                                                  JOIN salem_itemstrategy m ON (
                                                                    (
                                                                      (h.saleno):: text = (m.saleno):: text
                                                                    )
                                                                  )
                                                                ) 
                                                                JOIN tm14shkos "k" ON (
                                                                  (
                                                                    (m.itemcode):: text = ("k".itemcode):: text
                                                                  )
                                                                )
                                                              ) 
                                                              JOIN tm39item_strategy i ON (
                                                                (
                                                                  ("k".kosecode):: text = (i.itemcode):: text
                                                                )
                                                              )
                                                            ) 
                                                            LEFT JOIN tm67juch_nm tm67 ON (
                                                              (
                                                                (h.juchkbn):: text = (tm67.code):: text
                                                              )
                                                            )
                                                          ) 
                                                        WHERE 
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        h.shukadate >= (
                                                                          (
                                                                            (
                                                                              (
                                                                                to_char(
                                                                                  add_months(
                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                    (
                                                                                      - (36):: bigint
                                                                                    )
                                                                                  ), 
                                                                                  ('yyyymm' :: character varying):: text
                                                                                ) || ('01' :: character varying):: text
                                                                              )
                                                                            ):: integer
                                                                          ):: numeric
                                                                        ):: numeric(18, 0)
                                                                      ) 
                                                                      AND (
                                                                        h.shukadate <= (
                                                                          (
                                                                            (
                                                                              to_char(
                                                                                (
                                                                                  last_day(
                                                                                    add_months(
                                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                                      (
                                                                                        - (1):: bigint
                                                                                      )
                                                                                    )
                                                                                  )
                                                                                ):: timestamp without time zone, 
                                                                                ('yyyymmdd' :: character varying):: text
                                                                              )
                                                                            ):: integer
                                                                          ):: numeric
                                                                        ):: numeric(18, 0)
                                                                      )
                                                                    ) 
                                                                    AND (
                                                                      h.cancelflg = (
                                                                        (0):: numeric
                                                                      ):: numeric(18, 0)
                                                                    )
                                                                  ) 
                                                                  AND (
                                                                    (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                  )
                                                                ) 
                                                                AND (
                                                                  (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                                )
                                                              ) 
                                                              AND (
                                                                (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                              )
                                                            ) 
                                                            AND (
                                                              (
                                                                (
                                                                  (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                                ) 
                                                                OR (
                                                                  (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                                )
                                                              ) 
                                                              OR (
                                                                (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                              )
                                                            )
                                                          )
                                                      ) view_tuhan_bunkai_shohin 
                                                    WHERE 
                                                      (
                                                        (
                                                          view_tuhan_bunkai_shohin.shukadate >= (
                                                            (
                                                              (
                                                                to_char(
                                                                  add_months(
                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                    (
                                                                      - (14):: bigint
                                                                    )
                                                                  ), 
                                                                  ('yyyymm' :: character varying):: text
                                                                )
                                                              ):: integer
                                                            ):: character varying
                                                          ):: text
                                                        ) 
                                                        AND (
                                                          view_tuhan_bunkai_shohin.shukadate <= (
                                                            (
                                                              (
                                                                to_char(
                                                                  (
                                                                    last_day(
                                                                      add_months(
                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                        (
                                                                          - (3):: bigint
                                                                        )
                                                                      )
                                                                    )
                                                                  ):: timestamp without time zone, 
                                                                  ('yyyymm' :: character varying):: text
                                                                )
                                                              ):: integer
                                                            ):: character varying
                                                          ):: text
                                                        )
                                                      ) 
                                                    GROUP BY 
                                                      view_tuhan_bunkai_shohin.channel, 
                                                      view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                      view_tuhan_bunkai_shohin.itembunrui, 
                                                      view_tuhan_bunkai_shohin.juchkbn, 
                                                      view_tuhan_bunkai_shohin.juchkbncname, 
                                                      view_tuhan_bunkai_shohin.henreasoncode, 
                                                      view_tuhan_bunkai_shohin.henreasonname
                                                  ) 
                                                  UNION 
                                                  SELECT 
                                                    (
                                                      to_char(
                                                        add_months(
                                                          ('now' :: character varying):: timestamp without time zone, 
                                                          (
                                                            - (2):: bigint
                                                          )
                                                        ), 
                                                        ('yyyymm' :: character varying):: text
                                                      )
                                                    ):: integer AS nengetu, 
                                                    count(
                                                      DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                    ) AS ninzu, 
                                                    view_tuhan_bunkai_shohin.channel, 
                                                    view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                    view_tuhan_bunkai_shohin.itembunrui, 
                                                    view_tuhan_bunkai_shohin.juchkbn, 
                                                    (
                                                      view_tuhan_bunkai_shohin.juchkbncname
                                                    ):: character varying AS juchkbncname, 
                                                    view_tuhan_bunkai_shohin.henreasoncode, 
                                                    (
                                                      view_tuhan_bunkai_shohin.henreasonname
                                                    ):: character varying AS henreasonname 
                                                  FROM 
                                                    (
                                                      SELECT 
                                                        "substring"(
                                                          (
                                                            (h.shukadate):: character varying
                                                          ):: text, 
                                                          0, 
                                                          6
                                                        ) AS shukadate, 
                                                        h.kokyano, 
                                                        CASE WHEN (
                                                          (
                                                            h.kakokbn = (
                                                              (1):: numeric
                                                            ):: numeric(18, 0)
                                                          ) 
                                                          OR (
                                                            (h.kakokbn IS NULL) 
                                                            AND ('1' IS NULL)
                                                          )
                                                        ) THEN CASE WHEN (
                                                          (h.kaisha):: text = ('000' :: character varying):: text
                                                        ) THEN '01:通販' :: character varying WHEN (
                                                          (h.kaisha):: text = ('001' :: character varying):: text
                                                        ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                          h.smkeiroid = (
                                                            (6):: numeric
                                                          ):: numeric(18, 0)
                                                        ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                        h.daihanrobunname AS konyuchubuncode, 
                                                        i.itembunrui, 
                                                        h.juchkbn, 
                                                        tm67.cname AS juchkbncname, 
                                                        h.henreasoncode, 
                                                        (
                                                          (
                                                            (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                          ) || (h.henreasonname):: text
                                                        ) AS henreasonname 
                                                      FROM 
                                                        (
                                                          (
                                                            (
                                                              (
                                                                saleh_itemstrategy h 
                                                                JOIN salem_itemstrategy m ON (
                                                                  (
                                                                    (h.saleno):: text = (m.saleno):: text
                                                                  )
                                                                )
                                                              ) 
                                                              JOIN tm14shkos "k" ON (
                                                                (
                                                                  (m.itemcode):: text = ("k".itemcode):: text
                                                                )
                                                              )
                                                            ) 
                                                            JOIN tm39item_strategy i ON (
                                                              (
                                                                ("k".kosecode):: text = (i.itemcode):: text
                                                              )
                                                            )
                                                          ) 
                                                          LEFT JOIN tm67juch_nm tm67 ON (
                                                            (
                                                              (h.juchkbn):: text = (tm67.code):: text
                                                            )
                                                          )
                                                        ) 
                                                      WHERE 
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      h.shukadate >= (
                                                                        (
                                                                          (
                                                                            (
                                                                              to_char(
                                                                                add_months(
                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                  (
                                                                                    - (36):: bigint
                                                                                  )
                                                                                ), 
                                                                                ('yyyymm' :: character varying):: text
                                                                              ) || ('01' :: character varying):: text
                                                                            )
                                                                          ):: integer
                                                                        ):: numeric
                                                                      ):: numeric(18, 0)
                                                                    ) 
                                                                    AND (
                                                                      h.shukadate <= (
                                                                        (
                                                                          (
                                                                            to_char(
                                                                              (
                                                                                last_day(
                                                                                  add_months(
                                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                                    (
                                                                                      - (1):: bigint
                                                                                    )
                                                                                  )
                                                                                )
                                                                              ):: timestamp without time zone, 
                                                                              ('yyyymmdd' :: character varying):: text
                                                                            )
                                                                          ):: integer
                                                                        ):: numeric
                                                                      ):: numeric(18, 0)
                                                                    )
                                                                  ) 
                                                                  AND (
                                                                    h.cancelflg = (
                                                                      (0):: numeric
                                                                    ):: numeric(18, 0)
                                                                  )
                                                                ) 
                                                                AND (
                                                                  (h.torikeikbn):: text = ('01' :: character varying):: text
                                                                )
                                                              ) 
                                                              AND (
                                                                (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                              )
                                                            ) 
                                                            AND (
                                                              (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                            )
                                                          ) 
                                                          AND (
                                                            (
                                                              (
                                                                (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                              ) 
                                                              OR (
                                                                (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                              )
                                                            ) 
                                                            OR (
                                                              (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                            )
                                                          )
                                                        )
                                                    ) view_tuhan_bunkai_shohin 
                                                  WHERE 
                                                    (
                                                      (
                                                        view_tuhan_bunkai_shohin.shukadate >= (
                                                          (
                                                            (
                                                              to_char(
                                                                add_months(
                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                  (
                                                                    - (13):: bigint
                                                                  )
                                                                ), 
                                                                ('yyyymm' :: character varying):: text
                                                              )
                                                            ):: integer
                                                          ):: character varying
                                                        ):: text
                                                      ) 
                                                      AND (
                                                        view_tuhan_bunkai_shohin.shukadate <= (
                                                          (
                                                            (
                                                              to_char(
                                                                (
                                                                  last_day(
                                                                    add_months(
                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                      (
                                                                        - (2):: bigint
                                                                      )
                                                                    )
                                                                  )
                                                                ):: timestamp without time zone, 
                                                                ('yyyymm' :: character varying):: text
                                                              )
                                                            ):: integer
                                                          ):: character varying
                                                        ):: text
                                                      )
                                                    ) 
                                                  GROUP BY 
                                                    view_tuhan_bunkai_shohin.channel, 
                                                    view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                    view_tuhan_bunkai_shohin.itembunrui, 
                                                    view_tuhan_bunkai_shohin.juchkbn, 
                                                    view_tuhan_bunkai_shohin.juchkbncname, 
                                                    view_tuhan_bunkai_shohin.henreasoncode, 
                                                    view_tuhan_bunkai_shohin.henreasonname
                                                ) 
                                                UNION 
                                                SELECT 
                                                  (
                                                    to_char(
                                                      add_months(
                                                        ('now' :: character varying):: timestamp without time zone, 
                                                        (
                                                          - (1):: bigint
                                                        )
                                                      ), 
                                                      ('yyyymm' :: character varying):: text
                                                    )
                                                  ):: integer AS nengetu, 
                                                  count(
                                                    DISTINCT view_tuhan_bunkai_shohin.kokyano
                                                  ) AS ninzu, 
                                                  view_tuhan_bunkai_shohin.channel, 
                                                  view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                  view_tuhan_bunkai_shohin.itembunrui, 
                                                  view_tuhan_bunkai_shohin.juchkbn, 
                                                  (
                                                    view_tuhan_bunkai_shohin.juchkbncname
                                                  ):: character varying AS juchkbncname, 
                                                  view_tuhan_bunkai_shohin.henreasoncode, 
                                                  (
                                                    view_tuhan_bunkai_shohin.henreasonname
                                                  ):: character varying AS henreasonname 
                                                FROM 
                                                  (
                                                    SELECT 
                                                      "substring"(
                                                        (
                                                          (h.shukadate):: character varying
                                                        ):: text, 
                                                        0, 
                                                        6
                                                      ) AS shukadate, 
                                                      h.kokyano, 
                                                      CASE WHEN (
                                                        (
                                                          h.kakokbn = (
                                                            (1):: numeric
                                                          ):: numeric(18, 0)
                                                        ) 
                                                        OR (
                                                          (h.kakokbn IS NULL) 
                                                          AND ('1' IS NULL)
                                                        )
                                                      ) THEN CASE WHEN (
                                                        (h.kaisha):: text = ('000' :: character varying):: text
                                                      ) THEN '01:通販' :: character varying WHEN (
                                                        (h.kaisha):: text = ('001' :: character varying):: text
                                                      ) THEN '02:社内販売' :: character varying ELSE '03:職域販売' :: character varying END ELSE CASE WHEN (
                                                        h.smkeiroid = (
                                                          (6):: numeric
                                                        ):: numeric(18, 0)
                                                      ) THEN '02:社内販売' :: character varying ELSE '01:通販' :: character varying END END AS channel, 
                                                      h.daihanrobunname AS konyuchubuncode, 
                                                      i.itembunrui, 
                                                      h.juchkbn, 
                                                      tm67.cname AS juchkbncname, 
                                                      h.henreasoncode, 
                                                      (
                                                        (
                                                          (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                        ) || (h.henreasonname):: text
                                                      ) AS henreasonname 
                                                    FROM 
                                                      (
                                                        (
                                                          (
                                                            (
                                                              saleh_itemstrategy h 
                                                              JOIN salem_itemstrategy m ON (
                                                                (
                                                                  (h.saleno):: text = (m.saleno):: text
                                                                )
                                                              )
                                                            ) 
                                                            JOIN tm14shkos "k" ON (
                                                              (
                                                                (m.itemcode):: text = ("k".itemcode):: text
                                                              )
                                                            )
                                                          ) 
                                                          JOIN tm39item_strategy i ON (
                                                            (
                                                              ("k".kosecode):: text = (i.itemcode):: text
                                                            )
                                                          )
                                                        ) 
                                                        LEFT JOIN tm67juch_nm tm67 ON (
                                                          (
                                                            (h.juchkbn):: text = (tm67.code):: text
                                                          )
                                                        )
                                                      ) 
                                                    WHERE 
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    h.shukadate >= (
                                                                      (
                                                                        (
                                                                          (
                                                                            to_char(
                                                                              add_months(
                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                (
                                                                                  - (36):: bigint
                                                                                )
                                                                              ), 
                                                                              ('yyyymm' :: character varying):: text
                                                                            ) || ('01' :: character varying):: text
                                                                          )
                                                                        ):: integer
                                                                      ):: numeric
                                                                    ):: numeric(18, 0)
                                                                  ) 
                                                                  AND (
                                                                    h.shukadate <= (
                                                                      (
                                                                        (
                                                                          to_char(
                                                                            (
                                                                              last_day(
                                                                                add_months(
                                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                                  (
                                                                                    - (1):: bigint
                                                                                  )
                                                                                )
                                                                              )
                                                                            ):: timestamp without time zone, 
                                                                            ('yyyymmdd' :: character varying):: text
                                                                          )
                                                                        ):: integer
                                                                      ):: numeric
                                                                    ):: numeric(18, 0)
                                                                  )
                                                                ) 
                                                                AND (
                                                                  h.cancelflg = (
                                                                    (0):: numeric
                                                                  ):: numeric(18, 0)
                                                                )
                                                              ) 
                                                              AND (
                                                                (h.torikeikbn):: text = ('01' :: character varying):: text
                                                              )
                                                            ) 
                                                            AND (
                                                              (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                            )
                                                          ) 
                                                          AND (
                                                            (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                          )
                                                        ) 
                                                        AND (
                                                          (
                                                            (
                                                              (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                            ) 
                                                            OR (
                                                              (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                            )
                                                          ) 
                                                          OR (
                                                            (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                          )
                                                        )
                                                      )
                                                  ) view_tuhan_bunkai_shohin 
                                                WHERE 
                                                  (
                                                    (
                                                      view_tuhan_bunkai_shohin.shukadate >= (
                                                        (
                                                          (
                                                            to_char(
                                                              add_months(
                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                (
                                                                  - (12):: bigint
                                                                )
                                                              ), 
                                                              ('yyyymm' :: character varying):: text
                                                            )
                                                          ):: integer
                                                        ):: character varying
                                                      ):: text
                                                    ) 
                                                    AND (
                                                      view_tuhan_bunkai_shohin.shukadate <= (
                                                        (
                                                          (
                                                            to_char(
                                                              (
                                                                last_day(
                                                                  add_months(
                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                    (
                                                                      - (1):: bigint
                                                                    )
                                                                  )
                                                                )
                                                              ):: timestamp without time zone, 
                                                              ('yyyymm' :: character varying):: text
                                                            )
                                                          ):: integer
                                                        ):: character varying
                                                      ):: text
                                                    )
                                                  ) 
                                                GROUP BY 
                                                  view_tuhan_bunkai_shohin.channel, 
                                                  view_tuhan_bunkai_shohin.konyuchubuncode, 
                                                  view_tuhan_bunkai_shohin.itembunrui, 
                                                  view_tuhan_bunkai_shohin.juchkbn, 
                                                  view_tuhan_bunkai_shohin.juchkbncname, 
                                                  view_tuhan_bunkai_shohin.henreasoncode, 
                                                  view_tuhan_bunkai_shohin.henreasonname
                                              ) 
                                              UNION 
                                              SELECT 
                                                (
                                                  to_char(
                                                    add_months(
                                                      ('now' :: character varying):: timestamp without time zone, 
                                                      (
                                                        - (24):: bigint
                                                      )
                                                    ), 
                                                    ('yyyymm' :: character varying):: text
                                                  )
                                                ):: integer AS nengetu, 
                                                count(
                                                  DISTINCT view_tuhan_tempo_shohin.kokyano
                                                ) AS ninzu, 
                                                '10:百貨店' AS channel, 
                                                view_tuhan_tempo_shohin.konyuchubuncode, 
                                                view_tuhan_tempo_shohin.itembunrui, 
                                                view_tuhan_tempo_shohin.juchkbn, 
                                                (
                                                  view_tuhan_tempo_shohin.juchkbncname
                                                ):: character varying AS juchkbncname, 
                                                view_tuhan_tempo_shohin.henreasoncode, 
                                                (
                                                  view_tuhan_tempo_shohin.henreasonname
                                                ):: character varying AS henreasonname 
                                              FROM 
                                                (
                                                  SELECT 
                                                    "substring"(
                                                      (
                                                        (h.shukadate):: character varying
                                                      ):: text, 
                                                      0, 
                                                      6
                                                    ) AS shukadate, 
                                                    h.kokyano, 
                                                    h.daihanrobunname AS konyuchubuncode, 
                                                    i.itembunrui, 
                                                    h.juchkbn, 
                                                    tm67.cname AS juchkbncname, 
                                                    h.henreasoncode, 
                                                    (
                                                      (
                                                        (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                      ) || (h.henreasonname):: text
                                                    ) AS henreasonname 
                                                  FROM 
                                                    (
                                                      (
                                                        (
                                                          (
                                                            saleh_itemstrategy h 
                                                            JOIN salem_itemstrategy m ON (
                                                              (
                                                                (h.saleno):: text = (m.saleno):: text
                                                              )
                                                            )
                                                          ) 
                                                          JOIN tm14shkos "k" ON (
                                                            (
                                                              (m.itemcode):: text = ("k".itemcode):: text
                                                            )
                                                          )
                                                        ) 
                                                        LEFT JOIN tm39item_strategy i ON (
                                                          (
                                                            ("k".kosecode):: text = (i.itemcode):: text
                                                          )
                                                        )
                                                      ) 
                                                      LEFT JOIN tm67juch_nm tm67 ON (
                                                        (
                                                          (h.juchkbn):: text = (tm67.code):: text
                                                        )
                                                      )
                                                    ) 
                                                  WHERE 
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  h.shukadate >= (
                                                                    (
                                                                      (
                                                                        (
                                                                          to_char(
                                                                            add_months(
                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                              (
                                                                                - (36):: bigint
                                                                              )
                                                                            ), 
                                                                            ('yyyymm' :: character varying):: text
                                                                          ) || ('01' :: character varying):: text
                                                                        )
                                                                      ):: integer
                                                                    ):: numeric
                                                                  ):: numeric(18, 0)
                                                                ) 
                                                                AND (
                                                                  h.shukadate <= (
                                                                    (
                                                                      (
                                                                        to_char(
                                                                          (
                                                                            last_day(
                                                                              add_months(
                                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                                (
                                                                                  - (1):: bigint
                                                                                )
                                                                              )
                                                                            )
                                                                          ):: timestamp without time zone, 
                                                                          ('yyyymmdd' :: character varying):: text
                                                                        )
                                                                      ):: integer
                                                                    ):: numeric
                                                                  ):: numeric(18, 0)
                                                                )
                                                              ) 
                                                              AND (
                                                                h.cancelflg = (
                                                                  (0):: numeric
                                                                ):: numeric(18, 0)
                                                              )
                                                            ) 
                                                            AND (
                                                              (
                                                                (
                                                                  (h.torikeikbn):: text = ('03' :: character varying):: text
                                                                ) 
                                                                OR (
                                                                  (h.torikeikbn):: text = ('04' :: character varying):: text
                                                                )
                                                              ) 
                                                              OR (
                                                                (h.torikeikbn):: text = ('05' :: character varying):: text
                                                              )
                                                            )
                                                          ) 
                                                          AND (
                                                            (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                          )
                                                        ) 
                                                        AND (
                                                          (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                        )
                                                      ) 
                                                      AND (
                                                        (
                                                          (
                                                            (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                          ) 
                                                          OR (
                                                            (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                          )
                                                        ) 
                                                        OR (
                                                          (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                        )
                                                      )
                                                    )
                                                ) view_tuhan_tempo_shohin 
                                              WHERE 
                                                (
                                                  (
                                                    view_tuhan_tempo_shohin.shukadate >= (
                                                      (
                                                        (
                                                          to_char(
                                                            add_months(
                                                              ('now' :: character varying):: timestamp without time zone, 
                                                              (
                                                                - (35):: bigint
                                                              )
                                                            ), 
                                                            ('yyyymm' :: character varying):: text
                                                          )
                                                        ):: integer
                                                      ):: character varying
                                                    ):: text
                                                  ) 
                                                  AND (
                                                    view_tuhan_tempo_shohin.shukadate <= (
                                                      (
                                                        (
                                                          to_char(
                                                            add_months(
                                                              ('now' :: character varying):: timestamp without time zone, 
                                                              (
                                                                - (24):: bigint
                                                              )
                                                            ), 
                                                            ('yyyymm' :: character varying):: text
                                                          )
                                                        ):: integer
                                                      ):: character varying
                                                    ):: text
                                                  )
                                                ) 
                                              GROUP BY 
                                                view_tuhan_tempo_shohin.konyuchubuncode, 
                                                view_tuhan_tempo_shohin.itembunrui, 
                                                view_tuhan_tempo_shohin.juchkbn, 
                                                view_tuhan_tempo_shohin.juchkbncname, 
                                                view_tuhan_tempo_shohin.henreasoncode, 
                                                view_tuhan_tempo_shohin.henreasonname
                                            ) 
                                            UNION 
                                            SELECT 
                                              (
                                                to_char(
                                                  add_months(
                                                    ('now' :: character varying):: timestamp without time zone, 
                                                    (
                                                      - (23):: bigint
                                                    )
                                                  ), 
                                                  ('yyyymm' :: character varying):: text
                                                )
                                              ):: integer AS nengetu, 
                                              count(
                                                DISTINCT view_tuhan_tempo_shohin.kokyano
                                              ) AS ninzu, 
                                              '10:百貨店' AS channel, 
                                              view_tuhan_tempo_shohin.konyuchubuncode, 
                                              view_tuhan_tempo_shohin.itembunrui, 
                                              view_tuhan_tempo_shohin.juchkbn, 
                                              (
                                                view_tuhan_tempo_shohin.juchkbncname
                                              ):: character varying AS juchkbncname, 
                                              view_tuhan_tempo_shohin.henreasoncode, 
                                              (
                                                view_tuhan_tempo_shohin.henreasonname
                                              ):: character varying AS henreasonname 
                                            FROM 
                                              (
                                                SELECT 
                                                  "substring"(
                                                    (
                                                      (h.shukadate):: character varying
                                                    ):: text, 
                                                    0, 
                                                    6
                                                  ) AS shukadate, 
                                                  h.kokyano, 
                                                  h.daihanrobunname AS konyuchubuncode, 
                                                  i.itembunrui, 
                                                  h.juchkbn, 
                                                  tm67.cname AS juchkbncname, 
                                                  h.henreasoncode, 
                                                  (
                                                    (
                                                      (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                    ) || (h.henreasonname):: text
                                                  ) AS henreasonname 
                                                FROM 
                                                  (
                                                    (
                                                      (
                                                        (
                                                          saleh_itemstrategy h 
                                                          JOIN salem_itemstrategy m ON (
                                                            (
                                                              (h.saleno):: text = (m.saleno):: text
                                                            )
                                                          )
                                                        ) 
                                                        JOIN tm14shkos "k" ON (
                                                          (
                                                            (m.itemcode):: text = ("k".itemcode):: text
                                                          )
                                                        )
                                                      ) 
                                                      LEFT JOIN tm39item_strategy i ON (
                                                        (
                                                          ("k".kosecode):: text = (i.itemcode):: text
                                                        )
                                                      )
                                                    ) 
                                                    LEFT JOIN tm67juch_nm tm67 ON (
                                                      (
                                                        (h.juchkbn):: text = (tm67.code):: text
                                                      )
                                                    )
                                                  ) 
                                                WHERE 
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                h.shukadate >= (
                                                                  (
                                                                    (
                                                                      (
                                                                        to_char(
                                                                          add_months(
                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                            (
                                                                              - (36):: bigint
                                                                            )
                                                                          ), 
                                                                          ('yyyymm' :: character varying):: text
                                                                        ) || ('01' :: character varying):: text
                                                                      )
                                                                    ):: integer
                                                                  ):: numeric
                                                                ):: numeric(18, 0)
                                                              ) 
                                                              AND (
                                                                h.shukadate <= (
                                                                  (
                                                                    (
                                                                      to_char(
                                                                        (
                                                                          last_day(
                                                                            add_months(
                                                                              ('now' :: character varying):: timestamp without time zone, 
                                                                              (
                                                                                - (1):: bigint
                                                                              )
                                                                            )
                                                                          )
                                                                        ):: timestamp without time zone, 
                                                                        ('yyyymmdd' :: character varying):: text
                                                                      )
                                                                    ):: integer
                                                                  ):: numeric
                                                                ):: numeric(18, 0)
                                                              )
                                                            ) 
                                                            AND (
                                                              h.cancelflg = (
                                                                (0):: numeric
                                                              ):: numeric(18, 0)
                                                            )
                                                          ) 
                                                          AND (
                                                            (
                                                              (
                                                                (h.torikeikbn):: text = ('03' :: character varying):: text
                                                              ) 
                                                              OR (
                                                                (h.torikeikbn):: text = ('04' :: character varying):: text
                                                              )
                                                            ) 
                                                            OR (
                                                              (h.torikeikbn):: text = ('05' :: character varying):: text
                                                            )
                                                          )
                                                        ) 
                                                        AND (
                                                          (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                        )
                                                      ) 
                                                      AND (
                                                        (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                      )
                                                    ) 
                                                    AND (
                                                      (
                                                        (
                                                          (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                        ) 
                                                        OR (
                                                          (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                        )
                                                      ) 
                                                      OR (
                                                        (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                      )
                                                    )
                                                  )
                                              ) view_tuhan_tempo_shohin 
                                            WHERE 
                                              (
                                                (
                                                  view_tuhan_tempo_shohin.shukadate >= (
                                                    (
                                                      (
                                                        to_char(
                                                          add_months(
                                                            ('now' :: character varying):: timestamp without time zone, 
                                                            (
                                                              - (34):: bigint
                                                            )
                                                          ), 
                                                          ('yyyymm' :: character varying):: text
                                                        )
                                                      ):: integer
                                                    ):: character varying
                                                  ):: text
                                                ) 
                                                AND (
                                                  view_tuhan_tempo_shohin.shukadate <= (
                                                    (
                                                      (
                                                        to_char(
                                                          add_months(
                                                            ('now' :: character varying):: timestamp without time zone, 
                                                            (
                                                              - (23):: bigint
                                                            )
                                                          ), 
                                                          ('yyyymm' :: character varying):: text
                                                        )
                                                      ):: integer
                                                    ):: character varying
                                                  ):: text
                                                )
                                              ) 
                                            GROUP BY 
                                              view_tuhan_tempo_shohin.konyuchubuncode, 
                                              view_tuhan_tempo_shohin.itembunrui, 
                                              view_tuhan_tempo_shohin.juchkbn, 
                                              view_tuhan_tempo_shohin.juchkbncname, 
                                              view_tuhan_tempo_shohin.henreasoncode, 
                                              view_tuhan_tempo_shohin.henreasonname
                                          ) 
                                          UNION 
                                          SELECT 
                                            (
                                              to_char(
                                                add_months(
                                                  ('now' :: character varying):: timestamp without time zone, 
                                                  (
                                                    - (22):: bigint
                                                  )
                                                ), 
                                                ('yyyymm' :: character varying):: text
                                              )
                                            ):: integer AS nengetu, 
                                            count(
                                              DISTINCT view_tuhan_tempo_shohin.kokyano
                                            ) AS ninzu, 
                                            '10:百貨店' AS channel, 
                                            view_tuhan_tempo_shohin.konyuchubuncode, 
                                            view_tuhan_tempo_shohin.itembunrui, 
                                            view_tuhan_tempo_shohin.juchkbn, 
                                            (
                                              view_tuhan_tempo_shohin.juchkbncname
                                            ):: character varying AS juchkbncname, 
                                            view_tuhan_tempo_shohin.henreasoncode, 
                                            (
                                              view_tuhan_tempo_shohin.henreasonname
                                            ):: character varying AS henreasonname 
                                          FROM 
                                            (
                                              SELECT 
                                                "substring"(
                                                  (
                                                    (h.shukadate):: character varying
                                                  ):: text, 
                                                  0, 
                                                  6
                                                ) AS shukadate, 
                                                h.kokyano, 
                                                h.daihanrobunname AS konyuchubuncode, 
                                                i.itembunrui, 
                                                h.juchkbn, 
                                                tm67.cname AS juchkbncname, 
                                                h.henreasoncode, 
                                                (
                                                  (
                                                    (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                  ) || (h.henreasonname):: text
                                                ) AS henreasonname 
                                              FROM 
                                                (
                                                  (
                                                    (
                                                      (
                                                        saleh_itemstrategy h 
                                                        JOIN salem_itemstrategy m ON (
                                                          (
                                                            (h.saleno):: text = (m.saleno):: text
                                                          )
                                                        )
                                                      ) 
                                                      JOIN tm14shkos "k" ON (
                                                        (
                                                          (m.itemcode):: text = ("k".itemcode):: text
                                                        )
                                                      )
                                                    ) 
                                                    LEFT JOIN tm39item_strategy i ON (
                                                      (
                                                        ("k".kosecode):: text = (i.itemcode):: text
                                                      )
                                                    )
                                                  ) 
                                                  LEFT JOIN tm67juch_nm tm67 ON (
                                                    (
                                                      (h.juchkbn):: text = (tm67.code):: text
                                                    )
                                                  )
                                                ) 
                                              WHERE 
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              h.shukadate >= (
                                                                (
                                                                  (
                                                                    (
                                                                      to_char(
                                                                        add_months(
                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                          (
                                                                            - (36):: bigint
                                                                          )
                                                                        ), 
                                                                        ('yyyymm' :: character varying):: text
                                                                      ) || ('01' :: character varying):: text
                                                                    )
                                                                  ):: integer
                                                                ):: numeric
                                                              ):: numeric(18, 0)
                                                            ) 
                                                            AND (
                                                              h.shukadate <= (
                                                                (
                                                                  (
                                                                    to_char(
                                                                      (
                                                                        last_day(
                                                                          add_months(
                                                                            ('now' :: character varying):: timestamp without time zone, 
                                                                            (
                                                                              - (1):: bigint
                                                                            )
                                                                          )
                                                                        )
                                                                      ):: timestamp without time zone, 
                                                                      ('yyyymmdd' :: character varying):: text
                                                                    )
                                                                  ):: integer
                                                                ):: numeric
                                                              ):: numeric(18, 0)
                                                            )
                                                          ) 
                                                          AND (
                                                            h.cancelflg = (
                                                              (0):: numeric
                                                            ):: numeric(18, 0)
                                                          )
                                                        ) 
                                                        AND (
                                                          (
                                                            (
                                                              (h.torikeikbn):: text = ('03' :: character varying):: text
                                                            ) 
                                                            OR (
                                                              (h.torikeikbn):: text = ('04' :: character varying):: text
                                                            )
                                                          ) 
                                                          OR (
                                                            (h.torikeikbn):: text = ('05' :: character varying):: text
                                                          )
                                                        )
                                                      ) 
                                                      AND (
                                                        (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                      )
                                                    ) 
                                                    AND (
                                                      (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                    )
                                                  ) 
                                                  AND (
                                                    (
                                                      (
                                                        (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                      ) 
                                                      OR (
                                                        (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                      )
                                                    ) 
                                                    OR (
                                                      (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                    )
                                                  )
                                                )
                                            ) view_tuhan_tempo_shohin 
                                          WHERE 
                                            (
                                              (
                                                view_tuhan_tempo_shohin.shukadate >= (
                                                  (
                                                    (
                                                      to_char(
                                                        add_months(
                                                          ('now' :: character varying):: timestamp without time zone, 
                                                          (
                                                            - (33):: bigint
                                                          )
                                                        ), 
                                                        ('yyyymm' :: character varying):: text
                                                      )
                                                    ):: integer
                                                  ):: character varying
                                                ):: text
                                              ) 
                                              AND (
                                                view_tuhan_tempo_shohin.shukadate <= (
                                                  (
                                                    (
                                                      to_char(
                                                        add_months(
                                                          ('now' :: character varying):: timestamp without time zone, 
                                                          (
                                                            - (22):: bigint
                                                          )
                                                        ), 
                                                        ('yyyymm' :: character varying):: text
                                                      )
                                                    ):: integer
                                                  ):: character varying
                                                ):: text
                                              )
                                            ) 
                                          GROUP BY 
                                            view_tuhan_tempo_shohin.konyuchubuncode, 
                                            view_tuhan_tempo_shohin.itembunrui, 
                                            view_tuhan_tempo_shohin.juchkbn, 
                                            view_tuhan_tempo_shohin.juchkbncname, 
                                            view_tuhan_tempo_shohin.henreasoncode, 
                                            view_tuhan_tempo_shohin.henreasonname
                                        ) 
                                        UNION 
                                        SELECT 
                                          (
                                            to_char(
                                              add_months(
                                                ('now' :: character varying):: timestamp without time zone, 
                                                (
                                                  - (21):: bigint
                                                )
                                              ), 
                                              ('yyyymm' :: character varying):: text
                                            )
                                          ):: integer AS nengetu, 
                                          count(
                                            DISTINCT view_tuhan_tempo_shohin.kokyano
                                          ) AS ninzu, 
                                          '10:百貨店' AS channel, 
                                          view_tuhan_tempo_shohin.konyuchubuncode, 
                                          view_tuhan_tempo_shohin.itembunrui, 
                                          view_tuhan_tempo_shohin.juchkbn, 
                                          (
                                            view_tuhan_tempo_shohin.juchkbncname
                                          ):: character varying AS juchkbncname, 
                                          view_tuhan_tempo_shohin.henreasoncode, 
                                          (
                                            view_tuhan_tempo_shohin.henreasonname
                                          ):: character varying AS henreasonname 
                                        FROM 
                                          (
                                            SELECT 
                                              "substring"(
                                                (
                                                  (h.shukadate):: character varying
                                                ):: text, 
                                                0, 
                                                6
                                              ) AS shukadate, 
                                              h.kokyano, 
                                              h.daihanrobunname AS konyuchubuncode, 
                                              i.itembunrui, 
                                              h.juchkbn, 
                                              tm67.cname AS juchkbncname, 
                                              h.henreasoncode, 
                                              (
                                                (
                                                  (h.henreasoncode):: text || (' : ' :: character varying):: text
                                                ) || (h.henreasonname):: text
                                              ) AS henreasonname 
                                            FROM 
                                              (
                                                (
                                                  (
                                                    (
                                                      saleh_itemstrategy h 
                                                      JOIN salem_itemstrategy m ON (
                                                        (
                                                          (h.saleno):: text = (m.saleno):: text
                                                        )
                                                      )
                                                    ) 
                                                    JOIN tm14shkos "k" ON (
                                                      (
                                                        (m.itemcode):: text = ("k".itemcode):: text
                                                      )
                                                    )
                                                  ) 
                                                  LEFT JOIN tm39item_strategy i ON (
                                                    (
                                                      ("k".kosecode):: text = (i.itemcode):: text
                                                    )
                                                  )
                                                ) 
                                                LEFT JOIN tm67juch_nm tm67 ON (
                                                  (
                                                    (h.juchkbn):: text = (tm67.code):: text
                                                  )
                                                )
                                              ) 
                                            WHERE 
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            h.shukadate >= (
                                                              (
                                                                (
                                                                  (
                                                                    to_char(
                                                                      add_months(
                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                        (
                                                                          - (36):: bigint
                                                                        )
                                                                      ), 
                                                                      ('yyyymm' :: character varying):: text
                                                                    ) || ('01' :: character varying):: text
                                                                  )
                                                                ):: integer
                                                              ):: numeric
                                                            ):: numeric(18, 0)
                                                          ) 
                                                          AND (
                                                            h.shukadate <= (
                                                              (
                                                                (
                                                                  to_char(
                                                                    (
                                                                      last_day(
                                                                        add_months(
                                                                          ('now' :: character varying):: timestamp without time zone, 
                                                                          (
                                                                            - (1):: bigint
                                                                          )
                                                                        )
                                                                      )
                                                                    ):: timestamp without time zone, 
                                                                    ('yyyymmdd' :: character varying):: text
                                                                  )
                                                                ):: integer
                                                              ):: numeric
                                                            ):: numeric(18, 0)
                                                          )
                                                        ) 
                                                        AND (
                                                          h.cancelflg = (
                                                            (0):: numeric
                                                          ):: numeric(18, 0)
                                                        )
                                                      ) 
                                                      AND (
                                                        (
                                                          (
                                                            (h.torikeikbn):: text = ('03' :: character varying):: text
                                                          ) 
                                                          OR (
                                                            (h.torikeikbn):: text = ('04' :: character varying):: text
                                                          )
                                                        ) 
                                                        OR (
                                                          (h.torikeikbn):: text = ('05' :: character varying):: text
                                                        )
                                                      )
                                                    ) 
                                                    AND (
                                                      (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                    )
                                                  ) 
                                                  AND (
                                                    (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                  )
                                                ) 
                                                AND (
                                                  (
                                                    (
                                                      (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                    ) 
                                                    OR (
                                                      (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                    )
                                                  ) 
                                                  OR (
                                                    (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                  )
                                                )
                                              )
                                          ) view_tuhan_tempo_shohin 
                                        WHERE 
                                          (
                                            (
                                              view_tuhan_tempo_shohin.shukadate >= (
                                                (
                                                  (
                                                    to_char(
                                                      add_months(
                                                        ('now' :: character varying):: timestamp without time zone, 
                                                        (
                                                          - (32):: bigint
                                                        )
                                                      ), 
                                                      ('yyyymm' :: character varying):: text
                                                    )
                                                  ):: integer
                                                ):: character varying
                                              ):: text
                                            ) 
                                            AND (
                                              view_tuhan_tempo_shohin.shukadate <= (
                                                (
                                                  (
                                                    to_char(
                                                      add_months(
                                                        ('now' :: character varying):: timestamp without time zone, 
                                                        (
                                                          - (21):: bigint
                                                        )
                                                      ), 
                                                      ('yyyymm' :: character varying):: text
                                                    )
                                                  ):: integer
                                                ):: character varying
                                              ):: text
                                            )
                                          ) 
                                        GROUP BY 
                                          view_tuhan_tempo_shohin.konyuchubuncode, 
                                          view_tuhan_tempo_shohin.itembunrui, 
                                          view_tuhan_tempo_shohin.juchkbn, 
                                          view_tuhan_tempo_shohin.juchkbncname, 
                                          view_tuhan_tempo_shohin.henreasoncode, 
                                          view_tuhan_tempo_shohin.henreasonname
                                      ) 
                                      UNION 
                                      SELECT 
                                        (
                                          to_char(
                                            add_months(
                                              ('now' :: character varying):: timestamp without time zone, 
                                              (
                                                - (20):: bigint
                                              )
                                            ), 
                                            ('yyyymm' :: character varying):: text
                                          )
                                        ):: integer AS nengetu, 
                                        count(
                                          DISTINCT view_tuhan_tempo_shohin.kokyano
                                        ) AS ninzu, 
                                        '10:百貨店' AS channel, 
                                        view_tuhan_tempo_shohin.konyuchubuncode, 
                                        view_tuhan_tempo_shohin.itembunrui, 
                                        view_tuhan_tempo_shohin.juchkbn, 
                                        (
                                          view_tuhan_tempo_shohin.juchkbncname
                                        ):: character varying AS juchkbncname, 
                                        view_tuhan_tempo_shohin.henreasoncode, 
                                        (
                                          view_tuhan_tempo_shohin.henreasonname
                                        ):: character varying AS henreasonname 
                                      FROM 
                                        (
                                          SELECT 
                                            "substring"(
                                              (
                                                (h.shukadate):: character varying
                                              ):: text, 
                                              0, 
                                              6
                                            ) AS shukadate, 
                                            h.kokyano, 
                                            h.daihanrobunname AS konyuchubuncode, 
                                            i.itembunrui, 
                                            h.juchkbn, 
                                            tm67.cname AS juchkbncname, 
                                            h.henreasoncode, 
                                            (
                                              (
                                                (h.henreasoncode):: text || (' : ' :: character varying):: text
                                              ) || (h.henreasonname):: text
                                            ) AS henreasonname 
                                          FROM 
                                            (
                                              (
                                                (
                                                  (
                                                    saleh_itemstrategy h 
                                                    JOIN salem_itemstrategy m ON (
                                                      (
                                                        (h.saleno):: text = (m.saleno):: text
                                                      )
                                                    )
                                                  ) 
                                                  JOIN tm14shkos "k" ON (
                                                    (
                                                      (m.itemcode):: text = ("k".itemcode):: text
                                                    )
                                                  )
                                                ) 
                                                LEFT JOIN tm39item_strategy i ON (
                                                  (
                                                    ("k".kosecode):: text = (i.itemcode):: text
                                                  )
                                                )
                                              ) 
                                              LEFT JOIN tm67juch_nm tm67 ON (
                                                (
                                                  (h.juchkbn):: text = (tm67.code):: text
                                                )
                                              )
                                            ) 
                                          WHERE 
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          h.shukadate >= (
                                                            (
                                                              (
                                                                (
                                                                  to_char(
                                                                    add_months(
                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                      (
                                                                        - (36):: bigint
                                                                      )
                                                                    ), 
                                                                    ('yyyymm' :: character varying):: text
                                                                  ) || ('01' :: character varying):: text
                                                                )
                                                              ):: integer
                                                            ):: numeric
                                                          ):: numeric(18, 0)
                                                        ) 
                                                        AND (
                                                          h.shukadate <= (
                                                            (
                                                              (
                                                                to_char(
                                                                  (
                                                                    last_day(
                                                                      add_months(
                                                                        ('now' :: character varying):: timestamp without time zone, 
                                                                        (
                                                                          - (1):: bigint
                                                                        )
                                                                      )
                                                                    )
                                                                  ):: timestamp without time zone, 
                                                                  ('yyyymmdd' :: character varying):: text
                                                                )
                                                              ):: integer
                                                            ):: numeric
                                                          ):: numeric(18, 0)
                                                        )
                                                      ) 
                                                      AND (
                                                        h.cancelflg = (
                                                          (0):: numeric
                                                        ):: numeric(18, 0)
                                                      )
                                                    ) 
                                                    AND (
                                                      (
                                                        (
                                                          (h.torikeikbn):: text = ('03' :: character varying):: text
                                                        ) 
                                                        OR (
                                                          (h.torikeikbn):: text = ('04' :: character varying):: text
                                                        )
                                                      ) 
                                                      OR (
                                                        (h.torikeikbn):: text = ('05' :: character varying):: text
                                                      )
                                                    )
                                                  ) 
                                                  AND (
                                                    (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                  )
                                                ) 
                                                AND (
                                                  (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                                )
                                              ) 
                                              AND (
                                                (
                                                  (
                                                    (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                  ) 
                                                  OR (
                                                    (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                  )
                                                ) 
                                                OR (
                                                  (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                                )
                                              )
                                            )
                                        ) view_tuhan_tempo_shohin 
                                      WHERE 
                                        (
                                          (
                                            view_tuhan_tempo_shohin.shukadate >= (
                                              (
                                                (
                                                  to_char(
                                                    add_months(
                                                      ('now' :: character varying):: timestamp without time zone, 
                                                      (
                                                        - (31):: bigint
                                                      )
                                                    ), 
                                                    ('yyyymm' :: character varying):: text
                                                  )
                                                ):: integer
                                              ):: character varying
                                            ):: text
                                          ) 
                                          AND (
                                            view_tuhan_tempo_shohin.shukadate <= (
                                              (
                                                (
                                                  to_char(
                                                    add_months(
                                                      ('now' :: character varying):: timestamp without time zone, 
                                                      (
                                                        - (20):: bigint
                                                      )
                                                    ), 
                                                    ('yyyymm' :: character varying):: text
                                                  )
                                                ):: integer
                                              ):: character varying
                                            ):: text
                                          )
                                        ) 
                                      GROUP BY 
                                        view_tuhan_tempo_shohin.konyuchubuncode, 
                                        view_tuhan_tempo_shohin.itembunrui, 
                                        view_tuhan_tempo_shohin.juchkbn, 
                                        view_tuhan_tempo_shohin.juchkbncname, 
                                        view_tuhan_tempo_shohin.henreasoncode, 
                                        view_tuhan_tempo_shohin.henreasonname
                                    ) 
                                    UNION 
                                    SELECT 
                                      (
                                        to_char(
                                          add_months(
                                            ('now' :: character varying):: timestamp without time zone, 
                                            (
                                              - (19):: bigint
                                            )
                                          ), 
                                          ('yyyymm' :: character varying):: text
                                        )
                                      ):: integer AS nengetu, 
                                      count(
                                        DISTINCT view_tuhan_tempo_shohin.kokyano
                                      ) AS ninzu, 
                                      '10:百貨店' AS channel, 
                                      view_tuhan_tempo_shohin.konyuchubuncode, 
                                      view_tuhan_tempo_shohin.itembunrui, 
                                      view_tuhan_tempo_shohin.juchkbn, 
                                      (
                                        view_tuhan_tempo_shohin.juchkbncname
                                      ):: character varying AS juchkbncname, 
                                      view_tuhan_tempo_shohin.henreasoncode, 
                                      (
                                        view_tuhan_tempo_shohin.henreasonname
                                      ):: character varying AS henreasonname 
                                    FROM 
                                      (
                                        SELECT 
                                          "substring"(
                                            (
                                              (h.shukadate):: character varying
                                            ):: text, 
                                            0, 
                                            6
                                          ) AS shukadate, 
                                          h.kokyano, 
                                          h.daihanrobunname AS konyuchubuncode, 
                                          i.itembunrui, 
                                          h.juchkbn, 
                                          tm67.cname AS juchkbncname, 
                                          h.henreasoncode, 
                                          (
                                            (
                                              (h.henreasoncode):: text || (' : ' :: character varying):: text
                                            ) || (h.henreasonname):: text
                                          ) AS henreasonname 
                                        FROM 
                                          (
                                            (
                                              (
                                                (
                                                  saleh_itemstrategy h 
                                                  JOIN salem_itemstrategy m ON (
                                                    (
                                                      (h.saleno):: text = (m.saleno):: text
                                                    )
                                                  )
                                                ) 
                                                JOIN tm14shkos "k" ON (
                                                  (
                                                    (m.itemcode):: text = ("k".itemcode):: text
                                                  )
                                                )
                                              ) 
                                              LEFT JOIN tm39item_strategy i ON (
                                                (
                                                  ("k".kosecode):: text = (i.itemcode):: text
                                                )
                                              )
                                            ) 
                                            LEFT JOIN tm67juch_nm tm67 ON (
                                              (
                                                (h.juchkbn):: text = (tm67.code):: text
                                              )
                                            )
                                          ) 
                                        WHERE 
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        h.shukadate >= (
                                                          (
                                                            (
                                                              (
                                                                to_char(
                                                                  add_months(
                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                    (
                                                                      - (36):: bigint
                                                                    )
                                                                  ), 
                                                                  ('yyyymm' :: character varying):: text
                                                                ) || ('01' :: character varying):: text
                                                              )
                                                            ):: integer
                                                          ):: numeric
                                                        ):: numeric(18, 0)
                                                      ) 
                                                      AND (
                                                        h.shukadate <= (
                                                          (
                                                            (
                                                              to_char(
                                                                (
                                                                  last_day(
                                                                    add_months(
                                                                      ('now' :: character varying):: timestamp without time zone, 
                                                                      (
                                                                        - (1):: bigint
                                                                      )
                                                                    )
                                                                  )
                                                                ):: timestamp without time zone, 
                                                                ('yyyymmdd' :: character varying):: text
                                                              )
                                                            ):: integer
                                                          ):: numeric
                                                        ):: numeric(18, 0)
                                                      )
                                                    ) 
                                                    AND (
                                                      h.cancelflg = (
                                                        (0):: numeric
                                                      ):: numeric(18, 0)
                                                    )
                                                  ) 
                                                  AND (
                                                    (
                                                      (
                                                        (h.torikeikbn):: text = ('03' :: character varying):: text
                                                      ) 
                                                      OR (
                                                        (h.torikeikbn):: text = ('04' :: character varying):: text
                                                      )
                                                    ) 
                                                    OR (
                                                      (h.torikeikbn):: text = ('05' :: character varying):: text
                                                    )
                                                  )
                                                ) 
                                                AND (
                                                  (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                                )
                                              ) 
                                              AND (
                                                (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                              )
                                            ) 
                                            AND (
                                              (
                                                (
                                                  (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                                ) 
                                                OR (
                                                  (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                                )
                                              ) 
                                              OR (
                                                (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                              )
                                            )
                                          )
                                      ) view_tuhan_tempo_shohin 
                                    WHERE 
                                      (
                                        (
                                          view_tuhan_tempo_shohin.shukadate >= (
                                            (
                                              (
                                                to_char(
                                                  add_months(
                                                    ('now' :: character varying):: timestamp without time zone, 
                                                    (
                                                      - (30):: bigint
                                                    )
                                                  ), 
                                                  ('yyyymm' :: character varying):: text
                                                )
                                              ):: integer
                                            ):: character varying
                                          ):: text
                                        ) 
                                        AND (
                                          view_tuhan_tempo_shohin.shukadate <= (
                                            (
                                              (
                                                to_char(
                                                  add_months(
                                                    ('now' :: character varying):: timestamp without time zone, 
                                                    (
                                                      - (19):: bigint
                                                    )
                                                  ), 
                                                  ('yyyymm' :: character varying):: text
                                                )
                                              ):: integer
                                            ):: character varying
                                          ):: text
                                        )
                                      ) 
                                    GROUP BY 
                                      view_tuhan_tempo_shohin.konyuchubuncode, 
                                      view_tuhan_tempo_shohin.itembunrui, 
                                      view_tuhan_tempo_shohin.juchkbn, 
                                      view_tuhan_tempo_shohin.juchkbncname, 
                                      view_tuhan_tempo_shohin.henreasoncode, 
                                      view_tuhan_tempo_shohin.henreasonname
                                  ) 
                                  UNION 
                                  SELECT 
                                    (
                                      to_char(
                                        add_months(
                                          ('now' :: character varying):: timestamp without time zone, 
                                          (
                                            - (18):: bigint
                                          )
                                        ), 
                                        ('yyyymm' :: character varying):: text
                                      )
                                    ):: integer AS nengetu, 
                                    count(
                                      DISTINCT view_tuhan_tempo_shohin.kokyano
                                    ) AS ninzu, 
                                    '10:百貨店' AS channel, 
                                    view_tuhan_tempo_shohin.konyuchubuncode, 
                                    view_tuhan_tempo_shohin.itembunrui, 
                                    view_tuhan_tempo_shohin.juchkbn, 
                                    (
                                      view_tuhan_tempo_shohin.juchkbncname
                                    ):: character varying AS juchkbncname, 
                                    view_tuhan_tempo_shohin.henreasoncode, 
                                    (
                                      view_tuhan_tempo_shohin.henreasonname
                                    ):: character varying AS henreasonname 
                                  FROM 
                                    (
                                      SELECT 
                                        "substring"(
                                          (
                                            (h.shukadate):: character varying
                                          ):: text, 
                                          0, 
                                          6
                                        ) AS shukadate, 
                                        h.kokyano, 
                                        h.daihanrobunname AS konyuchubuncode, 
                                        i.itembunrui, 
                                        h.juchkbn, 
                                        tm67.cname AS juchkbncname, 
                                        h.henreasoncode, 
                                        (
                                          (
                                            (h.henreasoncode):: text || (' : ' :: character varying):: text
                                          ) || (h.henreasonname):: text
                                        ) AS henreasonname 
                                      FROM 
                                        (
                                          (
                                            (
                                              (
                                                saleh_itemstrategy h 
                                                JOIN salem_itemstrategy m ON (
                                                  (
                                                    (h.saleno):: text = (m.saleno):: text
                                                  )
                                                )
                                              ) 
                                              JOIN tm14shkos "k" ON (
                                                (
                                                  (m.itemcode):: text = ("k".itemcode):: text
                                                )
                                              )
                                            ) 
                                            LEFT JOIN tm39item_strategy i ON (
                                              (
                                                ("k".kosecode):: text = (i.itemcode):: text
                                              )
                                            )
                                          ) 
                                          LEFT JOIN tm67juch_nm tm67 ON (
                                            (
                                              (h.juchkbn):: text = (tm67.code):: text
                                            )
                                          )
                                        ) 
                                      WHERE 
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      h.shukadate >= (
                                                        (
                                                          (
                                                            (
                                                              to_char(
                                                                add_months(
                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                  (
                                                                    - (36):: bigint
                                                                  )
                                                                ), 
                                                                ('yyyymm' :: character varying):: text
                                                              ) || ('01' :: character varying):: text
                                                            )
                                                          ):: integer
                                                        ):: numeric
                                                      ):: numeric(18, 0)
                                                    ) 
                                                    AND (
                                                      h.shukadate <= (
                                                        (
                                                          (
                                                            to_char(
                                                              (
                                                                last_day(
                                                                  add_months(
                                                                    ('now' :: character varying):: timestamp without time zone, 
                                                                    (
                                                                      - (1):: bigint
                                                                    )
                                                                  )
                                                                )
                                                              ):: timestamp without time zone, 
                                                              ('yyyymmdd' :: character varying):: text
                                                            )
                                                          ):: integer
                                                        ):: numeric
                                                      ):: numeric(18, 0)
                                                    )
                                                  ) 
                                                  AND (
                                                    h.cancelflg = (
                                                      (0):: numeric
                                                    ):: numeric(18, 0)
                                                  )
                                                ) 
                                                AND (
                                                  (
                                                    (
                                                      (h.torikeikbn):: text = ('03' :: character varying):: text
                                                    ) 
                                                    OR (
                                                      (h.torikeikbn):: text = ('04' :: character varying):: text
                                                    )
                                                  ) 
                                                  OR (
                                                    (h.torikeikbn):: text = ('05' :: character varying):: text
                                                  )
                                                )
                                              ) 
                                              AND (
                                                (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                              )
                                            ) 
                                            AND (
                                              (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                            )
                                          ) 
                                          AND (
                                            (
                                              (
                                                (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                              ) 
                                              OR (
                                                (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                              )
                                            ) 
                                            OR (
                                              (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                            )
                                          )
                                        )
                                    ) view_tuhan_tempo_shohin 
                                  WHERE 
                                    (
                                      (
                                        view_tuhan_tempo_shohin.shukadate >= (
                                          (
                                            (
                                              to_char(
                                                add_months(
                                                  ('now' :: character varying):: timestamp without time zone, 
                                                  (
                                                    - (29):: bigint
                                                  )
                                                ), 
                                                ('yyyymm' :: character varying):: text
                                              )
                                            ):: integer
                                          ):: character varying
                                        ):: text
                                      ) 
                                      AND (
                                        view_tuhan_tempo_shohin.shukadate <= (
                                          (
                                            (
                                              to_char(
                                                add_months(
                                                  ('now' :: character varying):: timestamp without time zone, 
                                                  (
                                                    - (18):: bigint
                                                  )
                                                ), 
                                                ('yyyymm' :: character varying):: text
                                              )
                                            ):: integer
                                          ):: character varying
                                        ):: text
                                      )
                                    ) 
                                  GROUP BY 
                                    view_tuhan_tempo_shohin.konyuchubuncode, 
                                    view_tuhan_tempo_shohin.itembunrui, 
                                    view_tuhan_tempo_shohin.juchkbn, 
                                    view_tuhan_tempo_shohin.juchkbncname, 
                                    view_tuhan_tempo_shohin.henreasoncode, 
                                    view_tuhan_tempo_shohin.henreasonname
                                ) 
                                UNION 
                                SELECT 
                                  (
                                    to_char(
                                      add_months(
                                        ('now' :: character varying):: timestamp without time zone, 
                                        (
                                          - (17):: bigint
                                        )
                                      ), 
                                      ('yyyymm' :: character varying):: text
                                    )
                                  ):: integer AS nengetu, 
                                  count(
                                    DISTINCT view_tuhan_tempo_shohin.kokyano
                                  ) AS ninzu, 
                                  '10:百貨店' AS channel, 
                                  view_tuhan_tempo_shohin.konyuchubuncode, 
                                  view_tuhan_tempo_shohin.itembunrui, 
                                  view_tuhan_tempo_shohin.juchkbn, 
                                  (
                                    view_tuhan_tempo_shohin.juchkbncname
                                  ):: character varying AS juchkbncname, 
                                  view_tuhan_tempo_shohin.henreasoncode, 
                                  (
                                    view_tuhan_tempo_shohin.henreasonname
                                  ):: character varying AS henreasonname 
                                FROM 
                                  (
                                    SELECT 
                                      "substring"(
                                        (
                                          (h.shukadate):: character varying
                                        ):: text, 
                                        0, 
                                        6
                                      ) AS shukadate, 
                                      h.kokyano, 
                                      h.daihanrobunname AS konyuchubuncode, 
                                      i.itembunrui, 
                                      h.juchkbn, 
                                      tm67.cname AS juchkbncname, 
                                      h.henreasoncode, 
                                      (
                                        (
                                          (h.henreasoncode):: text || (' : ' :: character varying):: text
                                        ) || (h.henreasonname):: text
                                      ) AS henreasonname 
                                    FROM 
                                      (
                                        (
                                          (
                                            (
                                              saleh_itemstrategy h 
                                              JOIN salem_itemstrategy m ON (
                                                (
                                                  (h.saleno):: text = (m.saleno):: text
                                                )
                                              )
                                            ) 
                                            JOIN tm14shkos "k" ON (
                                              (
                                                (m.itemcode):: text = ("k".itemcode):: text
                                              )
                                            )
                                          ) 
                                          LEFT JOIN tm39item_strategy i ON (
                                            (
                                              ("k".kosecode):: text = (i.itemcode):: text
                                            )
                                          )
                                        ) 
                                        LEFT JOIN tm67juch_nm tm67 ON (
                                          (
                                            (h.juchkbn):: text = (tm67.code):: text
                                          )
                                        )
                                      ) 
                                    WHERE 
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    h.shukadate >= (
                                                      (
                                                        (
                                                          (
                                                            to_char(
                                                              add_months(
                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                (
                                                                  - (36):: bigint
                                                                )
                                                              ), 
                                                              ('yyyymm' :: character varying):: text
                                                            ) || ('01' :: character varying):: text
                                                          )
                                                        ):: integer
                                                      ):: numeric
                                                    ):: numeric(18, 0)
                                                  ) 
                                                  AND (
                                                    h.shukadate <= (
                                                      (
                                                        (
                                                          to_char(
                                                            (
                                                              last_day(
                                                                add_months(
                                                                  ('now' :: character varying):: timestamp without time zone, 
                                                                  (
                                                                    - (1):: bigint
                                                                  )
                                                                )
                                                              )
                                                            ):: timestamp without time zone, 
                                                            ('yyyymmdd' :: character varying):: text
                                                          )
                                                        ):: integer
                                                      ):: numeric
                                                    ):: numeric(18, 0)
                                                  )
                                                ) 
                                                AND (
                                                  h.cancelflg = (
                                                    (0):: numeric
                                                  ):: numeric(18, 0)
                                                )
                                              ) 
                                              AND (
                                                (
                                                  (
                                                    (h.torikeikbn):: text = ('03' :: character varying):: text
                                                  ) 
                                                  OR (
                                                    (h.torikeikbn):: text = ('04' :: character varying):: text
                                                  )
                                                ) 
                                                OR (
                                                  (h.torikeikbn):: text = ('05' :: character varying):: text
                                                )
                                              )
                                            ) 
                                            AND (
                                              (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                            )
                                          ) 
                                          AND (
                                            (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                          )
                                        ) 
                                        AND (
                                          (
                                            (
                                              (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                            ) 
                                            OR (
                                              (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                            )
                                          ) 
                                          OR (
                                            (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                          )
                                        )
                                      )
                                  ) view_tuhan_tempo_shohin 
                                WHERE 
                                  (
                                    (
                                      view_tuhan_tempo_shohin.shukadate >= (
                                        (
                                          (
                                            to_char(
                                              add_months(
                                                ('now' :: character varying):: timestamp without time zone, 
                                                (
                                                  - (28):: bigint
                                                )
                                              ), 
                                              ('yyyymm' :: character varying):: text
                                            )
                                          ):: integer
                                        ):: character varying
                                      ):: text
                                    ) 
                                    AND (
                                      view_tuhan_tempo_shohin.shukadate <= (
                                        (
                                          (
                                            to_char(
                                              add_months(
                                                ('now' :: character varying):: timestamp without time zone, 
                                                (
                                                  - (17):: bigint
                                                )
                                              ), 
                                              ('yyyymm' :: character varying):: text
                                            )
                                          ):: integer
                                        ):: character varying
                                      ):: text
                                    )
                                  ) 
                                GROUP BY 
                                  view_tuhan_tempo_shohin.konyuchubuncode, 
                                  view_tuhan_tempo_shohin.itembunrui, 
                                  view_tuhan_tempo_shohin.juchkbn, 
                                  view_tuhan_tempo_shohin.juchkbncname, 
                                  view_tuhan_tempo_shohin.henreasoncode, 
                                  view_tuhan_tempo_shohin.henreasonname
                              ) 
                              UNION 
                              SELECT 
                                (
                                  to_char(
                                    add_months(
                                      ('now' :: character varying):: timestamp without time zone, 
                                      (
                                        - (16):: bigint
                                      )
                                    ), 
                                    ('yyyymm' :: character varying):: text
                                  )
                                ):: integer AS nengetu, 
                                count(
                                  DISTINCT view_tuhan_tempo_shohin.kokyano
                                ) AS ninzu, 
                                '10:百貨店' AS channel, 
                                view_tuhan_tempo_shohin.konyuchubuncode, 
                                view_tuhan_tempo_shohin.itembunrui, 
                                view_tuhan_tempo_shohin.juchkbn, 
                                (
                                  view_tuhan_tempo_shohin.juchkbncname
                                ):: character varying AS juchkbncname, 
                                view_tuhan_tempo_shohin.henreasoncode, 
                                (
                                  view_tuhan_tempo_shohin.henreasonname
                                ):: character varying AS henreasonname 
                              FROM 
                                (
                                  SELECT 
                                    "substring"(
                                      (
                                        (h.shukadate):: character varying
                                      ):: text, 
                                      0, 
                                      6
                                    ) AS shukadate, 
                                    h.kokyano, 
                                    h.daihanrobunname AS konyuchubuncode, 
                                    i.itembunrui, 
                                    h.juchkbn, 
                                    tm67.cname AS juchkbncname, 
                                    h.henreasoncode, 
                                    (
                                      (
                                        (h.henreasoncode):: text || (' : ' :: character varying):: text
                                      ) || (h.henreasonname):: text
                                    ) AS henreasonname 
                                  FROM 
                                    (
                                      (
                                        (
                                          (
                                            saleh_itemstrategy h 
                                            JOIN salem_itemstrategy m ON (
                                              (
                                                (h.saleno):: text = (m.saleno):: text
                                              )
                                            )
                                          ) 
                                          JOIN tm14shkos "k" ON (
                                            (
                                              (m.itemcode):: text = ("k".itemcode):: text
                                            )
                                          )
                                        ) 
                                        LEFT JOIN tm39item_strategy i ON (
                                          (
                                            ("k".kosecode):: text = (i.itemcode):: text
                                          )
                                        )
                                      ) 
                                      LEFT JOIN tm67juch_nm tm67 ON (
                                        (
                                          (h.juchkbn):: text = (tm67.code):: text
                                        )
                                      )
                                    ) 
                                  WHERE 
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  h.shukadate >= (
                                                    (
                                                      (
                                                        (
                                                          to_char(
                                                            add_months(
                                                              ('now' :: character varying):: timestamp without time zone, 
                                                              (
                                                                - (36):: bigint
                                                              )
                                                            ), 
                                                            ('yyyymm' :: character varying):: text
                                                          ) || ('01' :: character varying):: text
                                                        )
                                                      ):: integer
                                                    ):: numeric
                                                  ):: numeric(18, 0)
                                                ) 
                                                AND (
                                                  h.shukadate <= (
                                                    (
                                                      (
                                                        to_char(
                                                          (
                                                            last_day(
                                                              add_months(
                                                                ('now' :: character varying):: timestamp without time zone, 
                                                                (
                                                                  - (1):: bigint
                                                                )
                                                              )
                                                            )
                                                          ):: timestamp without time zone, 
                                                          ('yyyymmdd' :: character varying):: text
                                                        )
                                                      ):: integer
                                                    ):: numeric
                                                  ):: numeric(18, 0)
                                                )
                                              ) 
                                              AND (
                                                h.cancelflg = (
                                                  (0):: numeric
                                                ):: numeric(18, 0)
                                              )
                                            ) 
                                            AND (
                                              (
                                                (
                                                  (h.torikeikbn):: text = ('03' :: character varying):: text
                                                ) 
                                                OR (
                                                  (h.torikeikbn):: text = ('04' :: character varying):: text
                                                )
                                              ) 
                                              OR (
                                                (h.torikeikbn):: text = ('05' :: character varying):: text
                                              )
                                            )
                                          ) 
                                          AND (
                                            (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                          )
                                        ) 
                                        AND (
                                          (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                        )
                                      ) 
                                      AND (
                                        (
                                          (
                                            (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                          ) 
                                          OR (
                                            (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                          )
                                        ) 
                                        OR (
                                          (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                        )
                                      )
                                    )
                                ) view_tuhan_tempo_shohin 
                              WHERE 
                                (
                                  (
                                    view_tuhan_tempo_shohin.shukadate >= (
                                      (
                                        (
                                          to_char(
                                            add_months(
                                              ('now' :: character varying):: timestamp without time zone, 
                                              (
                                                - (27):: bigint
                                              )
                                            ), 
                                            ('yyyymm' :: character varying):: text
                                          )
                                        ):: integer
                                      ):: character varying
                                    ):: text
                                  ) 
                                  AND (
                                    view_tuhan_tempo_shohin.shukadate <= (
                                      (
                                        (
                                          to_char(
                                            add_months(
                                              ('now' :: character varying):: timestamp without time zone, 
                                              (
                                                - (16):: bigint
                                              )
                                            ), 
                                            ('yyyymm' :: character varying):: text
                                          )
                                        ):: integer
                                      ):: character varying
                                    ):: text
                                  )
                                ) 
                              GROUP BY 
                                view_tuhan_tempo_shohin.konyuchubuncode, 
                                view_tuhan_tempo_shohin.itembunrui, 
                                view_tuhan_tempo_shohin.juchkbn, 
                                view_tuhan_tempo_shohin.juchkbncname, 
                                view_tuhan_tempo_shohin.henreasoncode, 
                                view_tuhan_tempo_shohin.henreasonname
                            ) 
                            UNION 
                            SELECT 
                              (
                                to_char(
                                  add_months(
                                    ('now' :: character varying):: timestamp without time zone, 
                                    (
                                      - (15):: bigint
                                    )
                                  ), 
                                  ('yyyymm' :: character varying):: text
                                )
                              ):: integer AS nengetu, 
                              count(
                                DISTINCT view_tuhan_tempo_shohin.kokyano
                              ) AS ninzu, 
                              '10:百貨店' AS channel, 
                              view_tuhan_tempo_shohin.konyuchubuncode, 
                              view_tuhan_tempo_shohin.itembunrui, 
                              view_tuhan_tempo_shohin.juchkbn, 
                              (
                                view_tuhan_tempo_shohin.juchkbncname
                              ):: character varying AS juchkbncname, 
                              view_tuhan_tempo_shohin.henreasoncode, 
                              (
                                view_tuhan_tempo_shohin.henreasonname
                              ):: character varying AS henreasonname 
                            FROM 
                              (
                                SELECT 
                                  "substring"(
                                    (
                                      (h.shukadate):: character varying
                                    ):: text, 
                                    0, 
                                    6
                                  ) AS shukadate, 
                                  h.kokyano, 
                                  h.daihanrobunname AS konyuchubuncode, 
                                  i.itembunrui, 
                                  h.juchkbn, 
                                  tm67.cname AS juchkbncname, 
                                  h.henreasoncode, 
                                  (
                                    (
                                      (h.henreasoncode):: text || (' : ' :: character varying):: text
                                    ) || (h.henreasonname):: text
                                  ) AS henreasonname 
                                FROM 
                                  (
                                    (
                                      (
                                        (
                                          saleh_itemstrategy h 
                                          JOIN salem_itemstrategy m ON (
                                            (
                                              (h.saleno):: text = (m.saleno):: text
                                            )
                                          )
                                        ) 
                                        JOIN tm14shkos "k" ON (
                                          (
                                            (m.itemcode):: text = ("k".itemcode):: text
                                          )
                                        )
                                      ) 
                                      LEFT JOIN tm39item_strategy i ON (
                                        (
                                          ("k".kosecode):: text = (i.itemcode):: text
                                        )
                                      )
                                    ) 
                                    LEFT JOIN tm67juch_nm tm67 ON (
                                      (
                                        (h.juchkbn):: text = (tm67.code):: text
                                      )
                                    )
                                  ) 
                                WHERE 
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                h.shukadate >= (
                                                  (
                                                    (
                                                      (
                                                        to_char(
                                                          add_months(
                                                            ('now' :: character varying):: timestamp without time zone, 
                                                            (
                                                              - (36):: bigint
                                                            )
                                                          ), 
                                                          ('yyyymm' :: character varying):: text
                                                        ) || ('01' :: character varying):: text
                                                      )
                                                    ):: integer
                                                  ):: numeric
                                                ):: numeric(18, 0)
                                              ) 
                                              AND (
                                                h.shukadate <= (
                                                  (
                                                    (
                                                      to_char(
                                                        (
                                                          last_day(
                                                            add_months(
                                                              ('now' :: character varying):: timestamp without time zone, 
                                                              (
                                                                - (1):: bigint
                                                              )
                                                            )
                                                          )
                                                        ):: timestamp without time zone, 
                                                        ('yyyymmdd' :: character varying):: text
                                                      )
                                                    ):: integer
                                                  ):: numeric
                                                ):: numeric(18, 0)
                                              )
                                            ) 
                                            AND (
                                              h.cancelflg = (
                                                (0):: numeric
                                              ):: numeric(18, 0)
                                            )
                                          ) 
                                          AND (
                                            (
                                              (
                                                (h.torikeikbn):: text = ('03' :: character varying):: text
                                              ) 
                                              OR (
                                                (h.torikeikbn):: text = ('04' :: character varying):: text
                                              )
                                            ) 
                                            OR (
                                              (h.torikeikbn):: text = ('05' :: character varying):: text
                                            )
                                          )
                                        ) 
                                        AND (
                                          (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                        )
                                      ) 
                                      AND (
                                        (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                      )
                                    ) 
                                    AND (
                                      (
                                        (
                                          (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                        ) 
                                        OR (
                                          (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                        )
                                      ) 
                                      OR (
                                        (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                      )
                                    )
                                  )
                              ) view_tuhan_tempo_shohin 
                            WHERE 
                              (
                                (
                                  view_tuhan_tempo_shohin.shukadate >= (
                                    (
                                      (
                                        to_char(
                                          add_months(
                                            ('now' :: character varying):: timestamp without time zone, 
                                            (
                                              - (26):: bigint
                                            )
                                          ), 
                                          ('yyyymm' :: character varying):: text
                                        )
                                      ):: integer
                                    ):: character varying
                                  ):: text
                                ) 
                                AND (
                                  view_tuhan_tempo_shohin.shukadate <= (
                                    (
                                      (
                                        to_char(
                                          add_months(
                                            ('now' :: character varying):: timestamp without time zone, 
                                            (
                                              - (15):: bigint
                                            )
                                          ), 
                                          ('yyyymm' :: character varying):: text
                                        )
                                      ):: integer
                                    ):: character varying
                                  ):: text
                                )
                              ) 
                            GROUP BY 
                              view_tuhan_tempo_shohin.konyuchubuncode, 
                              view_tuhan_tempo_shohin.itembunrui, 
                              view_tuhan_tempo_shohin.juchkbn, 
                              view_tuhan_tempo_shohin.juchkbncname, 
                              view_tuhan_tempo_shohin.henreasoncode, 
                              view_tuhan_tempo_shohin.henreasonname
                          ) 
                          UNION 
                          SELECT 
                            (
                              to_char(
                                add_months(
                                  ('now' :: character varying):: timestamp without time zone, 
                                  (
                                    - (14):: bigint
                                  )
                                ), 
                                ('yyyymm' :: character varying):: text
                              )
                            ):: integer AS nengetu, 
                            count(
                              DISTINCT view_tuhan_tempo_shohin.kokyano
                            ) AS ninzu, 
                            '10:百貨店' AS channel, 
                            view_tuhan_tempo_shohin.konyuchubuncode, 
                            view_tuhan_tempo_shohin.itembunrui, 
                            view_tuhan_tempo_shohin.juchkbn, 
                            (
                              view_tuhan_tempo_shohin.juchkbncname
                            ):: character varying AS juchkbncname, 
                            view_tuhan_tempo_shohin.henreasoncode, 
                            (
                              view_tuhan_tempo_shohin.henreasonname
                            ):: character varying AS henreasonname 
                          FROM 
                            (
                              SELECT 
                                "substring"(
                                  (
                                    (h.shukadate):: character varying
                                  ):: text, 
                                  0, 
                                  6
                                ) AS shukadate, 
                                h.kokyano, 
                                h.daihanrobunname AS konyuchubuncode, 
                                i.itembunrui, 
                                h.juchkbn, 
                                tm67.cname AS juchkbncname, 
                                h.henreasoncode, 
                                (
                                  (
                                    (h.henreasoncode):: text || (' : ' :: character varying):: text
                                  ) || (h.henreasonname):: text
                                ) AS henreasonname 
                              FROM 
                                (
                                  (
                                    (
                                      (
                                        saleh_itemstrategy h 
                                        JOIN salem_itemstrategy m ON (
                                          (
                                            (h.saleno):: text = (m.saleno):: text
                                          )
                                        )
                                      ) 
                                      JOIN tm14shkos "k" ON (
                                        (
                                          (m.itemcode):: text = ("k".itemcode):: text
                                        )
                                      )
                                    ) 
                                    LEFT JOIN tm39item_strategy i ON (
                                      (
                                        ("k".kosecode):: text = (i.itemcode):: text
                                      )
                                    )
                                  ) 
                                  LEFT JOIN tm67juch_nm tm67 ON (
                                    (
                                      (h.juchkbn):: text = (tm67.code):: text
                                    )
                                  )
                                ) 
                              WHERE 
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              h.shukadate >= (
                                                (
                                                  (
                                                    (
                                                      to_char(
                                                        add_months(
                                                          ('now' :: character varying):: timestamp without time zone, 
                                                          (
                                                            - (36):: bigint
                                                          )
                                                        ), 
                                                        ('yyyymm' :: character varying):: text
                                                      ) || ('01' :: character varying):: text
                                                    )
                                                  ):: integer
                                                ):: numeric
                                              ):: numeric(18, 0)
                                            ) 
                                            AND (
                                              h.shukadate <= (
                                                (
                                                  (
                                                    to_char(
                                                      (
                                                        last_day(
                                                          add_months(
                                                            ('now' :: character varying):: timestamp without time zone, 
                                                            (
                                                              - (1):: bigint
                                                            )
                                                          )
                                                        )
                                                      ):: timestamp without time zone, 
                                                      ('yyyymmdd' :: character varying):: text
                                                    )
                                                  ):: integer
                                                ):: numeric
                                              ):: numeric(18, 0)
                                            )
                                          ) 
                                          AND (
                                            h.cancelflg = (
                                              (0):: numeric
                                            ):: numeric(18, 0)
                                          )
                                        ) 
                                        AND (
                                          (
                                            (
                                              (h.torikeikbn):: text = ('03' :: character varying):: text
                                            ) 
                                            OR (
                                              (h.torikeikbn):: text = ('04' :: character varying):: text
                                            )
                                          ) 
                                          OR (
                                            (h.torikeikbn):: text = ('05' :: character varying):: text
                                          )
                                        )
                                      ) 
                                      AND (
                                        (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                      )
                                    ) 
                                    AND (
                                      (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                    )
                                  ) 
                                  AND (
                                    (
                                      (
                                        (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                      ) 
                                      OR (
                                        (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                      )
                                    ) 
                                    OR (
                                      (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                    )
                                  )
                                )
                            ) view_tuhan_tempo_shohin 
                          WHERE 
                            (
                              (
                                view_tuhan_tempo_shohin.shukadate >= (
                                  (
                                    (
                                      to_char(
                                        add_months(
                                          ('now' :: character varying):: timestamp without time zone, 
                                          (
                                            - (25):: bigint
                                          )
                                        ), 
                                        ('yyyymm' :: character varying):: text
                                      )
                                    ):: integer
                                  ):: character varying
                                ):: text
                              ) 
                              AND (
                                view_tuhan_tempo_shohin.shukadate <= (
                                  (
                                    (
                                      to_char(
                                        add_months(
                                          ('now' :: character varying):: timestamp without time zone, 
                                          (
                                            - (14):: bigint
                                          )
                                        ), 
                                        ('yyyymm' :: character varying):: text
                                      )
                                    ):: integer
                                  ):: character varying
                                ):: text
                              )
                            ) 
                          GROUP BY 
                            view_tuhan_tempo_shohin.konyuchubuncode, 
                            view_tuhan_tempo_shohin.itembunrui, 
                            view_tuhan_tempo_shohin.juchkbn, 
                            view_tuhan_tempo_shohin.juchkbncname, 
                            view_tuhan_tempo_shohin.henreasoncode, 
                            view_tuhan_tempo_shohin.henreasonname
                        ) 
                        UNION 
                        SELECT 
                          (
                            to_char(
                              add_months(
                                ('now' :: character varying):: timestamp without time zone, 
                                (
                                  - (13):: bigint
                                )
                              ), 
                              ('yyyymm' :: character varying):: text
                            )
                          ):: integer AS nengetu, 
                          count(
                            DISTINCT view_tuhan_tempo_shohin.kokyano
                          ) AS ninzu, 
                          '10:百貨店' AS channel, 
                          view_tuhan_tempo_shohin.konyuchubuncode, 
                          view_tuhan_tempo_shohin.itembunrui, 
                          view_tuhan_tempo_shohin.juchkbn, 
                          (
                            view_tuhan_tempo_shohin.juchkbncname
                          ):: character varying AS juchkbncname, 
                          view_tuhan_tempo_shohin.henreasoncode, 
                          (
                            view_tuhan_tempo_shohin.henreasonname
                          ):: character varying AS henreasonname 
                        FROM 
                          (
                            SELECT 
                              "substring"(
                                (
                                  (h.shukadate):: character varying
                                ):: text, 
                                0, 
                                6
                              ) AS shukadate, 
                              h.kokyano, 
                              h.daihanrobunname AS konyuchubuncode, 
                              i.itembunrui, 
                              h.juchkbn, 
                              tm67.cname AS juchkbncname, 
                              h.henreasoncode, 
                              (
                                (
                                  (h.henreasoncode):: text || (' : ' :: character varying):: text
                                ) || (h.henreasonname):: text
                              ) AS henreasonname 
                            FROM 
                              (
                                (
                                  (
                                    (
                                      saleh_itemstrategy h 
                                      JOIN salem_itemstrategy m ON (
                                        (
                                          (h.saleno):: text = (m.saleno):: text
                                        )
                                      )
                                    ) 
                                    JOIN tm14shkos "k" ON (
                                      (
                                        (m.itemcode):: text = ("k".itemcode):: text
                                      )
                                    )
                                  ) 
                                  LEFT JOIN tm39item_strategy i ON (
                                    (
                                      ("k".kosecode):: text = (i.itemcode):: text
                                    )
                                  )
                                ) 
                                LEFT JOIN tm67juch_nm tm67 ON (
                                  (
                                    (h.juchkbn):: text = (tm67.code):: text
                                  )
                                )
                              ) 
                            WHERE 
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            h.shukadate >= (
                                              (
                                                (
                                                  (
                                                    to_char(
                                                      add_months(
                                                        ('now' :: character varying):: timestamp without time zone, 
                                                        (
                                                          - (36):: bigint
                                                        )
                                                      ), 
                                                      ('yyyymm' :: character varying):: text
                                                    ) || ('01' :: character varying):: text
                                                  )
                                                ):: integer
                                              ):: numeric
                                            ):: numeric(18, 0)
                                          ) 
                                          AND (
                                            h.shukadate <= (
                                              (
                                                (
                                                  to_char(
                                                    (
                                                      last_day(
                                                        add_months(
                                                          ('now' :: character varying):: timestamp without time zone, 
                                                          (
                                                            - (1):: bigint
                                                          )
                                                        )
                                                      )
                                                    ):: timestamp without time zone, 
                                                    ('yyyymmdd' :: character varying):: text
                                                  )
                                                ):: integer
                                              ):: numeric
                                            ):: numeric(18, 0)
                                          )
                                        ) 
                                        AND (
                                          h.cancelflg = (
                                            (0):: numeric
                                          ):: numeric(18, 0)
                                        )
                                      ) 
                                      AND (
                                        (
                                          (
                                            (h.torikeikbn):: text = ('03' :: character varying):: text
                                          ) 
                                          OR (
                                            (h.torikeikbn):: text = ('04' :: character varying):: text
                                          )
                                        ) 
                                        OR (
                                          (h.torikeikbn):: text = ('05' :: character varying):: text
                                        )
                                      )
                                    ) 
                                    AND (
                                      (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                    )
                                  ) 
                                  AND (
                                    (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                  )
                                ) 
                                AND (
                                  (
                                    (
                                      (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                    ) 
                                    OR (
                                      (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                    )
                                  ) 
                                  OR (
                                    (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                  )
                                )
                              )
                          ) view_tuhan_tempo_shohin 
                        WHERE 
                          (
                            (
                              view_tuhan_tempo_shohin.shukadate >= (
                                (
                                  (
                                    to_char(
                                      add_months(
                                        ('now' :: character varying):: timestamp without time zone, 
                                        (
                                          - (24):: bigint
                                        )
                                      ), 
                                      ('yyyymm' :: character varying):: text
                                    )
                                  ):: integer
                                ):: character varying
                              ):: text
                            ) 
                            AND (
                              view_tuhan_tempo_shohin.shukadate <= (
                                (
                                  (
                                    to_char(
                                      add_months(
                                        ('now' :: character varying):: timestamp without time zone, 
                                        (
                                          - (13):: bigint
                                        )
                                      ), 
                                      ('yyyymm' :: character varying):: text
                                    )
                                  ):: integer
                                ):: character varying
                              ):: text
                            )
                          ) 
                        GROUP BY 
                          view_tuhan_tempo_shohin.konyuchubuncode, 
                          view_tuhan_tempo_shohin.itembunrui, 
                          view_tuhan_tempo_shohin.juchkbn, 
                          view_tuhan_tempo_shohin.juchkbncname, 
                          view_tuhan_tempo_shohin.henreasoncode, 
                          view_tuhan_tempo_shohin.henreasonname
                      ) 
                      UNION 
                      SELECT 
                        (
                          to_char(
                            add_months(
                              ('now' :: character varying):: timestamp without time zone, 
                              (
                                - (12):: bigint
                              )
                            ), 
                            ('yyyymm' :: character varying):: text
                          )
                        ):: integer AS nengetu, 
                        count(
                          DISTINCT view_tuhan_tempo_shohin.kokyano
                        ) AS ninzu, 
                        '10:百貨店' AS channel, 
                        view_tuhan_tempo_shohin.konyuchubuncode, 
                        view_tuhan_tempo_shohin.itembunrui, 
                        view_tuhan_tempo_shohin.juchkbn, 
                        (
                          view_tuhan_tempo_shohin.juchkbncname
                        ):: character varying AS juchkbncname, 
                        view_tuhan_tempo_shohin.henreasoncode, 
                        (
                          view_tuhan_tempo_shohin.henreasonname
                        ):: character varying AS henreasonname 
                      FROM 
                        (
                          SELECT 
                            "substring"(
                              (
                                (h.shukadate):: character varying
                              ):: text, 
                              0, 
                              6
                            ) AS shukadate, 
                            h.kokyano, 
                            h.daihanrobunname AS konyuchubuncode, 
                            i.itembunrui, 
                            h.juchkbn, 
                            tm67.cname AS juchkbncname, 
                            h.henreasoncode, 
                            (
                              (
                                (h.henreasoncode):: text || (' : ' :: character varying):: text
                              ) || (h.henreasonname):: text
                            ) AS henreasonname 
                          FROM 
                            (
                              (
                                (
                                  (
                                    saleh_itemstrategy h 
                                    JOIN salem_itemstrategy m ON (
                                      (
                                        (h.saleno):: text = (m.saleno):: text
                                      )
                                    )
                                  ) 
                                  JOIN tm14shkos "k" ON (
                                    (
                                      (m.itemcode):: text = ("k".itemcode):: text
                                    )
                                  )
                                ) 
                                LEFT JOIN tm39item_strategy i ON (
                                  (
                                    ("k".kosecode):: text = (i.itemcode):: text
                                  )
                                )
                              ) 
                              LEFT JOIN tm67juch_nm tm67 ON (
                                (
                                  (h.juchkbn):: text = (tm67.code):: text
                                )
                              )
                            ) 
                          WHERE 
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          h.shukadate >= (
                                            (
                                              (
                                                (
                                                  to_char(
                                                    add_months(
                                                      ('now' :: character varying):: timestamp without time zone, 
                                                      (
                                                        - (36):: bigint
                                                      )
                                                    ), 
                                                    ('yyyymm' :: character varying):: text
                                                  ) || ('01' :: character varying):: text
                                                )
                                              ):: integer
                                            ):: numeric
                                          ):: numeric(18, 0)
                                        ) 
                                        AND (
                                          h.shukadate <= (
                                            (
                                              (
                                                to_char(
                                                  (
                                                    last_day(
                                                      add_months(
                                                        ('now' :: character varying):: timestamp without time zone, 
                                                        (
                                                          - (1):: bigint
                                                        )
                                                      )
                                                    )
                                                  ):: timestamp without time zone, 
                                                  ('yyyymmdd' :: character varying):: text
                                                )
                                              ):: integer
                                            ):: numeric
                                          ):: numeric(18, 0)
                                        )
                                      ) 
                                      AND (
                                        h.cancelflg = (
                                          (0):: numeric
                                        ):: numeric(18, 0)
                                      )
                                    ) 
                                    AND (
                                      (
                                        (
                                          (h.torikeikbn):: text = ('03' :: character varying):: text
                                        ) 
                                        OR (
                                          (h.torikeikbn):: text = ('04' :: character varying):: text
                                        )
                                      ) 
                                      OR (
                                        (h.torikeikbn):: text = ('05' :: character varying):: text
                                      )
                                    )
                                  ) 
                                  AND (
                                    (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                  )
                                ) 
                                AND (
                                  (m.itemcode):: text <> ('9990000200' :: character varying):: text
                                )
                              ) 
                              AND (
                                (
                                  (
                                    (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                  ) 
                                  OR (
                                    (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                  )
                                ) 
                                OR (
                                  (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                                )
                              )
                            )
                        ) view_tuhan_tempo_shohin 
                      WHERE 
                        (
                          (
                            view_tuhan_tempo_shohin.shukadate >= (
                              (
                                (
                                  to_char(
                                    add_months(
                                      ('now' :: character varying):: timestamp without time zone, 
                                      (
                                        - (23):: bigint
                                      )
                                    ), 
                                    ('yyyymm' :: character varying):: text
                                  )
                                ):: integer
                              ):: character varying
                            ):: text
                          ) 
                          AND (
                            view_tuhan_tempo_shohin.shukadate <= (
                              (
                                (
                                  to_char(
                                    add_months(
                                      ('now' :: character varying):: timestamp without time zone, 
                                      (
                                        - (12):: bigint
                                      )
                                    ), 
                                    ('yyyymm' :: character varying):: text
                                  )
                                ):: integer
                              ):: character varying
                            ):: text
                          )
                        ) 
                      GROUP BY 
                        view_tuhan_tempo_shohin.konyuchubuncode, 
                        view_tuhan_tempo_shohin.itembunrui, 
                        view_tuhan_tempo_shohin.juchkbn, 
                        view_tuhan_tempo_shohin.juchkbncname, 
                        view_tuhan_tempo_shohin.henreasoncode, 
                        view_tuhan_tempo_shohin.henreasonname
                    ) 
                    UNION 
                    SELECT 
                      (
                        to_char(
                          add_months(
                            ('now' :: character varying):: timestamp without time zone, 
                            (
                              - (11):: bigint
                            )
                          ), 
                          ('yyyymm' :: character varying):: text
                        )
                      ):: integer AS nengetu, 
                      count(
                        DISTINCT view_tuhan_tempo_shohin.kokyano
                      ) AS ninzu, 
                      '10:百貨店' AS channel, 
                      view_tuhan_tempo_shohin.konyuchubuncode, 
                      view_tuhan_tempo_shohin.itembunrui, 
                      view_tuhan_tempo_shohin.juchkbn, 
                      (
                        view_tuhan_tempo_shohin.juchkbncname
                      ):: character varying AS juchkbncname, 
                      view_tuhan_tempo_shohin.henreasoncode, 
                      (
                        view_tuhan_tempo_shohin.henreasonname
                      ):: character varying AS henreasonname 
                    FROM 
                      (
                        SELECT 
                          "substring"(
                            (
                              (h.shukadate):: character varying
                            ):: text, 
                            0, 
                            6
                          ) AS shukadate, 
                          h.kokyano, 
                          h.daihanrobunname AS konyuchubuncode, 
                          i.itembunrui, 
                          h.juchkbn, 
                          tm67.cname AS juchkbncname, 
                          h.henreasoncode, 
                          (
                            (
                              (h.henreasoncode):: text || (' : ' :: character varying):: text
                            ) || (h.henreasonname):: text
                          ) AS henreasonname 
                        FROM 
                          (
                            (
                              (
                                (
                                  saleh_itemstrategy h 
                                  JOIN salem_itemstrategy m ON (
                                    (
                                      (h.saleno):: text = (m.saleno):: text
                                    )
                                  )
                                ) 
                                JOIN tm14shkos "k" ON (
                                  (
                                    (m.itemcode):: text = ("k".itemcode):: text
                                  )
                                )
                              ) 
                              LEFT JOIN tm39item_strategy i ON (
                                (
                                  ("k".kosecode):: text = (i.itemcode):: text
                                )
                              )
                            ) 
                            LEFT JOIN tm67juch_nm tm67 ON (
                              (
                                (h.juchkbn):: text = (tm67.code):: text
                              )
                            )
                          ) 
                        WHERE 
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        h.shukadate >= (
                                          (
                                            (
                                              (
                                                to_char(
                                                  add_months(
                                                    ('now' :: character varying):: timestamp without time zone, 
                                                    (
                                                      - (36):: bigint
                                                    )
                                                  ), 
                                                  ('yyyymm' :: character varying):: text
                                                ) || ('01' :: character varying):: text
                                              )
                                            ):: integer
                                          ):: numeric
                                        ):: numeric(18, 0)
                                      ) 
                                      AND (
                                        h.shukadate <= (
                                          (
                                            (
                                              to_char(
                                                (
                                                  last_day(
                                                    add_months(
                                                      ('now' :: character varying):: timestamp without time zone, 
                                                      (
                                                        - (1):: bigint
                                                      )
                                                    )
                                                  )
                                                ):: timestamp without time zone, 
                                                ('yyyymmdd' :: character varying):: text
                                              )
                                            ):: integer
                                          ):: numeric
                                        ):: numeric(18, 0)
                                      )
                                    ) 
                                    AND (
                                      h.cancelflg = (
                                        (0):: numeric
                                      ):: numeric(18, 0)
                                    )
                                  ) 
                                  AND (
                                    (
                                      (
                                        (h.torikeikbn):: text = ('03' :: character varying):: text
                                      ) 
                                      OR (
                                        (h.torikeikbn):: text = ('04' :: character varying):: text
                                      )
                                    ) 
                                    OR (
                                      (h.torikeikbn):: text = ('05' :: character varying):: text
                                    )
                                  )
                                ) 
                                AND (
                                  (m.itemcode):: text <> ('9990000100' :: character varying):: text
                                )
                              ) 
                              AND (
                                (m.itemcode):: text <> ('9990000200' :: character varying):: text
                              )
                            ) 
                            AND (
                              (
                                (
                                  (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                                ) 
                                OR (
                                  (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                                )
                              ) 
                              OR (
                                (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                              )
                            )
                          )
                      ) view_tuhan_tempo_shohin 
                    WHERE 
                      (
                        (
                          view_tuhan_tempo_shohin.shukadate >= (
                            (
                              (
                                to_char(
                                  add_months(
                                    ('now' :: character varying):: timestamp without time zone, 
                                    (
                                      - (22):: bigint
                                    )
                                  ), 
                                  ('yyyymm' :: character varying):: text
                                )
                              ):: integer
                            ):: character varying
                          ):: text
                        ) 
                        AND (
                          view_tuhan_tempo_shohin.shukadate <= (
                            (
                              (
                                to_char(
                                  add_months(
                                    ('now' :: character varying):: timestamp without time zone, 
                                    (
                                      - (11):: bigint
                                    )
                                  ), 
                                  ('yyyymm' :: character varying):: text
                                )
                              ):: integer
                            ):: character varying
                          ):: text
                        )
                      ) 
                    GROUP BY 
                      view_tuhan_tempo_shohin.konyuchubuncode, 
                      view_tuhan_tempo_shohin.itembunrui, 
                      view_tuhan_tempo_shohin.juchkbn, 
                      view_tuhan_tempo_shohin.juchkbncname, 
                      view_tuhan_tempo_shohin.henreasoncode, 
                      view_tuhan_tempo_shohin.henreasonname
                  ) 
                  UNION 
                  SELECT 
                    (
                      to_char(
                        add_months(
                          ('now' :: character varying):: timestamp without time zone, 
                          (
                            - (10):: bigint
                          )
                        ), 
                        ('yyyymm' :: character varying):: text
                      )
                    ):: integer AS nengetu, 
                    count(
                      DISTINCT view_tuhan_tempo_shohin.kokyano
                    ) AS ninzu, 
                    '10:百貨店' AS channel, 
                    view_tuhan_tempo_shohin.konyuchubuncode, 
                    view_tuhan_tempo_shohin.itembunrui, 
                    view_tuhan_tempo_shohin.juchkbn, 
                    (
                      view_tuhan_tempo_shohin.juchkbncname
                    ):: character varying AS juchkbncname, 
                    view_tuhan_tempo_shohin.henreasoncode, 
                    (
                      view_tuhan_tempo_shohin.henreasonname
                    ):: character varying AS henreasonname 
                  FROM 
                    (
                      SELECT 
                        "substring"(
                          (
                            (h.shukadate):: character varying
                          ):: text, 
                          0, 
                          6
                        ) AS shukadate, 
                        h.kokyano, 
                        h.daihanrobunname AS konyuchubuncode, 
                        i.itembunrui, 
                        h.juchkbn, 
                        tm67.cname AS juchkbncname, 
                        h.henreasoncode, 
                        (
                          (
                            (h.henreasoncode):: text || (' : ' :: character varying):: text
                          ) || (h.henreasonname):: text
                        ) AS henreasonname 
                      FROM 
                        (
                          (
                            (
                              (
                                saleh_itemstrategy h 
                                JOIN salem_itemstrategy m ON (
                                  (
                                    (h.saleno):: text = (m.saleno):: text
                                  )
                                )
                              ) 
                              JOIN tm14shkos "k" ON (
                                (
                                  (m.itemcode):: text = ("k".itemcode):: text
                                )
                              )
                            ) 
                            LEFT JOIN tm39item_strategy i ON (
                              (
                                ("k".kosecode):: text = (i.itemcode):: text
                              )
                            )
                          ) 
                          LEFT JOIN tm67juch_nm tm67 ON (
                            (
                              (h.juchkbn):: text = (tm67.code):: text
                            )
                          )
                        ) 
                      WHERE 
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      h.shukadate >= (
                                        (
                                          (
                                            (
                                              to_char(
                                                add_months(
                                                  ('now' :: character varying):: timestamp without time zone, 
                                                  (
                                                    - (36):: bigint
                                                  )
                                                ), 
                                                ('yyyymm' :: character varying):: text
                                              ) || ('01' :: character varying):: text
                                            )
                                          ):: integer
                                        ):: numeric
                                      ):: numeric(18, 0)
                                    ) 
                                    AND (
                                      h.shukadate <= (
                                        (
                                          (
                                            to_char(
                                              (
                                                last_day(
                                                  add_months(
                                                    ('now' :: character varying):: timestamp without time zone, 
                                                    (
                                                      - (1):: bigint
                                                    )
                                                  )
                                                )
                                              ):: timestamp without time zone, 
                                              ('yyyymmdd' :: character varying):: text
                                            )
                                          ):: integer
                                        ):: numeric
                                      ):: numeric(18, 0)
                                    )
                                  ) 
                                  AND (
                                    h.cancelflg = (
                                      (0):: numeric
                                    ):: numeric(18, 0)
                                  )
                                ) 
                                AND (
                                  (
                                    (
                                      (h.torikeikbn):: text = ('03' :: character varying):: text
                                    ) 
                                    OR (
                                      (h.torikeikbn):: text = ('04' :: character varying):: text
                                    )
                                  ) 
                                  OR (
                                    (h.torikeikbn):: text = ('05' :: character varying):: text
                                  )
                                )
                              ) 
                              AND (
                                (m.itemcode):: text <> ('9990000100' :: character varying):: text
                              )
                            ) 
                            AND (
                              (m.itemcode):: text <> ('9990000200' :: character varying):: text
                            )
                          ) 
                          AND (
                            (
                              (
                                (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                              ) 
                              OR (
                                (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                              )
                            ) 
                            OR (
                              (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                            )
                          )
                        )
                    ) view_tuhan_tempo_shohin 
                  WHERE 
                    (
                      (
                        view_tuhan_tempo_shohin.shukadate >= (
                          (
                            (
                              to_char(
                                add_months(
                                  ('now' :: character varying):: timestamp without time zone, 
                                  (
                                    - (21):: bigint
                                  )
                                ), 
                                ('yyyymm' :: character varying):: text
                              )
                            ):: integer
                          ):: character varying
                        ):: text
                      ) 
                      AND (
                        view_tuhan_tempo_shohin.shukadate <= (
                          (
                            (
                              to_char(
                                add_months(
                                  ('now' :: character varying):: timestamp without time zone, 
                                  (
                                    - (10):: bigint
                                  )
                                ), 
                                ('yyyymm' :: character varying):: text
                              )
                            ):: integer
                          ):: character varying
                        ):: text
                      )
                    ) 
                  GROUP BY 
                    view_tuhan_tempo_shohin.konyuchubuncode, 
                    view_tuhan_tempo_shohin.itembunrui, 
                    view_tuhan_tempo_shohin.juchkbn, 
                    view_tuhan_tempo_shohin.juchkbncname, 
                    view_tuhan_tempo_shohin.henreasoncode, 
                    view_tuhan_tempo_shohin.henreasonname
                ) 
                UNION 
                SELECT 
                  (
                    to_char(
                      add_months(
                        ('now' :: character varying):: timestamp without time zone, 
                        (
                          - (9):: bigint
                        )
                      ), 
                      ('yyyymm' :: character varying):: text
                    )
                  ):: integer AS nengetu, 
                  count(
                    DISTINCT view_tuhan_tempo_shohin.kokyano
                  ) AS ninzu, 
                  '10:百貨店' AS channel, 
                  view_tuhan_tempo_shohin.konyuchubuncode, 
                  view_tuhan_tempo_shohin.itembunrui, 
                  view_tuhan_tempo_shohin.juchkbn, 
                  (
                    view_tuhan_tempo_shohin.juchkbncname
                  ):: character varying AS juchkbncname, 
                  view_tuhan_tempo_shohin.henreasoncode, 
                  (
                    view_tuhan_tempo_shohin.henreasonname
                  ):: character varying AS henreasonname 
                FROM 
                  (
                    SELECT 
                      "substring"(
                        (
                          (h.shukadate):: character varying
                        ):: text, 
                        0, 
                        6
                      ) AS shukadate, 
                      h.kokyano, 
                      h.daihanrobunname AS konyuchubuncode, 
                      i.itembunrui, 
                      h.juchkbn, 
                      tm67.cname AS juchkbncname, 
                      h.henreasoncode, 
                      (
                        (
                          (h.henreasoncode):: text || (' : ' :: character varying):: text
                        ) || (h.henreasonname):: text
                      ) AS henreasonname 
                    FROM 
                      (
                        (
                          (
                            (
                              saleh_itemstrategy h 
                              JOIN salem_itemstrategy m ON (
                                (
                                  (h.saleno):: text = (m.saleno):: text
                                )
                              )
                            ) 
                            JOIN tm14shkos "k" ON (
                              (
                                (m.itemcode):: text = ("k".itemcode):: text
                              )
                            )
                          ) 
                          LEFT JOIN tm39item_strategy i ON (
                            (
                              ("k".kosecode):: text = (i.itemcode):: text
                            )
                          )
                        ) 
                        LEFT JOIN tm67juch_nm tm67 ON (
                          (
                            (h.juchkbn):: text = (tm67.code):: text
                          )
                        )
                      ) 
                    WHERE 
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    h.shukadate >= (
                                      (
                                        (
                                          (
                                            to_char(
                                              add_months(
                                                ('now' :: character varying):: timestamp without time zone, 
                                                (
                                                  - (36):: bigint
                                                )
                                              ), 
                                              ('yyyymm' :: character varying):: text
                                            ) || ('01' :: character varying):: text
                                          )
                                        ):: integer
                                      ):: numeric
                                    ):: numeric(18, 0)
                                  ) 
                                  AND (
                                    h.shukadate <= (
                                      (
                                        (
                                          to_char(
                                            (
                                              last_day(
                                                add_months(
                                                  ('now' :: character varying):: timestamp without time zone, 
                                                  (
                                                    - (1):: bigint
                                                  )
                                                )
                                              )
                                            ):: timestamp without time zone, 
                                            ('yyyymmdd' :: character varying):: text
                                          )
                                        ):: integer
                                      ):: numeric
                                    ):: numeric(18, 0)
                                  )
                                ) 
                                AND (
                                  h.cancelflg = (
                                    (0):: numeric
                                  ):: numeric(18, 0)
                                )
                              ) 
                              AND (
                                (
                                  (
                                    (h.torikeikbn):: text = ('03' :: character varying):: text
                                  ) 
                                  OR (
                                    (h.torikeikbn):: text = ('04' :: character varying):: text
                                  )
                                ) 
                                OR (
                                  (h.torikeikbn):: text = ('05' :: character varying):: text
                                )
                              )
                            ) 
                            AND (
                              (m.itemcode):: text <> ('9990000100' :: character varying):: text
                            )
                          ) 
                          AND (
                            (m.itemcode):: text <> ('9990000200' :: character varying):: text
                          )
                        ) 
                        AND (
                          (
                            (
                              (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                            ) 
                            OR (
                              (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                            )
                          ) 
                          OR (
                            (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                          )
                        )
                      )
                  ) view_tuhan_tempo_shohin 
                WHERE 
                  (
                    (
                      view_tuhan_tempo_shohin.shukadate >= (
                        (
                          (
                            to_char(
                              add_months(
                                ('now' :: character varying):: timestamp without time zone, 
                                (
                                  - (20):: bigint
                                )
                              ), 
                              ('yyyymm' :: character varying):: text
                            )
                          ):: integer
                        ):: character varying
                      ):: text
                    ) 
                    AND (
                      view_tuhan_tempo_shohin.shukadate <= (
                        (
                          (
                            to_char(
                              add_months(
                                ('now' :: character varying):: timestamp without time zone, 
                                (
                                  - (9):: bigint
                                )
                              ), 
                              ('yyyymm' :: character varying):: text
                            )
                          ):: integer
                        ):: character varying
                      ):: text
                    )
                  ) 
                GROUP BY 
                  view_tuhan_tempo_shohin.konyuchubuncode, 
                  view_tuhan_tempo_shohin.itembunrui, 
                  view_tuhan_tempo_shohin.juchkbn, 
                  view_tuhan_tempo_shohin.juchkbncname, 
                  view_tuhan_tempo_shohin.henreasoncode, 
                  view_tuhan_tempo_shohin.henreasonname
              ) 
              UNION 
              SELECT 
                (
                  to_char(
                    add_months(
                      ('now' :: character varying):: timestamp without time zone, 
                      (
                        - (8):: bigint
                      )
                    ), 
                    ('yyyymm' :: character varying):: text
                  )
                ):: integer AS nengetu, 
                count(
                  DISTINCT view_tuhan_tempo_shohin.kokyano
                ) AS ninzu, 
                '10:百貨店' AS channel, 
                view_tuhan_tempo_shohin.konyuchubuncode, 
                view_tuhan_tempo_shohin.itembunrui, 
                view_tuhan_tempo_shohin.juchkbn, 
                (
                  view_tuhan_tempo_shohin.juchkbncname
                ):: character varying AS juchkbncname, 
                view_tuhan_tempo_shohin.henreasoncode, 
                (
                  view_tuhan_tempo_shohin.henreasonname
                ):: character varying AS henreasonname 
              FROM 
                (
                  SELECT 
                    "substring"(
                      (
                        (h.shukadate):: character varying
                      ):: text, 
                      0, 
                      6
                    ) AS shukadate, 
                    h.kokyano, 
                    h.daihanrobunname AS konyuchubuncode, 
                    i.itembunrui, 
                    h.juchkbn, 
                    tm67.cname AS juchkbncname, 
                    h.henreasoncode, 
                    (
                      (
                        (h.henreasoncode):: text || (' : ' :: character varying):: text
                      ) || (h.henreasonname):: text
                    ) AS henreasonname 
                  FROM 
                    (
                      (
                        (
                          (
                            saleh_itemstrategy h 
                            JOIN salem_itemstrategy m ON (
                              (
                                (h.saleno):: text = (m.saleno):: text
                              )
                            )
                          ) 
                          JOIN tm14shkos "k" ON (
                            (
                              (m.itemcode):: text = ("k".itemcode):: text
                            )
                          )
                        ) 
                        LEFT JOIN tm39item_strategy i ON (
                          (
                            ("k".kosecode):: text = (i.itemcode):: text
                          )
                        )
                      ) 
                      LEFT JOIN tm67juch_nm tm67 ON (
                        (
                          (h.juchkbn):: text = (tm67.code):: text
                        )
                      )
                    ) 
                  WHERE 
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  h.shukadate >= (
                                    (
                                      (
                                        (
                                          to_char(
                                            add_months(
                                              ('now' :: character varying):: timestamp without time zone, 
                                              (
                                                - (36):: bigint
                                              )
                                            ), 
                                            ('yyyymm' :: character varying):: text
                                          ) || ('01' :: character varying):: text
                                        )
                                      ):: integer
                                    ):: numeric
                                  ):: numeric(18, 0)
                                ) 
                                AND (
                                  h.shukadate <= (
                                    (
                                      (
                                        to_char(
                                          (
                                            last_day(
                                              add_months(
                                                ('now' :: character varying):: timestamp without time zone, 
                                                (
                                                  - (1):: bigint
                                                )
                                              )
                                            )
                                          ):: timestamp without time zone, 
                                          ('yyyymmdd' :: character varying):: text
                                        )
                                      ):: integer
                                    ):: numeric
                                  ):: numeric(18, 0)
                                )
                              ) 
                              AND (
                                h.cancelflg = (
                                  (0):: numeric
                                ):: numeric(18, 0)
                              )
                            ) 
                            AND (
                              (
                                (
                                  (h.torikeikbn):: text = ('03' :: character varying):: text
                                ) 
                                OR (
                                  (h.torikeikbn):: text = ('04' :: character varying):: text
                                )
                              ) 
                              OR (
                                (h.torikeikbn):: text = ('05' :: character varying):: text
                              )
                            )
                          ) 
                          AND (
                            (m.itemcode):: text <> ('9990000100' :: character varying):: text
                          )
                        ) 
                        AND (
                          (m.itemcode):: text <> ('9990000200' :: character varying):: text
                        )
                      ) 
                      AND (
                        (
                          (
                            (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                          ) 
                          OR (
                            (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                          )
                        ) 
                        OR (
                          (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                        )
                      )
                    )
                ) view_tuhan_tempo_shohin 
              WHERE 
                (
                  (
                    view_tuhan_tempo_shohin.shukadate >= (
                      (
                        (
                          to_char(
                            add_months(
                              ('now' :: character varying):: timestamp without time zone, 
                              (
                                - (19):: bigint
                              )
                            ), 
                            ('yyyymm' :: character varying):: text
                          )
                        ):: integer
                      ):: character varying
                    ):: text
                  ) 
                  AND (
                    view_tuhan_tempo_shohin.shukadate <= (
                      (
                        (
                          to_char(
                            add_months(
                              ('now' :: character varying):: timestamp without time zone, 
                              (
                                - (8):: bigint
                              )
                            ), 
                            ('yyyymm' :: character varying):: text
                          )
                        ):: integer
                      ):: character varying
                    ):: text
                  )
                ) 
              GROUP BY 
                view_tuhan_tempo_shohin.konyuchubuncode, 
                view_tuhan_tempo_shohin.itembunrui, 
                view_tuhan_tempo_shohin.juchkbn, 
                view_tuhan_tempo_shohin.juchkbncname, 
                view_tuhan_tempo_shohin.henreasoncode, 
                view_tuhan_tempo_shohin.henreasonname
            ) 
            UNION 
            SELECT 
              (
                to_char(
                  add_months(
                    ('now' :: character varying):: timestamp without time zone, 
                    (
                      - (7):: bigint
                    )
                  ), 
                  ('yyyymm' :: character varying):: text
                )
              ):: integer AS nengetu, 
              count(
                DISTINCT view_tuhan_tempo_shohin.kokyano
              ) AS ninzu, 
              '10:百貨店' AS channel, 
              view_tuhan_tempo_shohin.konyuchubuncode, 
              view_tuhan_tempo_shohin.itembunrui, 
              view_tuhan_tempo_shohin.juchkbn, 
              (
                view_tuhan_tempo_shohin.juchkbncname
              ):: character varying AS juchkbncname, 
              view_tuhan_tempo_shohin.henreasoncode, 
              (
                view_tuhan_tempo_shohin.henreasonname
              ):: character varying AS henreasonname 
            FROM 
              (
                SELECT 
                  "substring"(
                    (
                      (h.shukadate):: character varying
                    ):: text, 
                    0, 
                    6
                  ) AS shukadate, 
                  h.kokyano, 
                  h.daihanrobunname AS konyuchubuncode, 
                  i.itembunrui, 
                  h.juchkbn, 
                  tm67.cname AS juchkbncname, 
                  h.henreasoncode, 
                  (
                    (
                      (h.henreasoncode):: text || (' : ' :: character varying):: text
                    ) || (h.henreasonname):: text
                  ) AS henreasonname 
                FROM 
                  (
                    (
                      (
                        (
                          saleh_itemstrategy h 
                          JOIN salem_itemstrategy m ON (
                            (
                              (h.saleno):: text = (m.saleno):: text
                            )
                          )
                        ) 
                        JOIN tm14shkos "k" ON (
                          (
                            (m.itemcode):: text = ("k".itemcode):: text
                          )
                        )
                      ) 
                      LEFT JOIN tm39item_strategy i ON (
                        (
                          ("k".kosecode):: text = (i.itemcode):: text
                        )
                      )
                    ) 
                    LEFT JOIN tm67juch_nm tm67 ON (
                      (
                        (h.juchkbn):: text = (tm67.code):: text
                      )
                    )
                  ) 
                WHERE 
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                h.shukadate >= (
                                  (
                                    (
                                      (
                                        to_char(
                                          add_months(
                                            ('now' :: character varying):: timestamp without time zone, 
                                            (
                                              - (36):: bigint
                                            )
                                          ), 
                                          ('yyyymm' :: character varying):: text
                                        ) || ('01' :: character varying):: text
                                      )
                                    ):: integer
                                  ):: numeric
                                ):: numeric(18, 0)
                              ) 
                              AND (
                                h.shukadate <= (
                                  (
                                    (
                                      to_char(
                                        (
                                          last_day(
                                            add_months(
                                              ('now' :: character varying):: timestamp without time zone, 
                                              (
                                                - (1):: bigint
                                              )
                                            )
                                          )
                                        ):: timestamp without time zone, 
                                        ('yyyymmdd' :: character varying):: text
                                      )
                                    ):: integer
                                  ):: numeric
                                ):: numeric(18, 0)
                              )
                            ) 
                            AND (
                              h.cancelflg = (
                                (0):: numeric
                              ):: numeric(18, 0)
                            )
                          ) 
                          AND (
                            (
                              (
                                (h.torikeikbn):: text = ('03' :: character varying):: text
                              ) 
                              OR (
                                (h.torikeikbn):: text = ('04' :: character varying):: text
                              )
                            ) 
                            OR (
                              (h.torikeikbn):: text = ('05' :: character varying):: text
                            )
                          )
                        ) 
                        AND (
                          (m.itemcode):: text <> ('9990000100' :: character varying):: text
                        )
                      ) 
                      AND (
                        (m.itemcode):: text <> ('9990000200' :: character varying):: text
                      )
                    ) 
                    AND (
                      (
                        (
                          (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                        ) 
                        OR (
                          (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                        )
                      ) 
                      OR (
                        (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                      )
                    )
                  )
              ) view_tuhan_tempo_shohin 
            WHERE 
              (
                (
                  view_tuhan_tempo_shohin.shukadate >= (
                    (
                      (
                        to_char(
                          add_months(
                            ('now' :: character varying):: timestamp without time zone, 
                            (
                              - (18):: bigint
                            )
                          ), 
                          ('yyyymm' :: character varying):: text
                        )
                      ):: integer
                    ):: character varying
                  ):: text
                ) 
                AND (
                  view_tuhan_tempo_shohin.shukadate <= (
                    (
                      (
                        to_char(
                          add_months(
                            ('now' :: character varying):: timestamp without time zone, 
                            (
                              - (7):: bigint
                            )
                          ), 
                          ('yyyymm' :: character varying):: text
                        )
                      ):: integer
                    ):: character varying
                  ):: text
                )
              ) 
            GROUP BY 
              view_tuhan_tempo_shohin.konyuchubuncode, 
              view_tuhan_tempo_shohin.itembunrui, 
              view_tuhan_tempo_shohin.juchkbn, 
              view_tuhan_tempo_shohin.juchkbncname, 
              view_tuhan_tempo_shohin.henreasoncode, 
              view_tuhan_tempo_shohin.henreasonname
          ) 
          UNION 
          SELECT 
            (
              to_char(
                add_months(
                  ('now' :: character varying):: timestamp without time zone, 
                  (
                    - (6):: bigint
                  )
                ), 
                ('yyyymm' :: character varying):: text
              )
            ):: integer AS nengetu, 
            count(
              DISTINCT view_tuhan_tempo_shohin.kokyano
            ) AS ninzu, 
            '10:百貨店' AS channel, 
            view_tuhan_tempo_shohin.konyuchubuncode, 
            view_tuhan_tempo_shohin.itembunrui, 
            view_tuhan_tempo_shohin.juchkbn, 
            (
              view_tuhan_tempo_shohin.juchkbncname
            ):: character varying AS juchkbncname, 
            view_tuhan_tempo_shohin.henreasoncode, 
            (
              view_tuhan_tempo_shohin.henreasonname
            ):: character varying AS henreasonname 
          FROM 
            (
              SELECT 
                "substring"(
                  (
                    (h.shukadate):: character varying
                  ):: text, 
                  0, 
                  6
                ) AS shukadate, 
                h.kokyano, 
                h.daihanrobunname AS konyuchubuncode, 
                i.itembunrui, 
                h.juchkbn, 
                tm67.cname AS juchkbncname, 
                h.henreasoncode, 
                (
                  (
                    (h.henreasoncode):: text || (' : ' :: character varying):: text
                  ) || (h.henreasonname):: text
                ) AS henreasonname 
              FROM 
                (
                  (
                    (
                      (
                        saleh_itemstrategy h 
                        JOIN salem_itemstrategy m ON (
                          (
                            (h.saleno):: text = (m.saleno):: text
                          )
                        )
                      ) 
                      JOIN tm14shkos "k" ON (
                        (
                          (m.itemcode):: text = ("k".itemcode):: text
                        )
                      )
                    ) 
                    LEFT JOIN tm39item_strategy i ON (
                      (
                        ("k".kosecode):: text = (i.itemcode):: text
                      )
                    )
                  ) 
                  LEFT JOIN tm67juch_nm tm67 ON (
                    (
                      (h.juchkbn):: text = (tm67.code):: text
                    )
                  )
                ) 
              WHERE 
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              h.shukadate >= (
                                (
                                  (
                                    (
                                      to_char(
                                        add_months(
                                          ('now' :: character varying):: timestamp without time zone, 
                                          (
                                            - (36):: bigint
                                          )
                                        ), 
                                        ('yyyymm' :: character varying):: text
                                      ) || ('01' :: character varying):: text
                                    )
                                  ):: integer
                                ):: numeric
                              ):: numeric(18, 0)
                            ) 
                            AND (
                              h.shukadate <= (
                                (
                                  (
                                    to_char(
                                      (
                                        last_day(
                                          add_months(
                                            ('now' :: character varying):: timestamp without time zone, 
                                            (
                                              - (1):: bigint
                                            )
                                          )
                                        )
                                      ):: timestamp without time zone, 
                                      ('yyyymmdd' :: character varying):: text
                                    )
                                  ):: integer
                                ):: numeric
                              ):: numeric(18, 0)
                            )
                          ) 
                          AND (
                            h.cancelflg = (
                              (0):: numeric
                            ):: numeric(18, 0)
                          )
                        ) 
                        AND (
                          (
                            (
                              (h.torikeikbn):: text = ('03' :: character varying):: text
                            ) 
                            OR (
                              (h.torikeikbn):: text = ('04' :: character varying):: text
                            )
                          ) 
                          OR (
                            (h.torikeikbn):: text = ('05' :: character varying):: text
                          )
                        )
                      ) 
                      AND (
                        (m.itemcode):: text <> ('9990000100' :: character varying):: text
                      )
                    ) 
                    AND (
                      (m.itemcode):: text <> ('9990000200' :: character varying):: text
                    )
                  ) 
                  AND (
                    (
                      (
                        (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                      ) 
                      OR (
                        (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                      )
                    ) 
                    OR (
                      (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                    )
                  )
                )
            ) view_tuhan_tempo_shohin 
          WHERE 
            (
              (
                view_tuhan_tempo_shohin.shukadate >= (
                  (
                    (
                      to_char(
                        add_months(
                          ('now' :: character varying):: timestamp without time zone, 
                          (
                            - (17):: bigint
                          )
                        ), 
                        ('yyyymm' :: character varying):: text
                      )
                    ):: integer
                  ):: character varying
                ):: text
              ) 
              AND (
                view_tuhan_tempo_shohin.shukadate <= (
                  (
                    (
                      to_char(
                        add_months(
                          ('now' :: character varying):: timestamp without time zone, 
                          (
                            - (6):: bigint
                          )
                        ), 
                        ('yyyymm' :: character varying):: text
                      )
                    ):: integer
                  ):: character varying
                ):: text
              )
            ) 
          GROUP BY 
            view_tuhan_tempo_shohin.konyuchubuncode, 
            view_tuhan_tempo_shohin.itembunrui, 
            view_tuhan_tempo_shohin.juchkbn, 
            view_tuhan_tempo_shohin.juchkbncname, 
            view_tuhan_tempo_shohin.henreasoncode, 
            view_tuhan_tempo_shohin.henreasonname
        ) 
        UNION 
        SELECT 
          (
            to_char(
              add_months(
                ('now' :: character varying):: timestamp without time zone, 
                (
                  - (5):: bigint
                )
              ), 
              ('yyyymm' :: character varying):: text
            )
          ):: integer AS nengetu, 
          count(
            DISTINCT view_tuhan_tempo_shohin.kokyano
          ) AS ninzu, 
          '10:百貨店' AS channel, 
          view_tuhan_tempo_shohin.konyuchubuncode, 
          view_tuhan_tempo_shohin.itembunrui, 
          view_tuhan_tempo_shohin.juchkbn, 
          (
            view_tuhan_tempo_shohin.juchkbncname
          ):: character varying AS juchkbncname, 
          view_tuhan_tempo_shohin.henreasoncode, 
          (
            view_tuhan_tempo_shohin.henreasonname
          ):: character varying AS henreasonname 
        FROM 
          (
            SELECT 
              "substring"(
                (
                  (h.shukadate):: character varying
                ):: text, 
                0, 
                6
              ) AS shukadate, 
              h.kokyano, 
              h.daihanrobunname AS konyuchubuncode, 
              i.itembunrui, 
              h.juchkbn, 
              tm67.cname AS juchkbncname, 
              h.henreasoncode, 
              (
                (
                  (h.henreasoncode):: text || (' : ' :: character varying):: text
                ) || (h.henreasonname):: text
              ) AS henreasonname 
            FROM 
              (
                (
                  (
                    (
                      saleh_itemstrategy h 
                      JOIN salem_itemstrategy m ON (
                        (
                          (h.saleno):: text = (m.saleno):: text
                        )
                      )
                    ) 
                    JOIN tm14shkos "k" ON (
                      (
                        (m.itemcode):: text = ("k".itemcode):: text
                      )
                    )
                  ) 
                  LEFT JOIN tm39item_strategy i ON (
                    (
                      ("k".kosecode):: text = (i.itemcode):: text
                    )
                  )
                ) 
                LEFT JOIN tm67juch_nm tm67 ON (
                  (
                    (h.juchkbn):: text = (tm67.code):: text
                  )
                )
              ) 
            WHERE 
              (
                (
                  (
                    (
                      (
                        (
                          (
                            h.shukadate >= (
                              (
                                (
                                  (
                                    to_char(
                                      add_months(
                                        ('now' :: character varying):: timestamp without time zone, 
                                        (
                                          - (36):: bigint
                                        )
                                      ), 
                                      ('yyyymm' :: character varying):: text
                                    ) || ('01' :: character varying):: text
                                  )
                                ):: integer
                              ):: numeric
                            ):: numeric(18, 0)
                          ) 
                          AND (
                            h.shukadate <= (
                              (
                                (
                                  to_char(
                                    (
                                      last_day(
                                        add_months(
                                          ('now' :: character varying):: timestamp without time zone, 
                                          (
                                            - (1):: bigint
                                          )
                                        )
                                      )
                                    ):: timestamp without time zone, 
                                    ('yyyymmdd' :: character varying):: text
                                  )
                                ):: integer
                              ):: numeric
                            ):: numeric(18, 0)
                          )
                        ) 
                        AND (
                          h.cancelflg = (
                            (0):: numeric
                          ):: numeric(18, 0)
                        )
                      ) 
                      AND (
                        (
                          (
                            (h.torikeikbn):: text = ('03' :: character varying):: text
                          ) 
                          OR (
                            (h.torikeikbn):: text = ('04' :: character varying):: text
                          )
                        ) 
                        OR (
                          (h.torikeikbn):: text = ('05' :: character varying):: text
                        )
                      )
                    ) 
                    AND (
                      (m.itemcode):: text <> ('9990000100' :: character varying):: text
                    )
                  ) 
                  AND (
                    (m.itemcode):: text <> ('9990000200' :: character varying):: text
                  )
                ) 
                AND (
                  (
                    (
                      (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                    ) 
                    OR (
                      (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                    )
                  ) 
                  OR (
                    (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                  )
                )
              )
          ) view_tuhan_tempo_shohin 
        WHERE 
          (
            (
              view_tuhan_tempo_shohin.shukadate >= (
                (
                  (
                    to_char(
                      add_months(
                        ('now' :: character varying):: timestamp without time zone, 
                        (
                          - (16):: bigint
                        )
                      ), 
                      ('yyyymm' :: character varying):: text
                    )
                  ):: integer
                ):: character varying
              ):: text
            ) 
            AND (
              view_tuhan_tempo_shohin.shukadate <= (
                (
                  (
                    to_char(
                      add_months(
                        ('now' :: character varying):: timestamp without time zone, 
                        (
                          - (5):: bigint
                        )
                      ), 
                      ('yyyymm' :: character varying):: text
                    )
                  ):: integer
                ):: character varying
              ):: text
            )
          ) 
        GROUP BY 
          view_tuhan_tempo_shohin.konyuchubuncode, 
          view_tuhan_tempo_shohin.itembunrui, 
          view_tuhan_tempo_shohin.juchkbn, 
          view_tuhan_tempo_shohin.juchkbncname, 
          view_tuhan_tempo_shohin.henreasoncode, 
          view_tuhan_tempo_shohin.henreasonname
      ) 
      UNION 
      SELECT 
        (
          to_char(
            add_months(
              ('now' :: character varying):: timestamp without time zone, 
              (
                - (4):: bigint
              )
            ), 
            ('yyyymm' :: character varying):: text
          )
        ):: integer AS nengetu, 
        count(
          DISTINCT view_tuhan_tempo_shohin.kokyano
        ) AS ninzu, 
        '10:百貨店' AS channel, 
        view_tuhan_tempo_shohin.konyuchubuncode, 
        view_tuhan_tempo_shohin.itembunrui, 
        view_tuhan_tempo_shohin.juchkbn, 
        (
          view_tuhan_tempo_shohin.juchkbncname
        ):: character varying AS juchkbncname, 
        view_tuhan_tempo_shohin.henreasoncode, 
        (
          view_tuhan_tempo_shohin.henreasonname
        ):: character varying AS henreasonname 
      FROM 
        (
          SELECT 
            "substring"(
              (
                (h.shukadate):: character varying
              ):: text, 
              0, 
              6
            ) AS shukadate, 
            h.kokyano, 
            h.daihanrobunname AS konyuchubuncode, 
            i.itembunrui, 
            h.juchkbn, 
            tm67.cname AS juchkbncname, 
            h.henreasoncode, 
            (
              (
                (h.henreasoncode):: text || (' : ' :: character varying):: text
              ) || (h.henreasonname):: text
            ) AS henreasonname 
          FROM 
            (
              (
                (
                  (
                    saleh_itemstrategy h 
                    JOIN salem_itemstrategy m ON (
                      (
                        (h.saleno):: text = (m.saleno):: text
                      )
                    )
                  ) 
                  JOIN tm14shkos "k" ON (
                    (
                      (m.itemcode):: text = ("k".itemcode):: text
                    )
                  )
                ) 
                LEFT JOIN tm39item_strategy i ON (
                  (
                    ("k".kosecode):: text = (i.itemcode):: text
                  )
                )
              ) 
              LEFT JOIN tm67juch_nm tm67 ON (
                (
                  (h.juchkbn):: text = (tm67.code):: text
                )
              )
            ) 
          WHERE 
            (
              (
                (
                  (
                    (
                      (
                        (
                          h.shukadate >= (
                            (
                              (
                                (
                                  to_char(
                                    add_months(
                                      ('now' :: character varying):: timestamp without time zone, 
                                      (
                                        - (36):: bigint
                                      )
                                    ), 
                                    ('yyyymm' :: character varying):: text
                                  ) || ('01' :: character varying):: text
                                )
                              ):: integer
                            ):: numeric
                          ):: numeric(18, 0)
                        ) 
                        AND (
                          h.shukadate <= (
                            (
                              (
                                to_char(
                                  (
                                    last_day(
                                      add_months(
                                        ('now' :: character varying):: timestamp without time zone, 
                                        (
                                          - (1):: bigint
                                        )
                                      )
                                    )
                                  ):: timestamp without time zone, 
                                  ('yyyymmdd' :: character varying):: text
                                )
                              ):: integer
                            ):: numeric
                          ):: numeric(18, 0)
                        )
                      ) 
                      AND (
                        h.cancelflg = (
                          (0):: numeric
                        ):: numeric(18, 0)
                      )
                    ) 
                    AND (
                      (
                        (
                          (h.torikeikbn):: text = ('03' :: character varying):: text
                        ) 
                        OR (
                          (h.torikeikbn):: text = ('04' :: character varying):: text
                        )
                      ) 
                      OR (
                        (h.torikeikbn):: text = ('05' :: character varying):: text
                      )
                    )
                  ) 
                  AND (
                    (m.itemcode):: text <> ('9990000100' :: character varying):: text
                  )
                ) 
                AND (
                  (m.itemcode):: text <> ('9990000200' :: character varying):: text
                )
              ) 
              AND (
                (
                  (
                    (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                  ) 
                  OR (
                    (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                  )
                ) 
                OR (
                  (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
                )
              )
            )
        ) view_tuhan_tempo_shohin 
      WHERE 
        (
          (
            view_tuhan_tempo_shohin.shukadate >= (
              (
                (
                  to_char(
                    add_months(
                      ('now' :: character varying):: timestamp without time zone, 
                      (
                        - (15):: bigint
                      )
                    ), 
                    ('yyyymm' :: character varying):: text
                  )
                ):: integer
              ):: character varying
            ):: text
          ) 
          AND (
            view_tuhan_tempo_shohin.shukadate <= (
              (
                (
                  to_char(
                    add_months(
                      ('now' :: character varying):: timestamp without time zone, 
                      (
                        - (4):: bigint
                      )
                    ), 
                    ('yyyymm' :: character varying):: text
                  )
                ):: integer
              ):: character varying
            ):: text
          )
        ) 
      GROUP BY 
        view_tuhan_tempo_shohin.konyuchubuncode, 
        view_tuhan_tempo_shohin.itembunrui, 
        view_tuhan_tempo_shohin.juchkbn, 
        view_tuhan_tempo_shohin.juchkbncname, 
        view_tuhan_tempo_shohin.henreasoncode, 
        view_tuhan_tempo_shohin.henreasonname
    ) 
    UNION 
    SELECT 
      (
        to_char(
          add_months(
            ('now' :: character varying):: timestamp without time zone, 
            (
              - (3):: bigint
            )
          ), 
          ('yyyymm' :: character varying):: text
        )
      ):: integer AS nengetu, 
      count(
        DISTINCT view_tuhan_tempo_shohin.kokyano
      ) AS ninzu, 
      '10:百貨店' AS channel, 
      view_tuhan_tempo_shohin.konyuchubuncode, 
      view_tuhan_tempo_shohin.itembunrui, 
      view_tuhan_tempo_shohin.juchkbn, 
      (
        view_tuhan_tempo_shohin.juchkbncname
      ):: character varying AS juchkbncname, 
      view_tuhan_tempo_shohin.henreasoncode, 
      (
        view_tuhan_tempo_shohin.henreasonname
      ):: character varying AS henreasonname 
    FROM 
      (
        SELECT 
          "substring"(
            (
              (h.shukadate):: character varying
            ):: text, 
            0, 
            6
          ) AS shukadate, 
          h.kokyano, 
          h.daihanrobunname AS konyuchubuncode, 
          i.itembunrui, 
          h.juchkbn, 
          tm67.cname AS juchkbncname, 
          h.henreasoncode, 
          (
            (
              (h.henreasoncode):: text || (' : ' :: character varying):: text
            ) || (h.henreasonname):: text
          ) AS henreasonname 
        FROM 
          (
            (
              (
                (
                  saleh_itemstrategy h 
                  JOIN salem_itemstrategy m ON (
                    (
                      (h.saleno):: text = (m.saleno):: text
                    )
                  )
                ) 
                JOIN tm14shkos "k" ON (
                  (
                    (m.itemcode):: text = ("k".itemcode):: text
                  )
                )
              ) 
              LEFT JOIN tm39item_strategy i ON (
                (
                  ("k".kosecode):: text = (i.itemcode):: text
                )
              )
            ) 
            LEFT JOIN tm67juch_nm tm67 ON (
              (
                (h.juchkbn):: text = (tm67.code):: text
              )
            )
          ) 
        WHERE 
          (
            (
              (
                (
                  (
                    (
                      (
                        h.shukadate >= (
                          (
                            (
                              (
                                to_char(
                                  add_months(
                                    ('now' :: character varying):: timestamp without time zone, 
                                    (
                                      - (36):: bigint
                                    )
                                  ), 
                                  ('yyyymm' :: character varying):: text
                                ) || ('01' :: character varying):: text
                              )
                            ):: integer
                          ):: numeric
                        ):: numeric(18, 0)
                      ) 
                      AND (
                        h.shukadate <= (
                          (
                            (
                              to_char(
                                (
                                  last_day(
                                    add_months(
                                      ('now' :: character varying):: timestamp without time zone, 
                                      (
                                        - (1):: bigint
                                      )
                                    )
                                  )
                                ):: timestamp without time zone, 
                                ('yyyymmdd' :: character varying):: text
                              )
                            ):: integer
                          ):: numeric
                        ):: numeric(18, 0)
                      )
                    ) 
                    AND (
                      h.cancelflg = (
                        (0):: numeric
                      ):: numeric(18, 0)
                    )
                  ) 
                  AND (
                    (
                      (
                        (h.torikeikbn):: text = ('03' :: character varying):: text
                      ) 
                      OR (
                        (h.torikeikbn):: text = ('04' :: character varying):: text
                      )
                    ) 
                    OR (
                      (h.torikeikbn):: text = ('05' :: character varying):: text
                    )
                  )
                ) 
                AND (
                  (m.itemcode):: text <> ('9990000100' :: character varying):: text
                )
              ) 
              AND (
                (m.itemcode):: text <> ('9990000200' :: character varying):: text
              )
            ) 
            AND (
              (
                (
                  (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
                ) 
                OR (
                  (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
                )
              ) 
              OR (
                (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
              )
            )
          )
      ) view_tuhan_tempo_shohin 
    WHERE 
      (
        (
          view_tuhan_tempo_shohin.shukadate >= (
            (
              (
                to_char(
                  add_months(
                    ('now' :: character varying):: timestamp without time zone, 
                    (
                      - (14):: bigint
                    )
                  ), 
                  ('yyyymm' :: character varying):: text
                )
              ):: integer
            ):: character varying
          ):: text
        ) 
        AND (
          view_tuhan_tempo_shohin.shukadate <= (
            (
              (
                to_char(
                  add_months(
                    ('now' :: character varying):: timestamp without time zone, 
                    (
                      - (3):: bigint
                    )
                  ), 
                  ('yyyymm' :: character varying):: text
                )
              ):: integer
            ):: character varying
          ):: text
        )
      ) 
    GROUP BY 
      view_tuhan_tempo_shohin.konyuchubuncode, 
      view_tuhan_tempo_shohin.itembunrui, 
      view_tuhan_tempo_shohin.juchkbn, 
      view_tuhan_tempo_shohin.juchkbncname, 
      view_tuhan_tempo_shohin.henreasoncode, 
      view_tuhan_tempo_shohin.henreasonname
  ) 
  UNION 
  SELECT 
    (
      to_char(
        add_months(
          ('now' :: character varying):: timestamp without time zone, 
          (
            - (2):: bigint
          )
        ), 
        ('yyyymm' :: character varying):: text
      )
    ):: integer AS nengetu, 
    count(
      DISTINCT view_tuhan_tempo_shohin.kokyano
    ) AS ninzu, 
    '10:百貨店' AS channel, 
    view_tuhan_tempo_shohin.konyuchubuncode, 
    view_tuhan_tempo_shohin.itembunrui, 
    view_tuhan_tempo_shohin.juchkbn, 
    (
      view_tuhan_tempo_shohin.juchkbncname
    ):: character varying AS juchkbncname, 
    view_tuhan_tempo_shohin.henreasoncode, 
    (
      view_tuhan_tempo_shohin.henreasonname
    ):: character varying AS henreasonname 
  FROM 
    (
      SELECT 
        "substring"(
          (
            (h.shukadate):: character varying
          ):: text, 
          0, 
          6
        ) AS shukadate, 
        h.kokyano, 
        h.daihanrobunname AS konyuchubuncode, 
        i.itembunrui, 
        h.juchkbn, 
        tm67.cname AS juchkbncname, 
        h.henreasoncode, 
        (
          (
            (h.henreasoncode):: text || (' : ' :: character varying):: text
          ) || (h.henreasonname):: text
        ) AS henreasonname 
      FROM 
        (
          (
            (
              (
                saleh_itemstrategy h 
                JOIN salem_itemstrategy m ON (
                  (
                    (h.saleno):: text = (m.saleno):: text
                  )
                )
              ) 
              JOIN tm14shkos "k" ON (
                (
                  (m.itemcode):: text = ("k".itemcode):: text
                )
              )
            ) 
            LEFT JOIN tm39item_strategy i ON (
              (
                ("k".kosecode):: text = (i.itemcode):: text
              )
            )
          ) 
          LEFT JOIN tm67juch_nm tm67 ON (
            (
              (h.juchkbn):: text = (tm67.code):: text
            )
          )
        ) 
      WHERE 
        (
          (
            (
              (
                (
                  (
                    (
                      h.shukadate >= (
                        (
                          (
                            (
                              to_char(
                                add_months(
                                  ('now' :: character varying):: timestamp without time zone, 
                                  (
                                    - (36):: bigint
                                  )
                                ), 
                                ('yyyymm' :: character varying):: text
                              ) || ('01' :: character varying):: text
                            )
                          ):: integer
                        ):: numeric
                      ):: numeric(18, 0)
                    ) 
                    AND (
                      h.shukadate <= (
                        (
                          (
                            to_char(
                              (
                                last_day(
                                  add_months(
                                    ('now' :: character varying):: timestamp without time zone, 
                                    (
                                      - (1):: bigint
                                    )
                                  )
                                )
                              ):: timestamp without time zone, 
                              ('yyyymmdd' :: character varying):: text
                            )
                          ):: integer
                        ):: numeric
                      ):: numeric(18, 0)
                    )
                  ) 
                  AND (
                    h.cancelflg = (
                      (0):: numeric
                    ):: numeric(18, 0)
                  )
                ) 
                AND (
                  (
                    (
                      (h.torikeikbn):: text = ('03' :: character varying):: text
                    ) 
                    OR (
                      (h.torikeikbn):: text = ('04' :: character varying):: text
                    )
                  ) 
                  OR (
                    (h.torikeikbn):: text = ('05' :: character varying):: text
                  )
                )
              ) 
              AND (
                (m.itemcode):: text <> ('9990000100' :: character varying):: text
              )
            ) 
            AND (
              (m.itemcode):: text <> ('9990000200' :: character varying):: text
            )
          ) 
          AND (
            (
              (
                (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
              ) 
              OR (
                (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
              )
            ) 
            OR (
              (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
            )
          )
        )
    ) view_tuhan_tempo_shohin 
  WHERE 
    (
      (
        view_tuhan_tempo_shohin.shukadate >= (
          (
            (
              to_char(
                add_months(
                  ('now' :: character varying):: timestamp without time zone, 
                  (
                    - (13):: bigint
                  )
                ), 
                ('yyyymm' :: character varying):: text
              )
            ):: integer
          ):: character varying
        ):: text
      ) 
      AND (
        view_tuhan_tempo_shohin.shukadate <= (
          (
            (
              to_char(
                add_months(
                  ('now' :: character varying):: timestamp without time zone, 
                  (
                    - (2):: bigint
                  )
                ), 
                ('yyyymm' :: character varying):: text
              )
            ):: integer
          ):: character varying
        ):: text
      )
    ) 
  GROUP BY 
    view_tuhan_tempo_shohin.konyuchubuncode, 
    view_tuhan_tempo_shohin.itembunrui, 
    view_tuhan_tempo_shohin.juchkbn, 
    view_tuhan_tempo_shohin.juchkbncname, 
    view_tuhan_tempo_shohin.henreasoncode, 
    view_tuhan_tempo_shohin.henreasonname
) 
UNION 
SELECT 
  (
    to_char(
      add_months(
        ('now' :: character varying):: timestamp without time zone, 
        (
          - (1):: bigint
        )
      ), 
      ('yyyymm' :: character varying):: text
    )
  ):: integer AS nengetu, 
  count(
    DISTINCT view_tuhan_tempo_shohin.kokyano
  ) AS ninzu, 
  '10:百貨店' AS channel, 
  view_tuhan_tempo_shohin.konyuchubuncode, 
  view_tuhan_tempo_shohin.itembunrui, 
  view_tuhan_tempo_shohin.juchkbn, 
  (
    view_tuhan_tempo_shohin.juchkbncname
  ):: character varying AS juchkbncname, 
  view_tuhan_tempo_shohin.henreasoncode, 
  (
    view_tuhan_tempo_shohin.henreasonname
  ):: character varying AS henreasonname 
FROM 
  (
    SELECT 
      "substring"(
        (
          (h.shukadate):: character varying
        ):: text, 
        0, 
        6
      ) AS shukadate, 
      h.kokyano, 
      h.daihanrobunname AS konyuchubuncode, 
      i.itembunrui, 
      h.juchkbn, 
      tm67.cname AS juchkbncname, 
      h.henreasoncode, 
      (
        (
          (h.henreasoncode):: text || (' : ' :: character varying):: text
        ) || (h.henreasonname):: text
      ) AS henreasonname 
    FROM 
      (
        (
          (
            (
              saleh_itemstrategy h 
              JOIN salem_itemstrategy m ON (
                (
                  (h.saleno):: text = (m.saleno):: text
                )
              )
            ) 
            JOIN tm14shkos "k" ON (
              (
                (m.itemcode):: text = ("k".itemcode):: text
              )
            )
          ) 
          LEFT JOIN tm39item_strategy i ON (
            (
              ("k".kosecode):: text = (i.itemcode):: text
            )
          )
        ) 
        LEFT JOIN tm67juch_nm tm67 ON (
          (
            (h.juchkbn):: text = (tm67.code):: text
          )
        )
      ) 
    WHERE 
      (
        (
          (
            (
              (
                (
                  (
                    h.shukadate >= (
                      (
                        (
                          (
                            to_char(
                              add_months(
                                ('now' :: character varying):: timestamp without time zone, 
                                (
                                  - (36):: bigint
                                )
                              ), 
                              ('yyyymm' :: character varying):: text
                            ) || ('01' :: character varying):: text
                          )
                        ):: integer
                      ):: numeric
                    ):: numeric(18, 0)
                  ) 
                  AND (
                    h.shukadate <= (
                      (
                        (
                          to_char(
                            (
                              last_day(
                                add_months(
                                  ('now' :: character varying):: timestamp without time zone, 
                                  (
                                    - (1):: bigint
                                  )
                                )
                              )
                            ):: timestamp without time zone, 
                            ('yyyymmdd' :: character varying):: text
                          )
                        ):: integer
                      ):: numeric
                    ):: numeric(18, 0)
                  )
                ) 
                AND (
                  h.cancelflg = (
                    (0):: numeric
                  ):: numeric(18, 0)
                )
              ) 
              AND (
                (
                  (
                    (h.torikeikbn):: text = ('03' :: character varying):: text
                  ) 
                  OR (
                    (h.torikeikbn):: text = ('04' :: character varying):: text
                  )
                ) 
                OR (
                  (h.torikeikbn):: text = ('05' :: character varying):: text
                )
              )
            ) 
            AND (
              (m.itemcode):: text <> ('9990000100' :: character varying):: text
            )
          ) 
          AND (
            (m.itemcode):: text <> ('9990000200' :: character varying):: text
          )
        ) 
        AND (
          (
            (
              (i.itembunrui):: text = ('01_ACGELEX' :: character varying):: text
            ) 
            OR (
              (i.itembunrui):: text = ('02_ACGBY' :: character varying):: text
            )
          ) 
          OR (
            (i.itembunrui):: text = ('03_ACGSMEX' :: character varying):: text
          )
        )
      )
  ) view_tuhan_tempo_shohin 
WHERE 
  (
    (
      view_tuhan_tempo_shohin.shukadate >= (
        (
          (
            to_char(
              add_months(
                ('now' :: character varying):: timestamp without time zone, 
                (
                  - (12):: bigint
                )
              ), 
              ('yyyymm' :: character varying):: text
            )
          ):: integer
        ):: character varying
      ):: text
    ) 
    AND (
      view_tuhan_tempo_shohin.shukadate <= (
        (
          (
            to_char(
              add_months(
                ('now' :: character varying):: timestamp without time zone, 
                (
                  - (1):: bigint
                )
              ), 
              ('yyyymm' :: character varying):: text
            )
          ):: integer
        ):: character varying
      ):: text
    )
  ) 
GROUP BY 
  view_tuhan_tempo_shohin.konyuchubuncode, 
  view_tuhan_tempo_shohin.itembunrui, 
  view_tuhan_tempo_shohin.juchkbn, 
  view_tuhan_tempo_shohin.juchkbncname, 
  view_tuhan_tempo_shohin.henreasoncode, 
  view_tuhan_tempo_shohin.henreasonname
)
select * from final