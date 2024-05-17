with edw_crncy_exch as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_CRNCY_EXCH
),
final as (
SELECT drvd_crncy.ex_rt_typ,
  drvd_crncy.from_crncy,
  drvd_crncy.to_crncy,
  drvd_crncy.vld_from,
  ((drvd_crncy.ex_rt * (drvd_crncy.to_ratio / drvd_crncy.from_ratio)))::NUMERIC(20, 10) AS ex_rt
FROM (
  (
    SELECT DISTINCT edw_crncy_exch.ex_rt_typ,
      edw_crncy_exch.from_crncy,
      edw_crncy_exch.to_crncy,
      to_date((((((99999999)::NUMERIC)::NUMERIC(18, 0) - ((edw_crncy_exch.vld_from)::NUMERIC)::NUMERIC(18, 0)))::CHARACTER VARYING)::TEXT, ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS vld_from,
      edw_crncy_exch.ex_rt,
      edw_crncy_exch.from_ratio,
      edw_crncy_exch.to_ratio
    FROM edw_crncy_exch edw_crncy_exch
    WHERE (
        (
          (
            (
              (
                ((edw_crncy_exch.to_crncy)::TEXT = ('HKD'::CHARACTER VARYING)::TEXT)
                OR ((edw_crncy_exch.to_crncy)::TEXT = ('KRW'::CHARACTER VARYING)::TEXT)
                )
              OR ((edw_crncy_exch.to_crncy)::TEXT = ('SGD'::CHARACTER VARYING)::TEXT)
              )
            OR ((edw_crncy_exch.to_crncy)::TEXT = ('USD'::CHARACTER VARYING)::TEXT)
            )
          OR ((edw_crncy_exch.to_crncy)::TEXT = ('TWD'::CHARACTER VARYING)::TEXT)
          )
        AND ((edw_crncy_exch.ex_rt_typ)::TEXT = ('BWAR'::CHARACTER VARYING)::TEXT)
        )
    ) drvd_crncy JOIN (
    SELECT edw_crncy_exch.ex_rt_typ,
      edw_crncy_exch.from_crncy,
      edw_crncy_exch.to_crncy,
      "max" (to_date((((((99999999)::NUMERIC)::NUMERIC(18, 0) - ((edw_crncy_exch.vld_from)::NUMERIC)::NUMERIC(18, 0)))::CHARACTER VARYING)::TEXT, ('YYYYMMDD'::CHARACTER VARYING)::TEXT)) AS vld_from
    FROM edw_crncy_exch edw_crncy_exch
    WHERE (
        (
          (
            (
              (
                ((edw_crncy_exch.to_crncy)::TEXT = ('HKD'::CHARACTER VARYING)::TEXT)
                OR ((edw_crncy_exch.to_crncy)::TEXT = ('KRW'::CHARACTER VARYING)::TEXT)
                )
              OR ((edw_crncy_exch.to_crncy)::TEXT = ('SGD'::CHARACTER VARYING)::TEXT)
              )
            OR ((edw_crncy_exch.to_crncy)::TEXT = ('USD'::CHARACTER VARYING)::TEXT)
            )
          OR ((edw_crncy_exch.to_crncy)::TEXT = ('TWD'::CHARACTER VARYING)::TEXT)
          )
        AND ((edw_crncy_exch.ex_rt_typ)::TEXT = ('BWAR'::CHARACTER VARYING)::TEXT)
        )
    GROUP BY edw_crncy_exch.ex_rt_typ,
      edw_crncy_exch.from_crncy,
      edw_crncy_exch.to_crncy
    ) max_dt ON (
      (
        (
          (
            ((drvd_crncy.ex_rt_typ)::TEXT = (max_dt.ex_rt_typ)::TEXT)
            AND ((drvd_crncy.from_crncy)::TEXT = (max_dt.from_crncy)::TEXT)
            )
          AND ((drvd_crncy.to_crncy)::TEXT = (max_dt.to_crncy)::TEXT)
          )
        AND (drvd_crncy.vld_from = max_dt.vld_from)
        )
      )
  )

UNION ALL

SELECT DISTINCT edw_crncy_exch.ex_rt_typ,
  edw_crncy_exch.from_crncy,
  edw_crncy_exch.from_crncy AS to_crncy,
  (current_timestamp()::TIMESTAMP without TIME zone)::DATE AS vld_from,
  1 AS ex_rt
FROM edw_crncy_exch edw_crncy_exch
WHERE (
    (
      (
        (
          (
            ((edw_crncy_exch.to_crncy)::TEXT = ('HKD'::CHARACTER VARYING)::TEXT)
            OR ((edw_crncy_exch.to_crncy)::TEXT = ('KRW'::CHARACTER VARYING)::TEXT)
            )
          OR ((edw_crncy_exch.to_crncy)::TEXT = ('SGD'::CHARACTER VARYING)::TEXT)
          )
        OR ((edw_crncy_exch.to_crncy)::TEXT = ('USD'::CHARACTER VARYING)::TEXT)
        )
      OR ((edw_crncy_exch.to_crncy)::TEXT = ('TWD'::CHARACTER VARYING)::TEXT)
      )
    AND ((edw_crncy_exch.ex_rt_typ)::TEXT = ('BWAR'::CHARACTER VARYING)::TEXT)
    )
)
select * from final
