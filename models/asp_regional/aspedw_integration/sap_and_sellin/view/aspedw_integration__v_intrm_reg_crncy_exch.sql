{{
    config(
        materialized='view'
    )
}}

with edw_crncy_exch as
(
    select * from {{ ref('aspedw_integration__edw_crncy_exch') }}
),
final as
(
    SELECT 
  drvd_crncy.ex_rt_typ, 
  drvd_crncy.from_crncy, 
  drvd_crncy.to_crncy, 
  drvd_crncy.vld_from, 
  (
    CASE WHEN (
      (
        (drvd_crncy.to_crncy):: text = ('USD' :: character varying):: text
      ) 
      AND (
        (
          (
            (drvd_crncy.from_crncy):: text = ('IDR' :: character varying):: text
          ) 
          OR (
            (drvd_crncy.from_crncy):: text = ('KRW' :: character varying):: text
          )
        ) 
        OR (
          (drvd_crncy.from_crncy):: text = ('VND' :: character varying):: text
        )
      )
    ) THEN (
      drvd_crncy.ex_rt / (
        (1000):: numeric
      ):: numeric(18, 0)
    ) WHEN (
      (
        (drvd_crncy.to_crncy):: text = ('USD' :: character varying):: text
      ) 
      AND (
        (drvd_crncy.from_crncy):: text = ('JPY' :: character varying):: text
      )
    ) THEN (
      drvd_crncy.ex_rt / (
        (100):: numeric
      ):: numeric(18, 0)
    ) ELSE drvd_crncy.ex_rt END
  ):: numeric(20, 10) AS ex_rt 
FROM 
  (
    (
      SELECT 
        DISTINCT edw_crncy_exch.ex_rt_typ, 
        edw_crncy_exch.from_crncy, 
        edw_crncy_exch.to_crncy, 
        to_date(
          (
            (
              (
                (
                  (99999999):: numeric
                ):: numeric(18, 0) - (
                  (edw_crncy_exch.vld_from):: numeric
                ):: numeric(18, 0)
              )
            ):: character varying
          ):: text, 
          ('YYYYMMDD' :: character varying):: text
        ) AS vld_from, 
        edw_crncy_exch.ex_rt, 
        edw_crncy_exch.from_ratio, 
        edw_crncy_exch.to_ratio 
      FROM 
        edw_crncy_exch edw_crncy_exch 
      WHERE 
        (
          (edw_crncy_exch.ex_rt_typ):: text = ('DWBP' :: character varying):: text
        )
    ) drvd_crncy 
    JOIN (
      SELECT 
        edw_crncy_exch.ex_rt_typ, 
        edw_crncy_exch.from_crncy, 
        edw_crncy_exch.to_crncy, 
        "max"(
          to_date(
            (
              (
                (
                  (
                    (99999999):: numeric
                  ):: numeric(18, 0) - (
                    (edw_crncy_exch.vld_from):: numeric
                  ):: numeric(18, 0)
                )
              ):: character varying
            ):: text, 
            ('YYYYMMDD' :: character varying):: text
          )
        ) AS vld_from 
      FROM 
        edw_crncy_exch edw_crncy_exch 
      WHERE 
        (
          (edw_crncy_exch.ex_rt_typ):: text = ('DWBP' :: character varying):: text
        ) 
      GROUP BY 
        edw_crncy_exch.ex_rt_typ, 
        edw_crncy_exch.from_crncy, 
        edw_crncy_exch.to_crncy
    ) max_dt ON (
      (
        (
          (
            (
              (drvd_crncy.ex_rt_typ):: text = (max_dt.ex_rt_typ):: text
            ) 
            AND (
              (drvd_crncy.from_crncy):: text = (max_dt.from_crncy):: text
            )
          ) 
          AND (
            (drvd_crncy.to_crncy):: text = (max_dt.to_crncy):: text
          )
        ) 
        AND (
          drvd_crncy.vld_from = max_dt.vld_from
        )
      )
    )
  ) 
UNION ALL 
SELECT 
  DISTINCT edw_crncy_exch.ex_rt_typ, 
  edw_crncy_exch.from_crncy, 
  edw_crncy_exch.from_crncy AS to_crncy, 
  (
    convert_timezone('UTC',current_timestamp()):: date
  ) AS vld_from, 
  1 AS ex_rt 
FROM 
  edw_crncy_exch edw_crncy_exch 
WHERE 
  (
    (edw_crncy_exch.ex_rt_typ):: text = ('DWBP' :: character varying):: text
  )
)
select * from final