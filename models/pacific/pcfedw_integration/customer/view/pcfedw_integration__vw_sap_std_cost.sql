with vw_curr_exch_dim as (
select * from {{ ref('pcfedw_integration__vw_curr_exch_dim') }}
),
edw_ecc_standard_cost as (
select * from {{ ref('aspedw_integration__edw_ecc_standard_cost') }}
),
final as (
SELECT 
  derived_table1.matnr, 
  derived_table1.cmp_no, 
  derived_table1.laepr, 
  derived_table1.peinh, 
  derived_table1.stprs, 
  derived_table1.std_cost, 
  derived_table1.exchng_rate, 
  derived_table1.bwkey, 
  (
    derived_table1.std_cost * derived_table1.exchng_rate
  ) AS std_cost_aud 
FROM 
  (
    SELECT 
      DISTINCT edw_ecc_standard_cost.matnr, 
      edw_ecc_standard_cost.bwkey, 
      CASE WHEN (
        edw_ecc_standard_cost.bwkey:: text = '3300'
      ) THEN '7470' 
      WHEN (
        edw_ecc_standard_cost.bwkey:: text = '330A'
      ) 
      THEN '7470' :: character varying 
      WHEN (
        edw_ecc_standard_cost.bwkey = '3410'
      ) THEN '8361' :: character varying 
      WHEN (
        edw_ecc_standard_cost.bwkey:: text = '341A'
      ) THEN '8361' :: character varying 
      ELSE NULL :: character varying END AS cmp_no, 
      edw_ecc_standard_cost.laepr, 
      edw_ecc_standard_cost.peinh, 
      edw_ecc_standard_cost.stprs, 
      (
        edw_ecc_standard_cost.stprs / edw_ecc_standard_cost.peinh
      ) AS std_cost, 
      (
        SELECT 
          vw_curr_exch_dim.exch_rate 
        FROM 
          vw_curr_exch_dim 
        WHERE 
          (
            (
              (
                (
                  (vw_curr_exch_dim.rate_type):: text = ('DWBP' :: character varying):: text
                ) 
                AND (
                  (vw_curr_exch_dim.from_ccy):: text = ('SGD' :: character varying):: text
                )
              ) 
              AND (
                (vw_curr_exch_dim.to_ccy):: text = ('AUD' :: character varying):: text
              )
            ) 
            AND (
              vw_curr_exch_dim.valid_date = (
                SELECT 
                  "max"(vw_curr_exch_dim.valid_date) AS "max" 
                FROM 
                  vw_curr_exch_dim 
                WHERE 
                  (
                    (
                      (
                        (
                          (vw_curr_exch_dim.rate_type):: text = ('DWBP' :: character varying):: text
                        ) 
                        AND (
                          (vw_curr_exch_dim.from_ccy):: text = ('SGD' :: character varying):: text
                        )
                      ) 
                      AND (
                        (vw_curr_exch_dim.to_ccy):: text = ('AUD' :: character varying):: text
                      )
                    ) 
                    AND (
                      vw_curr_exch_dim.valid_date <= (
                        (
                          (
                            to_char(
                              (current_timestamp() :: character varying):: timestamp without time zone, 
                              ('YYYYMMDD' :: character varying):: text
                            )
                          ):: integer
                        ):: numeric
                      ):: numeric(18, 0)
                    )
                  )
              )
            )
          )
      ) AS exchng_rate 
    FROM 
      edw_ecc_standard_cost 
    WHERE 
      (
        (
          (
            (
              (
                (
                  (edw_ecc_standard_cost.bwkey):: text = ('3330' :: character varying):: text
                ) 
                OR (
                  (edw_ecc_standard_cost.bwkey):: text = ('330A' :: character varying):: text
                )
              ) 
              OR (
                (edw_ecc_standard_cost.bwkey):: text = ('3410' :: character varying):: text
              )
            ) 
            OR (
              (edw_ecc_standard_cost.bwkey):: text = ('341A' :: character varying):: text
            )
          ) 
          AND (
            (edw_ecc_standard_cost.matnr):: text like ('00%' :: character varying):: text
          )
        ) 
        AND (
          (edw_ecc_standard_cost.bwkey):: text like ('%A' :: character varying):: text
        )
      )
  ) derived_table1
  )
  select * from final