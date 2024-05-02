with vw_jjbr_curr_exch_dim as (
select * from {{ ref('pcfedw_integration__vw_jjbr_curr_exch_dim') }}
),
vw_bwar_curr_exch_dim as (
select * from {{ ref('pcfedw_integration__vw_bwar_curr_exch_dim') }}
),
vw_curr_exch_dim as (
select * from {{ ref('pcfedw_integration__vw_curr_exch_dim') }}
),
edw_copa_trans_fact as (
select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_ciw_accnt_lkp as (
select * from {{ ref('pcfedw_integration__edw_ciw_accnt_lkp') }}
),
dly_sls_cust_attrb_lkp as (
select * from {{ ref('pcfedw_integration__dly_sls_cust_attrb_lkp') }}
),
vw_sap_std_cost as (
select * from {{ ref('pcfedw_integration__vw_sap_std_cost') }}
),
vw_actual_cogs_rate_dim as (
select * from {{ ref('pcfedw_integration__vw_actual_cogs_rate_dim') }}
),
edw_account_dim as (
select * from {{ ref('aspedw_integration__edw_account_dim') }}
),
max_query as (
                            SELECT 
                              max(
                                vw_actual_cogs_rate_dim.jj_mnth_id
                              ) AS "max" 
                            FROM 
                              vw_actual_cogs_rate_dim 
                            WHERE 
                              (
                                "substring"(
                                  (
                                    (
                                      vw_actual_cogs_rate_dim.jj_mnth_id
                                    ):: character varying
                                  ):: text, 
                                  1, 
                                  4
                                ) <=(
                                TO_CHAR(DATE_TRUNC('YEAR', CURRENT_TIMESTAMP), 'YYYY')))
                            
                        ) ,
final as (
SELECT 
  a.cmp_id, 
  a.cal_month_id, 
  a.jj_month_id, 
  a.cust_no, 
  a.matl_id, 
  a.key_measure, 
  a.ciw_category AS ciw_ctgry, 
  a.ciw_account_group AS ciw_accnt_grp, 
  a.sap_account AS sap_accnt, 
  a.sap_account_nm AS sap_accnt_nm, 
  a.local_ccy, 
  b.exch_rate AS aud_rate, 
  CASE WHEN (
    (
      (a.key_measure):: text = (
        'Cost of Goods Sold' :: character varying
      ):: text
    ) 
    OR (
      (a.key_measure IS NULL) 
      AND ('Cost of Goods Sold' IS NULL)
    )
  ) THEN (- a.base_measure) WHEN (
    (
      (a.key_measure):: text = (
        'Consumer Free Goods' :: character varying
      ):: text
    ) 
    OR (
      (a.key_measure IS NULL) 
      AND ('Consumer Free Goods' IS NULL)
    )
  ) THEN (- a.base_measure) ELSE a.base_measure END AS base_measure, 
  a.sales_qty, 
  a.gts_val, 
  a.retrn_val AS retrn, 
  a.gts_val AS gts_less_rtrn_val, 
  a.eff_val, 
  a.jgf_si_val, 
  a.pmt_terms_val, 
  a.datains_val, 
  a.exp_adj_val, 
  a.jgf_sd_val, 
  (
    a.gts_val - (
      (
        (
          (
            (a.eff_val + a.jgf_si_val) + a.pmt_terms_val
          ) + a.datains_val
        ) + a.exp_adj_val
      ) + a.jgf_sd_val
    )
  ) AS nts_val, 
  (
    (
      (
        (
          (a.eff_val + a.jgf_si_val) + a.pmt_terms_val
        ) + a.datains_val
      ) + a.exp_adj_val
    ) + a.jgf_sd_val
  ) AS tot_ciw_val, 
  (- a.cogs_val) AS cogs_val, 
  (- a.con_free_goods_val) AS con_free_goods_val, 
  (
    (
      (
        a.gts_val - (
          (
            (
              (
                (a.eff_val + a.jgf_si_val) + a.pmt_terms_val
              ) + a.datains_val
            ) + a.exp_adj_val
          ) + a.jgf_sd_val
        )
      ) + a.cogs_val
    ) + a.con_free_goods_val
  ) AS gp_val, 
  (
    COALESCE(
      c.std_cost_aud, 
      (
        (0):: numeric
      ):: numeric(18, 0)
    ) * (
      (
        (1):: numeric
      ):: numeric(18, 0) / b.exch_rate
    )
  ) AS pc_std_cost_lc, 
  (
    a.sales_qty * (
      COALESCE(
        c.std_cost_aud, 
        (
          (0):: numeric
        ):: numeric(18, 0)
      ) * (
        (
          (1):: numeric
        ):: numeric(18, 0) / b.exch_rate
      )
    )
  ) AS std_cost_lc, 
  (
    (
      a.gts_val - (
        (
          (
            (
              (a.eff_val + a.jgf_si_val) + a.pmt_terms_val
            ) + a.datains_val
          ) + a.exp_adj_val
        ) + a.jgf_sd_val
      )
    ) - (
      a.sales_qty * (
        COALESCE(
          c.std_cost_aud, 
          (
            (0):: numeric
          ):: numeric(18, 0)
        ) * (
          (
            (1):: numeric
          ):: numeric(18, 0) / b.exch_rate
        )
      )
    )
  ) AS std_cost_gp_val 
FROM 
  vw_jjbr_curr_exch_dim b, 
  (
    (
      SELECT 
        derived_table2.cmp_id, 
        derived_table2.cal_month_id, 
        derived_table2.jj_month_id, 
        derived_table2.cust_no, 
        derived_table2.matl_id, 
        derived_table2.key_measure, 
        derived_table2.ciw_category, 
        derived_table2.ciw_account_group, 
        derived_table2.sap_account, 
        derived_table2.sap_account_nm, 
        d.to_ccy AS local_ccy, 
        (
          derived_table2.base_measure * d.exch_rate
        ) AS base_measure, 
        derived_table2.sales_qty, 
        (
          derived_table2.gts_val * d.exch_rate
        ) AS gts_val, 
        (
          derived_table2.retrn_val * d.exch_rate
        ) AS retrn_val, 
        (
          derived_table2.eff_val * d.exch_rate
        ) AS eff_val, 
        (
          derived_table2.jgf_si_val * d.exch_rate
        ) AS jgf_si_val, 
        (
          derived_table2.pmt_terms_val * d.exch_rate
        ) AS pmt_terms_val, 
        (
          derived_table2.datains_val * d.exch_rate
        ) AS datains_val, 
        (
          derived_table2.exp_adj_val * d.exch_rate
        ) AS exp_adj_val, 
        (
          derived_table2.jgf_sd_val * d.exch_rate
        ) AS jgf_sd_val, 
        (
          derived_table2.cogs_val * d.exch_rate
        ) AS cogs_val, 
        (
          derived_table2.con_free_goods_val * d.exch_rate
        ) AS con_free_goods_val 
      FROM 
        (
          (
            (
              (
                (
                  (
                    (
                      SELECT 
                        vw_actual_cogs_rate_dim.cmp_id, 
                        vw_actual_cogs_rate_dim.from_ccy, 
                        vw_actual_cogs_rate_dim.to_ccy, 
                        vw_actual_cogs_rate_dim.jj_mnth_id, 
                        vw_actual_cogs_rate_dim.exch_rate 
                      FROM 
                        vw_actual_cogs_rate_dim 
                      WHERE 
                        (
                          vw_actual_cogs_rate_dim.jj_mnth_id IN (
                            SELECT 
                              "max"(
                                vw_actual_cogs_rate_dim.jj_mnth_id
                              ) AS "max" 
                            FROM 
                              vw_actual_cogs_rate_dim 
                            WHERE 
                              (
                                "substring"(
                                  (
                                    (
                                      vw_actual_cogs_rate_dim.jj_mnth_id
                                    ):: character varying
                                  ):: text, 
                                  1, 
                                  4
                                ) <= (
                                  (
                                    date_part(year,
                                      
                                        (current_timestamp()::character varying)::date
                                      
                                    )
                                  ):: character varying
                                ):: text
                              ) 
                            GROUP BY 
                              vw_actual_cogs_rate_dim.cmp_id, 
                              vw_actual_cogs_rate_dim.from_ccy, 
                              vw_actual_cogs_rate_dim.to_ccy
                          )
                        ) 
                      UNION 
                      SELECT 
                        '7470' :: character varying AS cmp_id, 
                        vw_jjbr_curr_exch_dim.from_ccy, 
                        vw_jjbr_curr_exch_dim.to_ccy, 
                        vw_jjbr_curr_exch_dim.jj_mnth_id, 
                        vw_jjbr_curr_exch_dim.exch_rate 
                      FROM 
                        vw_jjbr_curr_exch_dim 
                      WHERE 
                        (
                          (
                            (
                              (vw_jjbr_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
                            ) 
                            AND (
                              (vw_jjbr_curr_exch_dim.to_ccy):: text = ('AUD' :: character varying):: text
                            )
                          ) 
                          AND (
                            vw_jjbr_curr_exch_dim.jj_mnth_id IN (
                              SELECT 
                                "max"(
                                  vw_jjbr_curr_exch_dim.jj_mnth_id
                                ) AS "max" 
                              FROM 
                                vw_jjbr_curr_exch_dim 
                              WHERE 
                                (
                                  (
                                    (
                                      (vw_jjbr_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
                                    ) 
                                    AND (
                                      (vw_jjbr_curr_exch_dim.to_ccy):: text = ('AUD' :: character varying):: text
                                    )
                                  ) 
                                  AND (
                                    "substring"(
                                      (
                                        (
                                          vw_jjbr_curr_exch_dim.jj_mnth_id
                                        ):: character varying
                                      ):: text, 
                                      1, 
                                      4
                                    ) <= (
                                      (
                                        date_part(year,
                                          
                                            (current_timestamp()::character varying)::date
                                          
                                        )
                                      ):: character varying
                                    ):: text
                                  )
                                ) 
                              GROUP BY 
                                vw_jjbr_curr_exch_dim.from_ccy, 
                                vw_jjbr_curr_exch_dim.to_ccy
                            )
                          )
                        )
                    ) 
                    UNION 
                    SELECT 
                      '8361' :: character varying AS cmp_id, 
                      vw_jjbr_curr_exch_dim.from_ccy, 
                      vw_jjbr_curr_exch_dim.to_ccy, 
                      vw_jjbr_curr_exch_dim.jj_mnth_id, 
                      vw_jjbr_curr_exch_dim.exch_rate 
                    FROM 
                      vw_jjbr_curr_exch_dim 
                    WHERE 
                      (
                        (
                          (
                            (vw_jjbr_curr_exch_dim.from_ccy):: text = ('NZD' :: character varying):: text
                          ) 
                          AND (
                            (vw_jjbr_curr_exch_dim.to_ccy):: text = ('NZD' :: character varying):: text
                          )
                        ) 
                        AND (
                          vw_jjbr_curr_exch_dim.jj_mnth_id IN (
                            SELECT 
                              "max"(
                                vw_jjbr_curr_exch_dim.jj_mnth_id
                              ) AS "max" 
                            FROM 
                              vw_jjbr_curr_exch_dim 
                            WHERE 
                              (
                                (
                                  (
                                    (vw_jjbr_curr_exch_dim.from_ccy):: text = ('NZD' :: character varying):: text
                                  ) 
                                  AND (
                                    (vw_jjbr_curr_exch_dim.to_ccy):: text = ('NZD' :: character varying):: text
                                  )
                                ) 
                                AND (
                                  "substring"(
                                    (
                                      (
                                        vw_jjbr_curr_exch_dim.jj_mnth_id
                                      ):: character varying
                                    ):: text, 
                                    1, 
                                    4
                                  ) <= (
                                    (
                                      date_part(year,
                                       
                                          (current_timestamp() :: character varying):: date
                                        
                                      )
                                    ):: character varying
                                  ):: text
                                )
                              ) 
                            GROUP BY 
                              vw_jjbr_curr_exch_dim.from_ccy, 
                              vw_jjbr_curr_exch_dim.to_ccy
                          )
                        )
                      )
                  ) 
                  UNION 
                  SELECT 
                    '7470' :: character varying AS cmp_id, 
                    vw_jjbr_curr_exch_dim.from_ccy, 
                    vw_jjbr_curr_exch_dim.to_ccy, 
                    vw_jjbr_curr_exch_dim.jj_mnth_id, 
                    vw_jjbr_curr_exch_dim.exch_rate 
                  FROM 
                    vw_jjbr_curr_exch_dim 
                  WHERE 
                    (
                      (
                        (
                          (vw_jjbr_curr_exch_dim.from_ccy):: text = ('NZD' :: character varying):: text
                        ) 
                        AND (
                          (vw_jjbr_curr_exch_dim.to_ccy):: text = ('AUD' :: character varying):: text
                        )
                      ) 
                      AND (
                        vw_jjbr_curr_exch_dim.jj_mnth_id IN (
                          SELECT 
                            "max"(
                              vw_jjbr_curr_exch_dim.jj_mnth_id
                            ) AS "max" 
                          FROM 
                            vw_jjbr_curr_exch_dim 
                          WHERE 
                            (
                              (
                                (
                                  (vw_jjbr_curr_exch_dim.from_ccy):: text = ('NZD' :: character varying):: text
                                ) 
                                AND (
                                  (vw_jjbr_curr_exch_dim.to_ccy):: text = ('AUD' :: character varying):: text
                                )
                              ) 
                              AND (
                                "substring"(
                                  (
                                    (
                                      vw_jjbr_curr_exch_dim.jj_mnth_id
                                    ):: character varying
                                  ):: text, 
                                  1, 
                                  4
                                ) <= (
                                  (
                                    date_part(year,
                                      
                                        (current_timestamp() :: character varying):: date
                                      
                                    )
                                  ):: character varying
                                ):: text
                              )
                            ) 
                          GROUP BY 
                            vw_jjbr_curr_exch_dim.from_ccy, 
                            vw_jjbr_curr_exch_dim.to_ccy
                        )
                      )
                    )
                ) 
                UNION 
                SELECT 
                  '8361' :: character varying AS cmp_id, 
                  vw_jjbr_curr_exch_dim.to_ccy AS from_ccy, 
                  vw_jjbr_curr_exch_dim.from_ccy AS to_ccy, 
                  vw_jjbr_curr_exch_dim.jj_mnth_id, 
                  (
                    (
                      (1):: numeric
                    ):: numeric(18, 0) / vw_jjbr_curr_exch_dim.exch_rate
                  ) AS exch_rate 
                FROM 
                  vw_jjbr_curr_exch_dim 
                WHERE 
                  (
                    (
                      (
                        (vw_jjbr_curr_exch_dim.from_ccy):: text = ('NZD' :: character varying):: text
                      ) 
                      AND (
                        (vw_jjbr_curr_exch_dim.to_ccy):: text = ('AUD' :: character varying):: text
                      )
                    ) 
                    AND (
                      vw_jjbr_curr_exch_dim.jj_mnth_id IN (
                        SELECT 
                          "max"(
                            vw_jjbr_curr_exch_dim.jj_mnth_id
                          ) AS "max" 
                        FROM 
                          vw_jjbr_curr_exch_dim 
                        WHERE 
                          (
                            (
                              (
                                (vw_jjbr_curr_exch_dim.from_ccy):: text = ('NZD' :: character varying):: text
                              ) 
                              AND (
                                (vw_jjbr_curr_exch_dim.to_ccy):: text = ('AUD' :: character varying):: text
                              )
                            ) 
                            AND (
                              "substring"(
                                (
                                  (
                                    vw_jjbr_curr_exch_dim.jj_mnth_id
                                  ):: character varying
                                ):: text, 
                                1, 
                                4
                              ) <= (
                                (
                                  date_part(year,
                                    
                                      (current_timestamp() :: character varying):: date
                                    
                                  )
                                ):: character varying
                              ):: text
                            )
                          ) 
                        GROUP BY 
                          vw_jjbr_curr_exch_dim.from_ccy, 
                          vw_jjbr_curr_exch_dim.to_ccy
                      )
                    )
                  )
              ) 
              UNION 
              SELECT 
                CASE WHEN (
                  (vw_bwar_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
                ) THEN '7470' :: character varying WHEN (
                  (vw_bwar_curr_exch_dim.from_ccy):: text = ('NZD' :: character varying):: text
                ) THEN '8361' :: character varying ELSE NULL :: character varying END AS cmp_id, 
                vw_bwar_curr_exch_dim.to_ccy AS from_ccy, 
                vw_bwar_curr_exch_dim.from_ccy AS to_ccy, 
                vw_bwar_curr_exch_dim.jj_mnth_id, 
                (
                  (
                    (1):: numeric
                  ):: numeric(18, 0) / vw_bwar_curr_exch_dim.exch_rate
                ) AS exch_rate 
              FROM 
                vw_bwar_curr_exch_dim 
              WHERE 
                (
                  (
                    (vw_bwar_curr_exch_dim.to_ccy):: text = ('USD' :: character varying):: text
                  ) 
                  AND (
                    vw_bwar_curr_exch_dim.jj_mnth_id IN (
                      SELECT 
                        "max"(
                          vw_bwar_curr_exch_dim.jj_mnth_id
                        ) AS "max" 
                      FROM 
                        vw_bwar_curr_exch_dim 
                      WHERE 
                        (
                          (
                            (vw_bwar_curr_exch_dim.to_ccy):: text = ('USD' :: character varying):: text
                          ) 
                          AND (
                            "substring"(
                              (
                                (
                                  vw_bwar_curr_exch_dim.jj_mnth_id
                                ):: character varying
                              ):: text, 
                              1, 
                              4
                            ) <= (
                              (
                                date_part(year,
                                  
                                    (current_timestamp() :: character varying)::date
                                  
                                )
                              ):: character varying
                            ):: text
                          )
                        ) 
                      GROUP BY 
                        vw_bwar_curr_exch_dim.from_ccy, 
                        vw_bwar_curr_exch_dim.to_ccy
                    )
                  )
                )
            ) 
            UNION 
            SELECT 
              '747A' :: character varying AS cmp_id, 
              vw_curr_exch_dim.from_ccy, 
              vw_curr_exch_dim.to_ccy, 
              (
                "substring"(
                  (
                    (vw_curr_exch_dim.valid_date):: character varying
                  ):: text, 
                  1, 
                  6
                )
              ):: numeric(18, 0) AS jj_mnth_id, 
              vw_curr_exch_dim.exch_rate 
            FROM 
              vw_curr_exch_dim 
            WHERE 
              (
                (
                  (
                    (
                      (vw_curr_exch_dim.rate_type):: text = ('BWAR' :: character varying):: text
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
                  vw_curr_exch_dim.valid_date IN (
                    SELECT 
                      "max"(vw_curr_exch_dim.valid_date) AS "max" 
                    FROM 
                      vw_curr_exch_dim 
                    WHERE 
                      (
                        (
                          (
                            (
                              (vw_curr_exch_dim.rate_type):: text = ('BWAR' :: character varying):: text
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
                          "substring"(
                            (
                              (vw_curr_exch_dim.valid_date):: character varying
                            ):: text, 
                            1, 
                            4
                          ) <= (
                            (
                              date_part(year,
                               
                                  (current_timestamp() :: character varying)::date
                                
                              )
                            ):: character varying
                          ):: text
                        )
                      ) 
                    GROUP BY 
                      vw_curr_exch_dim.rate_type, 
                      vw_curr_exch_dim.from_ccy, 
                      vw_curr_exch_dim.to_ccy
                  )
                )
              )
          ) 
          UNION 
          SELECT 
            '836A' :: character varying AS cmp_id, 
            vw_curr_exch_dim.from_ccy, 
            vw_curr_exch_dim.to_ccy, 
            (
              "substring"(
                (
                  (vw_curr_exch_dim.valid_date):: character varying
                ):: text, 
                1, 
                6
              )
            ):: numeric(18, 0) AS jj_mnth_id, 
            vw_curr_exch_dim.exch_rate 
          FROM 
            vw_curr_exch_dim 
          WHERE 
            (
              (
                (
                  (
                    (vw_curr_exch_dim.rate_type):: text = ('BWAR' :: character varying):: text
                  ) 
                  AND (
                    (vw_curr_exch_dim.from_ccy):: text = ('SGD' :: character varying):: text
                  )
                ) 
                AND (
                  (vw_curr_exch_dim.to_ccy):: text = ('NZD' :: character varying):: text
                )
              ) 
              AND (
                vw_curr_exch_dim.valid_date IN (
                  SELECT 
                    "max"(vw_curr_exch_dim.valid_date) AS "max" 
                  FROM 
                    vw_curr_exch_dim 
                  WHERE 
                    (
                      (
                        (
                          (
                            (vw_curr_exch_dim.rate_type):: text = ('BWAR' :: character varying):: text
                          ) 
                          AND (
                            (vw_curr_exch_dim.from_ccy):: text = ('SGD' :: character varying):: text
                          )
                        ) 
                        AND (
                          (vw_curr_exch_dim.to_ccy):: text = ('NZD' :: character varying):: text
                        )
                      ) 
                      AND (
                        "substring"(
                          (
                            (vw_curr_exch_dim.valid_date):: character varying
                          ):: text, 
                          1, 
                          4
                        ) <= (
                          (
                            date_part(year,
                             
                                (current_timestamp() :: character varying)::date
                              
                            )
                          ):: character varying
                        ):: text
                      )
                    ) 
                  GROUP BY 
                    vw_curr_exch_dim.rate_type, 
                    vw_curr_exch_dim.from_ccy, 
                    vw_curr_exch_dim.to_ccy
                )
              )
            )
        ) d, 
        (
          SELECT 
            derived_table1.cmp_id, 
            derived_table1.cal_month_id, 
            derived_table1.jj_month_id, 
            derived_table1.cust_no, 
            coalesce(derived_table1.matl_id,'') as matl_id, 
            derived_table1.key_measure, 
            derived_table1.ciw_category, 
            derived_table1.ciw_account_group, 
            derived_table1.sap_account, 
            derived_table1.sap_account_nm, 
            derived_table1.local_ccy, 
            sum(derived_table1.base_measure) AS base_measure, 
            sum(derived_table1.sales_qty) AS sales_qty, 
            sum(derived_table1.gts_val) AS gts_val, 
            sum(derived_table1.retrn_val) AS retrn_val, 
            sum(derived_table1.eff_val) AS eff_val, 
            sum(derived_table1.jgf_si_val) AS jgf_si_val, 
            sum(derived_table1.pmt_terms_val) AS pmt_terms_val, 
            sum(derived_table1.datains_val) AS datains_val, 
            sum(derived_table1.exp_adj_val) AS exp_adj_val, 
            sum(derived_table1.jgf_sd_val) AS jgf_sd_val, 
            sum(derived_table1.cogs_val) AS cogs_val, 
            sum(
              derived_table1.con_free_goods_val
            ) AS con_free_goods_val 
          FROM 
            (
              SELECT 
                CASE WHEN (
                  (copa.co_cd IS NULL) 
                  AND ('747A' IS NULL)
                ) THEN '7470' :: character varying WHEN (
                  (copa.co_cd IS NULL) 
                  AND ('836A' IS NULL)
                ) THEN '8361' :: character varying ELSE copa.co_cd END AS cmp_id, 
                copa.caln_day AS cal_day, 
                NULL :: character varying AS cal_month_id, 
                (
                  (
                    "substring"(
                      (
                        (copa.fisc_yr_per):: character varying
                      ):: text, 
                      1, 
                      4
                    ) || "substring"(
                      (
                        (copa.fisc_yr_per):: character varying
                      ):: text, 
                      6, 
                      2
                    )
                  )
                ):: integer AS jj_month_id, 
                copa.cust_num AS cust_no, 
                copa.matl_num AS matl_id, 
                accnt.key_measure, 
                accnt.ciw_category, 
                accnt.ciw_account_group, 
                accnt.sap_account, 
                ead.acct_nm AS sap_account_nm, 
                copa.obj_crncy_co_obj AS local_ccy, 
                copa.amt_obj_crncy AS base_measure, 
                CASE WHEN (
                  (accnt.key_measure):: text = (
                    'Gross Trade Sales' :: character varying
                  ):: text
                ) THEN copa.qty WHEN (
                  (accnt.ciw_account_group):: text = ('Returns' :: character varying):: text
                ) THEN (- copa.qty) ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS sales_qty, 
                CASE WHEN (
                  (accnt.key_measure):: text = (
                    'Gross Trade Sales' :: character varying
                  ):: text
                ) THEN copa.amt_obj_crncy ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS gts_val, 
                CASE WHEN (
                  (accnt.ciw_account_group):: text = ('Returns' :: character varying):: text
                ) THEN copa.amt_obj_crncy ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS retrn_val, 
                CASE WHEN (
                  (accnt.key_measure):: text = ('Efficiency' :: character varying):: text
                ) THEN copa.amt_obj_crncy ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS eff_val, 
                CASE WHEN (
                  (accnt.key_measure):: text = (
                    'Joint Growth Fund: Shopper Indirect' :: character varying
                  ):: text
                ) THEN copa.amt_obj_crncy ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS jgf_si_val, 
                CASE WHEN (
                  (accnt.key_measure):: text = (
                    'Payment Terms' :: character varying
                  ):: text
                ) THEN copa.amt_obj_crncy ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS pmt_terms_val, 
                CASE WHEN (
                  trim(accnt.key_measure):: text = (
                    'Data and Insights' :: character varying
                  ):: text
                ) THEN copa.amt_obj_crncy ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS datains_val, 
                CASE WHEN (
                  (accnt.key_measure):: text = (
                    'Expenses and Adjustments' :: character varying
                  ):: text
                ) THEN copa.amt_obj_crncy ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS exp_adj_val, 
                CASE WHEN (
                  (accnt.key_measure):: text = (
                    'Joint Growth Fund: Shopper Direct' :: character varying
                  ):: text
                ) THEN copa.amt_obj_crncy ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS jgf_sd_val, 
                CASE WHEN (
                  (accnt.key_measure):: text = (
                    'Cost of Goods Sold' :: character varying
                  ):: text
                ) THEN copa.amt_obj_crncy ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS cogs_val, 
                CASE WHEN (
                  (accnt.key_measure):: text = (
                    'Consumer Free Goods' :: character varying
                  ):: text
                ) THEN copa.amt_obj_crncy ELSE (
                  (0):: numeric
                ):: numeric(18, 0) END AS con_free_goods_val 
              FROM 
                edw_copa_trans_fact copa, 
                edw_ciw_accnt_lkp accnt, 
                (
                  SELECT 
                    DISTINCT edw_account_dim.acct_num, 
                    edw_account_dim.acct_nm 
                  FROM 
                    edw_account_dim 
                  WHERE 
                    (
                      (edw_account_dim.bravo_acct_l1):: text <> ('' :: character varying):: text
                    )
                ) ead, 
                (
                  SELECT 
                    DISTINCT derived_table1.sls_org, 
                    derived_table1.cmp_id 
                  FROM 
                    (
                      (
                        SELECT 
                          DISTINCT dly_sls_cust_attrb_lkp.sls_org, 
                          dly_sls_cust_attrb_lkp.cmp_id 
                        FROM 
                          dly_sls_cust_attrb_lkp 
                        UNION ALL 
                        SELECT 
                          '330A' :: character varying AS "varchar", 
                          '747A' :: character varying AS "varchar"
                      ) 
                      UNION ALL 
                      SELECT 
                        '341A' :: character varying AS "varchar", 
                        '836A' :: character varying AS "varchar"
                    ) derived_table1
                ) lkp 
              WHERE 
                (
                  (
                    (
                      (
                        (
                          (
                            ltrim(
                              (copa.acct_num):: text, 
                              ('0' :: character varying):: text
                            ) = (accnt.sap_account):: text
                          ) 
                          AND (
                            ltrim(
                              (ead.acct_num):: text, 
                              ('0' :: character varying):: text
                            ) = (accnt.sap_account):: text
                          )
                        ) 
                        AND (
                          (copa.co_cd):: text = (lkp.cmp_id):: text
                        )
                      ) 
                      AND (
                        (copa.sls_org):: text = (lkp.sls_org):: text
                      )
                    ) 
                    AND (
                      (copa.acct_hier_shrt_desc):: text <> ('NTS' :: character varying):: text
                    )
                  ) 
                  AND (
                    (copa.acct_hier_shrt_desc):: text <> ('FGC' :: character varying):: text
                  )
                )
            ) derived_table1 
          GROUP BY 
            derived_table1.cmp_id, 
            derived_table1.cal_month_id, 
            derived_table1.jj_month_id, 
            derived_table1.cust_no, 
            coalesce(derived_table1.matl_id,''), 
            derived_table1.key_measure, 
            derived_table1.ciw_category, 
            derived_table1.ciw_account_group, 
            derived_table1.sap_account, 
            derived_table1.sap_account_nm, 
            derived_table1.local_ccy
        ) derived_table2 
      WHERE 
        (
          (
            (derived_table2.local_ccy):: text = (d.from_ccy):: text
          ) 
          AND (
            (derived_table2.cmp_id):: text = (d.cmp_id):: text
          )
        )
    ) a 
    LEFT JOIN vw_sap_std_cost c ON (
      (
        (
          (c.matnr):: text = (a.matl_id):: text
        ) 
        AND (
          (c.cmp_no):: text = (a.cmp_id):: text
        )
      )
    )
  ) 
WHERE 
  (
    (
      (
        (
          (a.jj_month_id):: numeric
        ):: numeric(18, 0) = b.jj_mnth_id
      ) 
      AND (
        (a.local_ccy):: text = (b.from_ccy):: text
      )
    ) 
    AND (
      (b.to_ccy):: text = ('AUD' :: character varying):: text
    )
  )

)
select * from final