with edw_ims_fact as (
select * from {{ ref('ntaedw_integration__edw_ims_fact') }}
),
itg_tw_ims_dstr_prod_price_map as (
select * from {{ ref('ntaitg_integration__itg_tw_ims_dstr_prod_map') }}
),
edw_vw_store_dim as (
select * from {{ ref('ntaedw_integration__edw_vw_store_dim') }}
),
edw_vw_gt_msl_items as (
select * from {{ ref('ntaedw_integration__edw_vw_gt_msl_items') }}
),
edw_product_attr_dim as (
select * from {{ source('aspedw_integration', 'edw_product_attr_dim') }}
),
EDW_STORE_DIM as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_STORE_DIM
),
final as (
SELECT a.ims_txn_dt,
  a.ctry_cd,
  a.dstr_cd,
  "max" ((a.dstr_nm)::TEXT) AS dstr_nm,
  a.sls_rep_cd,
  a.cust_cd AS store_cd,
  a.store_class,
  a.crncy_cd,
  a.prod_cd,
  a.ean_num,
  "max" ((a.prnt_ean_num)::TEXT) AS prnt_ean_num,
  "max" ((a.msl_flg)::TEXT) AS msl_flg,
  "max" (a.store_sls_amt) AS store_sls_amt,
  sum(a.sls_amt) AS sls_amt,
  sum(a.sls_qty) AS sls_qty,
  sum(a.rtrn_qty) AS rtrn_qty,
  sum(a.rtrn_amt) AS rtrn_amt,
  "max" (a.sell_out_price_manual) AS sell_in_price_manual,
  "max" ((store.hq)::TEXT) AS hq,
  "max" ((store.store_type)::TEXT) AS store_type,
  "max" (a.unit_prc) AS sell_out_unit_price
FROM (
  (
    SELECT txn.ims_txn_dt,
      txn.dstr_cd,
      txn.dstr_nm,
      txn.cust_cd,
      txn.sls_rep_cd,
      txn.ctry_cd,
      txn.crncy_cd,
      cust.store_class,
      txn.prod_cd,
      NULL::CHARACTER VARYING AS msl_flg,
      b.store_sls_amt,
      txn.ean_num,
      NULL::CHARACTER VARYING AS prnt_ean_num,
      txn.sls_amt,
      txn.sls_qty,
      txn.rtrn_qty,
      txn.rtrn_amt,
      txn.sell_out_price_manual,
      txn.unit_prc,
      CASE 
        WHEN (
            (
              (
                (
                  ((txn.prod_cd)::TEXT like ('1U%'::CHARACTER VARYING)::TEXT)
                  OR ((txn.prod_cd)::TEXT like ('COUNTER TOP%'::CHARACTER VARYING)::TEXT)
                  )
                OR (txn.prod_cd IS NULL)
                )
              OR ((txn.prod_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
              )
            OR ((txn.prod_cd)::TEXT like ('DUMPBIN%'::CHARACTER VARYING)::TEXT)
            )
          THEN 'non sellable products'::CHARACTER VARYING
        ELSE 'sellable products'::CHARACTER VARYING
        END AS non_sellable_product
    FROM (
      (
        (
          SELECT ims_price.ims_txn_dt,
            ims_price.dstr_cd,
            ims_price.dstr_nm,
            ims_price.cust_cd,
            ims_price.sls_rep_cd,
            ims_price.ctry_cd,
            ims_price.crncy_cd,
            ims_price.prod_cd,
            ims_price.ean_num,
            ims_price.sls_amt,
            ims_price.sls_qty,
            ims_price.rtrn_qty,
            ims_price.rtrn_amt,
            ims_price.sell_out_price_manual,
            ims_price.unit_prc
          FROM (
            SELECT ims_fact.ims_txn_dt,
              ims_fact.dstr_cd,
              ims_fact.dstr_nm,
              ims_fact.cust_cd,
              ims_fact.sls_rep_cd,
              ims_fact.ctry_cd,
              ims_fact.crncy_cd,
              ims_fact.prod_cd,
              ims_fact.ean_num,
              ims_fact.sls_amt,
              ims_fact.sls_qty,
              ims_fact.rtrn_qty,
              ims_fact.rtrn_amt,
              pmf.sell_out_price_manual,
              ims_fact.unit_prc
            FROM (
              edw_ims_fact ims_fact JOIN itg_tw_ims_dstr_prod_price_map pmf ON (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                ((ims_fact.dstr_cd)::TEXT = (pmf.dstr_cd)::TEXT)
                                AND ((ims_fact.prod_cd)::TEXT = (pmf.dstr_prod_cd)::TEXT)
                                )
                              AND (ltrim((ims_fact.ean_num)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ltrim((pmf.ean_cd)::TEXT, ((0)::CHARACTER VARYING)::TEXT))
                              )
                            AND (ims_fact.ims_txn_dt >= pmf.promotion_start_date)
                            )
                          AND (ims_fact.ims_txn_dt <= pmf.promotion_end_date)
                          )
                        AND (
                          (pmf.dstr_cd IS NOT NULL)
                          OR ((pmf.dstr_cd)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                          )
                        )
                      AND (
                        (pmf.dstr_prod_cd IS NOT NULL)
                        OR ((pmf.dstr_prod_cd)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                        )
                      )
                    AND (
                      (pmf.ean_cd IS NOT NULL)
                      OR ((pmf.ean_cd)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                      )
                    )
                  )
              )
            ) ims_price
          
          UNION ALL
          
          SELECT ims_fact_2.ims_txn_dt,
            ims_fact_2.dstr_cd,
            ims_fact_2.dstr_nm,
            ims_fact_2.cust_cd,
            ims_fact_2.sls_rep_cd,
            ims_fact_2.ctry_cd,
            ims_fact_2.crncy_cd,
            ims_fact_2.prod_cd,
            ims_fact_2.ean_num,
            ims_fact_2.sls_amt,
            ims_fact_2.sls_qty,
            ims_fact_2.rtrn_qty,
            ims_fact_2.rtrn_amt,
            pm.sell_out_price_manual,
            ims_fact_2.unit_prc
          FROM (
            edw_ims_fact ims_fact_2 LEFT JOIN (
              SELECT itg_tw_ims_dstr_prod_price_map.dstr_cd,
                itg_tw_ims_dstr_prod_price_map.dstr_nm,
                itg_tw_ims_dstr_prod_price_map.ean_cd,
                itg_tw_ims_dstr_prod_price_map.dstr_prod_cd,
                itg_tw_ims_dstr_prod_price_map.dstr_prod_nm,
                itg_tw_ims_dstr_prod_price_map.sell_out_price_manual,
                itg_tw_ims_dstr_prod_price_map.promotion_start_date,
                itg_tw_ims_dstr_prod_price_map.promotion_end_date,
                itg_tw_ims_dstr_prod_price_map.crt_dttm,
                itg_tw_ims_dstr_prod_price_map.updt_dttm
              FROM itg_tw_ims_dstr_prod_price_map
              WHERE (
                  (
                    (itg_tw_ims_dstr_prod_price_map.dstr_cd IS NULL)
                    OR ((itg_tw_ims_dstr_prod_price_map.dstr_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
                    )
                  AND (
                    (itg_tw_ims_dstr_prod_price_map.dstr_prod_cd IS NULL)
                    OR ((itg_tw_ims_dstr_prod_price_map.dstr_prod_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
                    )
                  )
              ) pm ON (
                (
                  (
                    (ltrim((ims_fact_2.ean_num)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ltrim((pm.ean_cd)::TEXT, ((0)::CHARACTER VARYING)::TEXT))
                    AND (ims_fact_2.ims_txn_dt >= pm.promotion_start_date)
                    )
                  AND (ims_fact_2.ims_txn_dt <= pm.promotion_end_date)
                  )
                )
            )
          WHERE (
               (
                (((((ims_fact_2.ims_txn_dt)::CHARACTER VARYING)::TEXT || (ims_fact_2.dstr_cd)::TEXT) || (ims_fact_2.prod_cd)::TEXT) || (ims_fact_2.ean_num)::TEXT) NOT IN (
                  SELECT (((((ims_price.ims_txn_dt)::CHARACTER VARYING)::TEXT || (ims_price.dstr_cd)::TEXT) || (ims_price.prod_cd)::TEXT) || (ims_price.ean_num)::TEXT)
                  FROM (
                    SELECT ims_fact.ims_txn_dt,
                      ims_fact.dstr_cd,
                      ims_fact.dstr_nm,
                      ims_fact.cust_cd,
                      ims_fact.sls_rep_cd,
                      ims_fact.ctry_cd,
                      ims_fact.crncy_cd,
                      ims_fact.prod_cd,
                      ims_fact.ean_num,
                      ims_fact.sls_amt,
                      ims_fact.sls_qty,
                      ims_fact.rtrn_qty,
                      ims_fact.rtrn_amt,
                      pmf.sell_out_price_manual,
                      ims_fact.unit_prc
                    FROM (
                      edw_ims_fact ims_fact JOIN itg_tw_ims_dstr_prod_price_map pmf ON (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        ((ims_fact.dstr_cd)::TEXT = (pmf.dstr_cd)::TEXT)
                                        AND ((ims_fact.prod_cd)::TEXT = (pmf.dstr_prod_cd)::TEXT)
                                        )
                                      AND (ltrim((ims_fact.ean_num)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ltrim((pmf.ean_cd)::TEXT, ((0)::CHARACTER VARYING)::TEXT))
                                      )
                                    AND (ims_fact.ims_txn_dt >= pmf.promotion_start_date)
                                    )
                                  AND (ims_fact.ims_txn_dt <= pmf.promotion_end_date)
                                  )
                                AND (
                                  (pmf.dstr_cd IS NOT NULL)
                                  OR ((pmf.dstr_cd)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                                  )
                                )
                              AND (
                                (pmf.dstr_prod_cd IS NOT NULL)
                                OR ((pmf.dstr_prod_cd)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                                )
                              )
                            AND (
                              (pmf.ean_cd IS NOT NULL)
                              OR ((pmf.ean_cd)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                              )
                            )
                          )
                      )
                    ) ims_price
                  )
                )
              )
          ) txn LEFT JOIN (
          SELECT txn.ims_txn_dt,
            txn.dstr_cd,
            txn.cust_cd,
            txn.sls_rep_cd,
            txn.ctry_cd,
            txn.crncy_cd,
            sum((txn.sls_amt - txn.rtrn_amt)) AS store_sls_amt
          FROM edw_ims_fact txn
          WHERE (
              "date_part" (
                year,
                txn.ims_txn_dt
                ) > (
                "date_part" (
                  year,
                  current_timestamp()
                  ) - 3
                )
              )
          GROUP BY txn.ims_txn_dt,
            txn.dstr_cd,
            txn.cust_cd,
            txn.sls_rep_cd,
            txn.ctry_cd,
            txn.crncy_cd
          ) b ON (
            (
              (
                (
                  (
                    ((b.ctry_cd)::TEXT = (txn.ctry_cd)::TEXT)
                    AND ((b.dstr_cd)::TEXT = (txn.dstr_cd)::TEXT)
                    )
                  AND ((b.cust_cd)::TEXT = (txn.cust_cd)::TEXT)
                  )
                AND ((b.sls_rep_cd)::TEXT = (txn.sls_rep_cd)::TEXT)
                )
              AND (b.ims_txn_dt = txn.ims_txn_dt)
              )
            )
        ) LEFT JOIN edw_vw_store_dim cust ON (
          (
            (
              (
                (
                  ((cust.ctry_cd)::TEXT = (txn.ctry_cd)::TEXT)
                  AND ((cust.dstr_cd)::TEXT = (txn.dstr_cd)::TEXT)
                  )
                AND ((cust.store_cd)::TEXT = (txn.cust_cd)::TEXT)
                )
              AND (((txn.ims_txn_dt)::CHARACTER VARYING)::TEXT >= (cust.effctv_strt_dt)::TEXT)
              )
            AND (((txn.ims_txn_dt)::CHARACTER VARYING)::TEXT <= (cust.effctv_end_dt)::TEXT)
            )
          )
      )
    WHERE (
        (
          CASE 
            WHEN (
                (
                  (
                    (
                      ((txn.prod_cd)::TEXT like ('1U%'::CHARACTER VARYING)::TEXT)
                      OR ((txn.prod_cd)::TEXT like ('COUNTER TOP%'::CHARACTER VARYING)::TEXT)
                      )
                    OR (txn.prod_cd IS NULL)
                    )
                  OR ((txn.prod_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
                  )
                OR ((txn.prod_cd)::TEXT like ('DUMPBIN%'::CHARACTER VARYING)::TEXT)
                )
              THEN 'non sellable products'::CHARACTER VARYING
            ELSE 'sellable products'::CHARACTER VARYING
            END
          )::TEXT = ('sellable products'::CHARACTER VARYING)::TEXT
        )
    
    UNION ALL
    
    SELECT COALESCE(txn.ims_txn_dt, msl.msl_dt) AS "coalesce",
      COALESCE(txn.dstr_cd, msl.dstr_cd) AS "coalesce",
      NULL::CHARACTER VARYING AS dstr_nm,
      txn.cust_cd,
      txn.sls_rep_cd,
      COALESCE(txn.ctry_cd, msl.ctry_cd) AS ctry_cd,
      txn.crncy_cd,
      COALESCE(txn.store_class, msl.store_class) AS store_class,
      COALESCE(ean1.prod_cd, dim.sap_matl_num) AS sap_matl_cd,
      msl.msl_flg,
      txn.store_sls_amt,
      dim.ean AS ean_num,
      ean.ean_num AS prnt_ean_num,
      (NULL::NUMERIC)::NUMERIC(18, 0) AS sls_amt,
      NULL::INTEGER AS sls_qty,
      NULL::INTEGER AS rtrn_qty,
      (NULL::NUMERIC)::NUMERIC(18, 0) AS rtrn_amt,
      (NULL::NUMERIC)::NUMERIC(18, 0) AS sell_out_price_manual,
      (NULL::NUMERIC)::NUMERIC(18, 0) AS unit_prc,
      NULL::CHARACTER VARYING AS non_sellable_product
    FROM (
      (
        (
          (
            (
              (
                SELECT txn.ims_txn_dt,
                  txn.dstr_cd,
                  txn.cust_cd,
                  txn.sls_rep_cd,
                  txn.ctry_cd,
                  txn.crncy_cd,
                  cust.store_class,
                  sum((txn.sls_amt - txn.rtrn_amt)) AS store_sls_amt
                FROM (
                  edw_ims_fact txn LEFT JOIN edw_vw_store_dim cust ON (
                      (
                        (
                          (
                            (
                              ((cust.ctry_cd)::TEXT = (txn.ctry_cd)::TEXT)
                              AND ((cust.dstr_cd)::TEXT = (txn.dstr_cd)::TEXT)
                              )
                            AND ((cust.store_cd)::TEXT = (txn.cust_cd)::TEXT)
                            )
                          AND (((txn.ims_txn_dt)::CHARACTER VARYING)::TEXT >= (cust.effctv_strt_dt)::TEXT)
                          )
                        AND (((txn.ims_txn_dt)::CHARACTER VARYING)::TEXT <= (cust.effctv_end_dt)::TEXT)
                        )
                      )
                  )
                WHERE (
                    "date_part" (
                      year,
                      txn.ims_txn_dt
                      ) > (
                      "date_part" (
                        year,
                        current_timestamp()
                        ) - 3
                      )
                    )
                GROUP BY txn.ims_txn_dt,
                  txn.dstr_cd,
                  txn.cust_cd,
                  txn.sls_rep_cd,
                  txn.ctry_cd,
                  txn.crncy_cd,
                  cust.store_class
                ) txn LEFT JOIN edw_vw_gt_msl_items msl ON (
                  (
                    (
                      (
                        ((msl.ctry_cd)::TEXT = (txn.ctry_cd)::TEXT)
                        AND ((msl.dstr_cd)::TEXT = (txn.dstr_cd)::TEXT)
                        )
                      AND (txn.ims_txn_dt = msl.msl_dt)
                      )
                    AND ((txn.store_class)::TEXT = (msl.store_class)::TEXT)
                    )
                  )
              ) LEFT JOIN (
              SELECT DISTINCT txn.ctry_cd,
                txn.prod_cd,
                txn.ean_num
              FROM edw_ims_fact txn
              WHERE (txn.ims_txn_dt >= '2017-01-01'::DATE)
              ) ean ON (
                (
                  ((msl.sap_matl_cd)::TEXT = (ean.prod_cd)::TEXT)
                  AND ((ean.ctry_cd)::TEXT = (msl.ctry_cd)::TEXT)
                  )
                )
            ) LEFT JOIN edw_product_attr_dim prnt_dim ON (
              (
                ((prnt_dim.ean)::TEXT = (ean.ean_num)::TEXT)
                AND ((prnt_dim.cntry)::TEXT = (ean.ctry_cd)::TEXT)
                )
              )
          ) LEFT JOIN edw_product_attr_dim dim ON (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            ((prnt_dim.cntry)::TEXT = (dim.cntry)::TEXT)
                            AND ((prnt_dim.prod_hier_l1)::TEXT = (dim.prod_hier_l1)::TEXT)
                            )
                          AND ((prnt_dim.prod_hier_l2)::TEXT = (dim.prod_hier_l2)::TEXT)
                          )
                        AND ((prnt_dim.prod_hier_l3)::TEXT = (dim.prod_hier_l3)::TEXT)
                        )
                      AND ((prnt_dim.prod_hier_l4)::TEXT = (dim.prod_hier_l4)::TEXT)
                      )
                    AND ((prnt_dim.prod_hier_l5)::TEXT = (dim.prod_hier_l5)::TEXT)
                    )
                  AND ((prnt_dim.prod_hier_l6)::TEXT = (dim.prod_hier_l6)::TEXT)
                  )
                AND ((prnt_dim.prod_hier_l7)::TEXT = (dim.prod_hier_l7)::TEXT)
                )
              AND ((prnt_dim.prod_hier_l8)::TEXT = (dim.prod_hier_l8)::TEXT)
              )
            )
        ) LEFT JOIN (
        SELECT DISTINCT txn.ims_txn_dt,
          txn.ctry_cd,
          txn.prod_cd,
          txn.ean_num
        FROM edw_ims_fact txn
        WHERE (
            "date_part" (
              year,
              txn.ims_txn_dt
              ) > (
              "date_part" (
                year,
                current_timestamp()
                ) - 3
              )
            )
        ) ean1 ON (
          (
            (
              ((dim.ean)::TEXT = (ean1.ean_num)::TEXT)
              AND ((dim.cntry)::TEXT = (ean1.ctry_cd)::TEXT)
              )
            AND (msl.msl_dt = ean1.ims_txn_dt)
            )
          )
      )
    WHERE (
        (dim.sap_matl_num IS NOT NULL)
        AND ((msl.msl_flg)::TEXT = ('Y'::CHARACTER VARYING)::TEXT)
        )
    ) a LEFT JOIN (
    SELECT derived_table1.rank,
      derived_table1.ctry_cd,
      derived_table1.dstr_cd,
      derived_table1.store_cd,
      derived_table1.store_type,
      derived_table1.hq
    FROM (
      SELECT row_number() OVER (
          PARTITION BY t4.ctry_cd,
          t4.dstr_cd,
          ltrim((t4.store_cd)::TEXT, ('0'::CHARACTER VARYING)::TEXT) ORDER BY t4.store_type,
            t4.hq DESC
          ) AS rank,
        t4.ctry_cd,
        t4.dstr_cd,
        t4.store_cd,
        t4.store_type,
        t4.hq
      FROM (
        SELECT edw_store_dim.ctry_cd,
          edw_store_dim.dstr_cd,
          edw_store_dim.dstr_nm,
          edw_store_dim.store_cd,
          edw_store_dim.store_nm,
          edw_store_dim.store_class,
          edw_store_dim.dstr_cust_area,
          edw_store_dim.dstr_cust_clsn1,
          edw_store_dim.dstr_cust_clsn2,
          edw_store_dim.dstr_cust_clsn3,
          edw_store_dim.dstr_cust_clsn4,
          edw_store_dim.dstr_cust_clsn5,
          edw_store_dim.effctv_strt_dt,
          edw_store_dim.effctv_end_dt,
          edw_store_dim.distributor_address,
          edw_store_dim.distributor_telephone,
          edw_store_dim.distributor_contact,
          edw_store_dim.store_type,
          edw_store_dim.hq
        FROM edw_store_dim
        WHERE ((edw_store_dim.ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
        ) t4
      ) derived_table1
    WHERE (derived_table1.rank = 1)
    ) store ON (
      (
        (
          ((a.ctry_cd)::TEXT = (store.ctry_cd)::TEXT)
          AND ((a.dstr_cd)::TEXT = (store.dstr_cd)::TEXT)
          )
        AND (ltrim((a.cust_cd)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((store.store_cd)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
        )
      )
  )
GROUP BY a.ims_txn_dt,
  a.ctry_cd,
  a.dstr_cd,
  a.sls_rep_cd,
  a.cust_cd,
  a.store_class,
  a.crncy_cd,
  a.prod_cd,
  a.ean_num
)
select * from final