with edw_copa_trans_fact as (
select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_material_dim as (
select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_company_dim as (
select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_customer_attr_flat_dim as (
select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
v_intrm_crncy_exch as (
select * from {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}
),
EDW_CUSTOMER_ATTR_HIER_DIM as (
select * from {{ ref('aspedw_integration__edw_customer_attr_hier_dim') }}
),
final as (SELECT 
  x.co_cd, 
  x.co_nm, 
  x.prft_ctr, 
  (
    COALESCE(
      x.cust_hier_l2, 
      ('#' :: character varying):: text
    )
  ):: character varying(500) AS cust_hier_l2, 
  x.cust_num, 
  x.ctry_nm, 
  x.ctry_cd, 
  x.sls_grp_desc, 
  x.yr, 
  x.mo, 
  x.mega_brnd_desc, 
  x.brnd_desc, 
  x.crncy_cd, 
  x.sls_amt, 
  y.to_crncy, 
  y.ex_rt 
FROM 
  (
    (
      SELECT 
        a.co_cd, 
        d.company_nm AS co_nm, 
        a.prft_ctr, 
        a.cust_num, 
        e.cust_hier_l2, 
        d.ctry_nm, 
        d.ctry_key AS ctry_cd, 
        (h.sls_grp):: character varying(40) AS sls_grp_desc, 
        a.fisc_yr AS yr, 
        a.pstng_per AS mo, 
        c.mega_brnd_desc, 
        c.mega_brnd_desc AS brnd_desc, 
        a.obj_crncy_co_obj AS crncy_cd, 
        (
          sum(a.copa_amt) * (
            - (
              (1):: numeric
            ):: numeric(18, 0)
          )
        ) AS sls_amt 
      FROM 
        (
          (
            (
              (
                (
                  SELECT 
                    x.matl_num, 
                    x.sls_org, 
                    x.dstr_chnl, 
                    x.ctry_key, 
                    x.fisc_yr_per, 
                    x.obj_crncy_co_obj, 
                    x.caln_day, 
                    x.cust_num, 
                    x.co_cd, 
                    x.div, 
                    x.prft_ctr, 
                    x.fisc_yr, 
                    x.pstng_per, 
                    CASE WHEN (
                      (x.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
                    ) THEN (
                      (
                        - (
                          (1):: numeric
                        ):: numeric(18, 0)
                      ) * x.amt_obj_crncy
                    ) ELSE x.amt_obj_crncy END AS copa_amt 
                  FROM 
                    edw_copa_trans_fact x 
                  WHERE 
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (x.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
                                    ) 
                                    OR (
                                      (x.acct_hier_shrt_desc):: text = ('CD' :: character varying):: text
                                    )
                                  ) 
                                  OR (
                                    (x.acct_hier_shrt_desc):: text = ('LF' :: character varying):: text
                                  )
                                ) 
                                OR (
                                  (x.acct_hier_shrt_desc):: text = ('PSO' :: character varying):: text
                                )
                              ) 
                              OR (
                                (x.acct_hier_shrt_desc):: text = ('PMA' :: character varying):: text
                              )
                            ) 
                            OR (
                              (x.acct_hier_shrt_desc):: text = ('RTN' :: character varying):: text
                            )
                          ) 
                          OR (
                            (x.acct_hier_shrt_desc):: text = ('SA' :: character varying):: text
                          )
                        ) 
                        OR (
                          (x.acct_hier_shrt_desc):: text = ('TLO' :: character varying):: text
                        )
                      ) 
                      OR (
                        (x.acct_hier_shrt_desc):: text = ('VGF' :: character varying):: text
                      )
                    )
                ) a 
                LEFT JOIN edw_material_dim c ON (
                  (
                    (a.matl_num):: text = (c.matl_num):: text
                  )
                )
              ) 
              LEFT JOIN edw_company_dim d ON (
                (
                  (a.co_cd):: text = (d.co_cd):: text
                )
              )
            ) 
            LEFT JOIN (
              SELECT 
                DISTINCT edw_customer_attr_hier_dim.sold_to_party, 
                CASE WHEN (
                  (
                    edw_customer_attr_hier_dim.cntry
                  ):: text = ('Korea' :: character varying):: text
                ) THEN 'KR' :: character varying WHEN (
                  (
                    edw_customer_attr_hier_dim.cntry
                  ):: text = ('Taiwan' :: character varying):: text
                ) THEN 'TW' :: character varying ELSE 'Hong Kong' :: character varying END AS ctry_cd, 
                "max"(
                  (
                    edw_customer_attr_hier_dim.cust_hier_l2
                  ):: text
                ) AS cust_hier_l2 
              FROM 
                edw_customer_attr_hier_dim 
              WHERE 
                (
                  (
                    edw_customer_attr_hier_dim.trgt_type
                  ):: text = ('hierarchy' :: character varying):: text
                ) 
              GROUP BY 
                edw_customer_attr_hier_dim.sold_to_party, 
                CASE WHEN (
                  (
                    edw_customer_attr_hier_dim.cntry
                  ):: text = ('Korea' :: character varying):: text
                ) THEN 'KR' :: character varying WHEN (
                  (
                    edw_customer_attr_hier_dim.cntry
                  ):: text = ('Taiwan' :: character varying):: text
                ) THEN 'TW' :: character varying ELSE 'Hong Kong' :: character varying END
            ) e ON (
              (
                (
                  ltrim(
                    (e.sold_to_party):: text, 
                    (
                      (0):: character varying
                    ):: text
                  ) = ltrim(
                    (a.cust_num):: text, 
                    (
                      (0):: character varying
                    ):: text
                  )
                ) 
                AND (
                  (e.ctry_cd):: text = (a.ctry_key):: text
                )
              )
            )
          ) 
          LEFT JOIN (
            SELECT 
              DISTINCT edw_customer_attr_flat_dim.sold_to_party AS sold_to_prty, 
              edw_customer_attr_flat_dim.channel, 
              edw_customer_attr_flat_dim.sls_grp 
            FROM 
              edw_customer_attr_flat_dim 
            WHERE 
              (
                (
                  edw_customer_attr_flat_dim.trgt_type
                ):: text = ('flat' :: character varying):: text
              )
          ) h ON (
            (
              ltrim(
                (a.cust_num):: text, 
                (
                  (0):: character varying
                ):: text
              ) = ltrim(
                (h.sold_to_prty):: text, 
                (
                  (0):: character varying
                ):: text
              )
            )
          )
        ) 
      WHERE 
        (
          (
            (a.ctry_key):: text = ('KR' :: character varying):: text
          ) 
          OR (
            (a.ctry_key):: text = ('TW' :: character varying):: text
          )
        ) 
      GROUP BY 
        a.co_cd, 
        a.prft_ctr, 
        a.cust_num, 
        d.ctry_nm, 
        d.ctry_key, 
        a.fisc_yr, 
        a.pstng_per, 
        a.obj_crncy_co_obj, 
        h.sls_grp, 
        c.mega_brnd_desc, 
        e.cust_hier_l2, 
        d.company_nm
    ) x 
    LEFT JOIN v_intrm_crncy_exch y ON (
      (
        (x.crncy_cd):: text = (y.from_crncy):: text
      )
    )
  )
)
select * from final
