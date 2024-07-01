with edw_vw_ph_survey_details as (
select * from {{ ref('phledw_integration__edw_vw_ph_survey_details') }}
),
sdl_mds_ph_clobotics_sos_ref as (
select * from {{ source('phlsdl_raw', 'sdl_mds_ph_clobotics_sos_ref') }}
),
itg_ph_clobotics_task as (
select * from {{ ref('phlitg_integration__itg_ph_clobotics_task') }}
),
itg_ph_clobotics_store as (
select * from {{ ref('phlitg_integration__itg_ph_clobotics_store') }}
),
itg_ph_clobotics_survey as (
select * from {{ ref('phlitg_integration__itg_ph_clobotics_survey') }}
),
itg_mds_ph_ps_targets as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ps_targets') }}
),
itg_mds_ph_ps_weights as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ps_weights') }}
),
itg_mds_ph_gt_customer as (
select * from {{ ref('phlitg_integration__itg_mds_ph_gt_customer') }}
),
itg_mds_ph_product_hierarchy as (
select * from {{ ref('phlitg_integration__itg_mds_ph_product_hierarchy') }}
),
itg_ph_tbl_isebranchmaster as (
select * from {{ ref('phlitg_integration__itg_ph_tbl_isebranchmaster') }}
),
itg_mds_ph_ise_weights as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ise_weights') }}
),
edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
ph_kpi2data_mapping as (
select * from {{ source('phledw_integration','ph_kpi2data_mapping') }}
),
itg_mds_ph_ise_sos_targets as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ise_sos_targets') }}
),
itg_ph_non_ise_msl_osa as (
select * from {{ ref('phlitg_integration__itg_ph_non_ise_msl_osa') }}
),
itg_mds_ph_lav_product as (
select * from {{ ref('phlitg_integration__itg_mds_ph_lav_product') }}
),
itg_mds_ph_pos_customers as (
select * from {{ ref('phlitg_integration__itg_mds_ph_pos_customers') }}
),
itg_ph_ps_retailer_soldto_map as (
select * from {{ ref('phlitg_integration__itg_ph_ps_retailer_soldto_map') }}
),
itg_mds_ph_ise_parent as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ise_parent') }}
),
itg_mds_ph_ref_parent_customer as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ref_parent_customer') }}
),
itg_ph_tbl_surveyisequestion as (
select * from {{ ref('phlitg_integration__itg_ph_tbl_surveyisequestion') }}
),
itg_ph_tbl_surveyisehdr as (
select * from {{ ref('phlitg_integration__itg_ph_tbl_surveyisehdr') }}
),
itg_ph_tbl_surveychoices as (
select * from {{ ref('phlitg_integration__itg_ph_tbl_surveychoices') }}
),
itg_ph_non_ise_weights as (
select * from {{ ref('phlitg_integration__itg_ph_non_ise_weights') }}
),
vw_ph_clobotics_perfect_store as (
select * from {{ ref('phledw_integration__vw_ph_clobotics_perfect_store') }}
),
final as (
(
  (
    (
      SELECT 
        'Clobotics' AS dataset, 
        task.store_code AS customerid, 
        task.username AS salespersonid, 
        task.task_action_time AS mrch_resp_startdt, 
        task.plan_finish_time AS mrch_resp_enddt, 
        to_date(task.plan_finish_time) AS survey_enddate, 
        NULL AS value, 
        'TRUE' AS mustcarryitem, 
        NULL AS answerscore, 
        CASE WHEN (
          task.value = (
            (1):: numeric
          ):: numeric(10, 1)
        ) THEN 'TRUE' :: character varying ELSE NULL :: character varying END AS presence, 
        NULL AS outofstock, 
        'MSL Compliance' AS kpi, 
        to_date(task.task_action_time) AS scheduleddate, 
        task.plan_status AS vst_status, 
        time_dim."year" AS fisc_yr, 
        (time_dim.mnth_id):: character varying AS fisc_per, 
        (
          (
            (
              '(Clobotics) ' :: character varying
            ):: text || (
              CASE WHEN (
                regexp_count(
                  (task.display_username):: text, 
                  (' ' :: character varying):: text, 
                  1
                ) = 1
              ) THEN (
                split_part(
                  (task.display_username):: text, 
                  (' ' :: character varying):: text, 
                  1
                )
              ):: character varying WHEN (
                regexp_count(
                  (task.display_username):: text, 
                  (' ' :: character varying):: text, 
                  1
                ) = 2
              ) THEN (
                (
                  (
                    split_part(
                      (task.display_username):: text, 
                      (' ' :: character varying):: text, 
                      1
                    ) || (' ' :: character varying):: text
                  ) || split_part(
                    (task.display_username):: text, 
                    (' ' :: character varying):: text, 
                    2
                  )
                )
              ):: character varying ELSE NULL :: character varying END
            ):: text
          )
        ):: character varying AS firstname, 
        (
          reverse(
            split_part(
              reverse(
                (task.display_username):: text
              ), 
              (' ' :: character varying):: text, 
              1
            )
          )
        ):: character varying AS lastname, 
        COALESCE(
          store.brnch_nm, 'Not Available' :: character varying
        ) AS customername, 
        'Philippines' AS country, 
        COALESCE(
          store.sales_grp, 'Not Available' :: character varying
        ) AS storereference, 
        COALESCE(
          store.store_mtrx, 'Not Available' :: character varying
        ) AS store_type, 
        COALESCE(
          store.chnl, 'Not Available' :: character varying
        ) AS channel, 
        COALESCE(
          store.sales_grp, 'Not Available' :: character varying
        ) AS salesgroup, 
        COALESCE(
          prod_hier.franchise_nm, 'Not Available' :: character varying
        ) AS prod_hier_l3, 
        COALESCE(
          prod_hier.brand_nm, 'Not Available' :: character varying
        ) AS prod_hier_l4, 
        COALESCE(
          prod_hier.variant_nm, 'Not Available' :: character varying
        ) AS prod_hier_l5, 
        COALESCE(
          prod_hier.ph_ref_repvarputup_nm, 
          'Not Available' :: character varying
        ) AS prod_hier_l6, 
        COALESCE(
          prod_hier.franchise_nm, 'Not Available' :: character varying
        ) AS category, 
        COALESCE(
          prod_hier.brand_nm, 'Not Available' :: character varying
        ) AS segment, 
        weights.weight AS kpi_chnl_wt, 
        targets.target AS mkt_share, 
        NULL AS question_text, 
        task.task_name AS ques_desc, 
        NULL AS "y/n_flag" 
      FROM 
        (
          (
            (
              (
                (
                  (select * from itg_ph_clobotics_task) task 
                  LEFT JOIN (
                    SELECT 
                      DISTINCT (
                        upper(
                          (itg_mds_ph_pos_customers.chnl):: text
                        )
                      ):: character varying AS chnl, 
                      (
                        upper(
                          trim(
                            (
                              itg_mds_ph_pos_customers.store_mtrx
                            ):: text
                          )
                        )
                      ):: character varying AS store_mtrx, 
                      (
                        upper(
                          (
                            itg_mds_ph_pos_customers.sales_grp
                          ):: text
                        )
                      ):: character varying AS sales_grp, 
                      (
                        upper(
                          (itg_mds_ph_pos_customers.code):: text
                        )
                      ):: character varying AS code, 
                      (
                        (
                          (
                            upper(
                              (itg_mds_ph_pos_customers.code):: text
                            ) || (' | ' :: character varying):: text
                          ) || upper(
                            (
                              itg_mds_ph_pos_customers.brnch_nm
                            ):: text
                          )
                        )
                      ):: character varying AS brnch_nm 
                    FROM 
                      itg_mds_ph_pos_customers 
                    WHERE 
                      (
                        (
                          itg_mds_ph_pos_customers.active
                        ):: text = ('Y' :: character varying):: text
                      ) 
                    UNION ALL 
                    SELECT 
                      DISTINCT 'GENERAL TRADE' :: character varying AS chnl, 
                      (
                        upper(
                          trim(
                            (
                              itg_mds_ph_gt_customer.rpt_grp11_desc
                            ):: text
                          )
                        )
                      ):: character varying AS store_mtrx, 
                      (
                        upper(
                          (
                            itg_mds_ph_gt_customer.dstrbtr_grp_nm
                          ):: text
                        )
                      ):: character varying AS sales_grp, 
                      (
                        upper(
                          (
                            itg_mds_ph_gt_customer.dstrbtr_cust_id
                          ):: text
                        )
                      ):: character varying AS code, 
                      (
                        (
                          (
                            upper(
                              ltrim(
                                (
                                  itg_mds_ph_gt_customer.dstrbtr_cust_id
                                ):: text, 
                                (
                                  (0):: character varying
                                ):: text
                              )
                            ) || (' | ' :: character varying):: text
                          ) || upper(
                            (
                              itg_mds_ph_gt_customer.dstrbtr_cust_nm
                            ):: text
                          )
                        )
                      ):: character varying AS brnch_nm 
                    FROM 
                      itg_mds_ph_gt_customer 
                    WHERE 
                      (
                        (itg_mds_ph_gt_customer.active):: text = ('Y' :: character varying):: text
                      )
                  ) store ON (
                    (
                      upper(
                        trim(
                          (task.store_code):: text
                        )
                      ) = upper(
                        (store.code):: text
                      )
                    )
                  )
                ) 
                LEFT JOIN (
                  SELECT 
                    DISTINCT edw_vw_os_time_dim."year", 
                    edw_vw_os_time_dim.mnth_id, 
                    edw_vw_os_time_dim.cal_date 
                  FROM 
                    edw_vw_os_time_dim
                ) time_dim ON (
                  (
                    to_date(task.create_time) = time_dim.cal_date
                  )
                )
              ) 
              LEFT JOIN (
                SELECT 
                  DISTINCT a.item_cd, 
                  a.scard_put_up_cd, 
                  b.franchise_nm, 
                  b.brand_nm, 
                  b.variant_nm, 
                  b.ph_ref_repvarputup_nm 
                FROM 
                  (
                    itg_mds_ph_lav_product a 
                    LEFT JOIN (
                      SELECT 
                        DISTINCT itg_mds_ph_product_hierarchy.ph_ref_repvarputup_cd, 
                        itg_mds_ph_product_hierarchy.franchise_nm, 
                        itg_mds_ph_product_hierarchy.brand_nm, 
                        itg_mds_ph_product_hierarchy.variant_nm, 
                        itg_mds_ph_product_hierarchy.ph_ref_repvarputup_nm 
                      FROM 
                        itg_mds_ph_product_hierarchy
                    ) b ON (
                      (
                        (b.ph_ref_repvarputup_cd):: text = (a.scard_put_up_cd):: text
                      )
                    )
                  )
              ) prod_hier ON (
                (
                  (prod_hier.item_cd):: text = split_part(
                    (task.sku_id):: text, 
                    ('.' :: character varying):: text, 
                    1
                  )
                )
              )
            ) 
            JOIN itg_mds_ph_ps_weights weights ON (
              (
                (
                  (
                    (
                      upper(
                        (weights.kpi):: text
                      ) = (
                        'MSL COMPLIANCE' :: character varying
                      ):: text
                    ) 
                    AND (
                      upper(
                        (weights.retail_env):: text
                      ) = upper(
                        (store.store_mtrx):: text
                      )
                    )
                  ) 
                  AND (
                    weights.valid_from <= to_date(task.create_time)
                  )
                ) 
                AND (
                  weights.valid_to >= to_date(task.create_time)
                )
              )
            )
          ) 
          LEFT JOIN itg_mds_ph_ps_targets targets ON (
            (
              (
                upper(
                  (targets.kpi):: text
                ) = (
                  'MSL COMPLIANCE' :: character varying
                ):: text
              ) 
              AND (
                upper(
                  (targets.retail_env):: text
                ) = upper(
                  (store.store_mtrx):: text
                )
              )
            )
          )
        ) 
      WHERE 
        (
          (
            (
              (task.kpi):: text = ('MSL' :: character varying):: text
            ) 
            AND (
              upper(
                (task.manufacturer):: text
              ) = (
                'JOHNSON & JOHNSON' :: character varying
              ):: text
            )
          ) 
          AND (
            upper(
              (task.plan_status):: text
            ) = ('COMPLETED' :: character varying):: text
          )
        ) 
      UNION ALL 
      SELECT 
        'Clobotics' AS dataset, 
        store_trans.store_code AS customerid, 
        store_trans.username AS salespersonid, 
        store_trans.plan_start_time AS mrch_resp_startdt, 
        store_trans.plan_finish_time AS mrch_resp_enddt, 
        to_date(store_trans.plan_finish_time) AS survey_enddate, 
        NULL AS value, 
        'TRUE' AS mustcarryitem, 
        NULL AS answerscore, 
        'TRUE' AS presence, 
        CASE WHEN (
          store_trans.value = (
            (0):: numeric
          ):: numeric(18, 0)
        ) THEN 'TRUE' :: character varying ELSE NULL :: character varying END AS outofstock, 
        'OOS Compliance' AS kpi, 
        to_date(store_trans.plan_start_time) AS scheduleddate, 
        store_trans.plan_status AS vst_status, 
        time_dim."year" AS fisc_yr, 
        (time_dim.mnth_id):: character varying AS fisc_per, 
        (
          (
            (
              '(Clobotics) ' :: character varying
            ):: text || (
              CASE WHEN (
                regexp_count(
                  (store_trans.display_username):: text, 
                  (' ' :: character varying):: text, 
                  1
                ) = 1
              ) THEN (
                split_part(
                  (store_trans.display_username):: text, 
                  (' ' :: character varying):: text, 
                  1
                )
              ):: character varying WHEN (
                regexp_count(
                  (store_trans.display_username):: text, 
                  (' ' :: character varying):: text, 
                  1
                ) = 2
              ) THEN (
                (
                  (
                    split_part(
                      (store_trans.display_username):: text, 
                      (' ' :: character varying):: text, 
                      1
                    ) || (' ' :: character varying):: text
                  ) || split_part(
                    (store_trans.display_username):: text, 
                    (' ' :: character varying):: text, 
                    2
                  )
                )
              ):: character varying ELSE NULL :: character varying END
            ):: text
          )
        ):: character varying AS firstname, 
        (
          reverse(
            split_part(
              reverse(
                (store_trans.display_username):: text
              ), 
              (' ' :: character varying):: text, 
              1
            )
          )
        ):: character varying AS lastname, 
        COALESCE(
          store.brnch_nm, 'Not Available' :: character varying
        ) AS customername, 
        'Philippines' AS country, 
        COALESCE(
          store.sales_grp, 'Not Available' :: character varying
        ) AS storereference, 
        COALESCE(
          store.store_mtrx, 'Not Available' :: character varying
        ) AS store_type, 
        COALESCE(
          store.chnl, 'Not Available' :: character varying
        ) AS channel, 
        COALESCE(
          store.sales_grp, 'Not Available' :: character varying
        ) AS salesgroup, 
        COALESCE(
          prod_hier.franchise_nm, 'Not Available' :: character varying
        ) AS prod_hier_l3, 
        COALESCE(
          prod_hier.brand_nm, 'Not Available' :: character varying
        ) AS prod_hier_l4, 
        COALESCE(
          prod_hier.variant_nm, 'Not Available' :: character varying
        ) AS prod_hier_l5, 
        COALESCE(
          prod_hier.ph_ref_repvarputup_nm, 
          'Not Available' :: character varying
        ) AS prod_hier_l6, 
        COALESCE(
          prod_hier.franchise_nm, 'Not Available' :: character varying
        ) AS category, 
        COALESCE(
          prod_hier.brand_nm, 'Not Available' :: character varying
        ) AS segment, 
        weights.weight AS kpi_chnl_wt, 
        targets.target AS mkt_share, 
        NULL AS question_text, 
        NULL AS ques_desc, 
        NULL AS "y/n_flag" 
      FROM 
        (
          (
            (
              (
                (
                  itg_ph_clobotics_store store_trans 
                  LEFT JOIN (
                    SELECT 
                      DISTINCT (
                        upper(
                          (itg_mds_ph_pos_customers.chnl):: text
                        )
                      ):: character varying AS chnl, 
                      (
                        upper(
                          trim(
                            (
                              itg_mds_ph_pos_customers.store_mtrx
                            ):: text
                          )
                        )
                      ):: character varying AS store_mtrx, 
                      (
                        upper(
                          (
                            itg_mds_ph_pos_customers.sales_grp
                          ):: text
                        )
                      ):: character varying AS sales_grp, 
                      (
                        upper(
                          (itg_mds_ph_pos_customers.code):: text
                        )
                      ):: character varying AS code, 
                      (
                        (
                          (
                            upper(
                              (itg_mds_ph_pos_customers.code):: text
                            ) || (' | ' :: character varying):: text
                          ) || upper(
                            (
                              itg_mds_ph_pos_customers.brnch_nm
                            ):: text
                          )
                        )
                      ):: character varying AS brnch_nm 
                    FROM 
                      itg_mds_ph_pos_customers 
                    WHERE 
                      (
                        (
                          itg_mds_ph_pos_customers.active
                        ):: text = ('Y' :: character varying):: text
                      ) 
                    UNION ALL 
                    SELECT 
                      DISTINCT 'GENERAL TRADE' :: character varying AS chnl, 
                      (
                        upper(
                          trim(
                            (
                              itg_mds_ph_gt_customer.rpt_grp11_desc
                            ):: text
                          )
                        )
                      ):: character varying AS store_mtrx, 
                      (
                        upper(
                          (
                            itg_mds_ph_gt_customer.dstrbtr_grp_nm
                          ):: text
                        )
                      ):: character varying AS sales_grp, 
                      (
                        upper(
                          (
                            itg_mds_ph_gt_customer.dstrbtr_cust_id
                          ):: text
                        )
                      ):: character varying AS code, 
                      (
                        (
                          (
                            upper(
                              ltrim(
                                (
                                  itg_mds_ph_gt_customer.dstrbtr_cust_id
                                ):: text, 
                                (
                                  (0):: character varying
                                ):: text
                              )
                            ) || (' | ' :: character varying):: text
                          ) || upper(
                            (
                              itg_mds_ph_gt_customer.dstrbtr_cust_nm
                            ):: text
                          )
                        )
                      ):: character varying AS brnch_nm 
                    FROM 
                      itg_mds_ph_gt_customer 
                    WHERE 
                      (
                        (itg_mds_ph_gt_customer.active):: text = ('Y' :: character varying):: text
                      )
                  ) store ON (
                    (
                      upper(
                        trim(
                          (store_trans.store_code):: text
                        )
                      ) = upper(
                        (store.code):: text
                      )
                    )
                  )
                ) 
                LEFT JOIN (
                  SELECT 
                    DISTINCT edw_vw_os_time_dim."year", 
                    edw_vw_os_time_dim.mnth_id, 
                    edw_vw_os_time_dim.cal_date 
                  FROM 
                    edw_vw_os_time_dim
                ) time_dim ON (
                  (
                    to_date(store_trans.plan_upload_time) = time_dim.cal_date
                  )
                )
              ) 
              LEFT JOIN (
                SELECT 
                  DISTINCT a.item_cd, 
                  a.scard_put_up_cd, 
                  b.franchise_nm, 
                  b.brand_nm, 
                  b.variant_nm, 
                  b.ph_ref_repvarputup_nm 
                FROM 
                  (
                    itg_mds_ph_lav_product a 
                    LEFT JOIN (
                      SELECT 
                        DISTINCT itg_mds_ph_product_hierarchy.ph_ref_repvarputup_cd, 
                        itg_mds_ph_product_hierarchy.franchise_nm, 
                        itg_mds_ph_product_hierarchy.brand_nm, 
                        itg_mds_ph_product_hierarchy.variant_nm, 
                        itg_mds_ph_product_hierarchy.ph_ref_repvarputup_nm 
                      FROM 
                        itg_mds_ph_product_hierarchy
                    ) b ON (
                      (
                        (b.ph_ref_repvarputup_cd):: text = (a.scard_put_up_cd):: text
                      )
                    )
                  )
              ) prod_hier ON (
                (
                  (prod_hier.item_cd):: text = split_part(
                    (store_trans.sku_id):: text, 
                    ('.' :: character varying):: text, 
                    1
                  )
                )
              )
            ) 
            JOIN itg_mds_ph_ps_weights weights ON (
              (
                (
                  (
                    (
                      upper(
                        (weights.kpi):: text
                      ) = (
                        'OSA COMPLIANCE' :: character varying
                      ):: text
                    ) 
                    AND (
                      upper(
                        (weights.retail_env):: text
                      ) = upper(
                        (store.store_mtrx):: text
                      )
                    )
                  ) 
                  AND (
                    weights.valid_from <= to_date(store_trans.plan_upload_time)
                  )
                ) 
                AND (
                  weights.valid_to >= to_date(store_trans.plan_upload_time)
                )
              )
            )
          ) 
          LEFT JOIN itg_mds_ph_ps_targets targets ON (
            (
              (
                upper(
                  (targets.kpi):: text
                ) = (
                  'OSA COMPLIANCE' :: character varying
                ):: text
              ) 
              AND (
                upper(
                  (targets.retail_env):: text
                ) = upper(
                  (store.store_mtrx):: text
                )
              )
            )
          )
        ) 
      WHERE 
        (
          (
            upper(
              (store_trans.manufacturer):: text
            ) = (
              'JOHNSON & JOHNSON' :: character varying
            ):: text
          ) 
          AND (
            upper(
              (store_trans.plan_status):: text
            ) = ('COMPLETED' :: character varying):: text
          )
        )
    ) 
    UNION ALL 
    SELECT 
      'Clobotics' AS dataset, 
      survey.store_code AS customerid, 
      survey.username AS salespersonid, 
      survey.task_action_time AS mrch_resp_startdt, 
      survey.plan_finish_time AS mrch_resp_enddt, 
      to_date(survey.plan_finish_time) AS survey_enddate, 
      NULL AS value, 
      NULL AS mustcarryitem, 
      NULL AS answerscore, 
      NULL AS presence, 
      NULL AS outofstock, 
      (
        (
          split_part(
            (survey.task_name):: text, 
            (' ' :: character varying):: text, 
            1
          ) || (
            ' Compliance' :: character varying
          ):: text
        )
      ):: character varying AS kpi, 
      to_date(survey.task_action_time) AS scheduleddate, 
      survey.plan_status AS vst_status, 
      time_dim."year" AS fisc_yr, 
      (time_dim.mnth_id):: character varying AS fisc_per, 
      (
        (
          (
            '(Clobotics) ' :: character varying
          ):: text || (
            CASE WHEN (
              regexp_count(
                (survey.user_display_name):: text, 
                (' ' :: character varying):: text, 
                1
              ) = 1
            ) THEN (
              split_part(
                (survey.user_display_name):: text, 
                (' ' :: character varying):: text, 
                1
              )
            ):: character varying WHEN (
              regexp_count(
                (survey.user_display_name):: text, 
                (' ' :: character varying):: text, 
                1
              ) = 2
            ) THEN (
              (
                (
                  split_part(
                    (survey.user_display_name):: text, 
                    (' ' :: character varying):: text, 
                    1
                  ) || (' ' :: character varying):: text
                ) || split_part(
                  (survey.user_display_name):: text, 
                  (' ' :: character varying):: text, 
                  2
                )
              )
            ):: character varying ELSE NULL :: character varying END
          ):: text
        )
      ):: character varying AS firstname, 
      (
        reverse(
          split_part(
            reverse(
              (survey.user_display_name):: text
            ), 
            (' ' :: character varying):: text, 
            1
          )
        )
      ):: character varying AS lastname, 
      COALESCE(
        store.brnch_nm, 'Not Available' :: character varying
      ) AS customername, 
      'Philippines' AS country, 
      COALESCE(
        store.sales_grp, 'Not Available' :: character varying
      ) AS storereference, 
      COALESCE(
        store.store_mtrx, 'Not Available' :: character varying
      ) AS store_type, 
      COALESCE(
        store.chnl, 'Not Available' :: character varying
      ) AS channel, 
      COALESCE(
        store.sales_grp, 'Not Available' :: character varying
      ) AS salesgroup, 
      'STORE LEVEL' AS prod_hier_l3, 
      'STORE LEVEL' AS prod_hier_l4, 
      NULL AS prod_hier_l5, 
      NULL AS prod_hier_l6, 
      'STORE LEVEL' AS category, 
      'STORE LEVEL' AS segment, 
      weights.weight AS kpi_chnl_wt, 
      targets.target AS mkt_share, 
      survey.question_content AS question_text, 
      survey.qn_name AS ques_desc, 
      (
        upper(
          (survey.question_answer_value):: text
        )
      ):: character varying AS "y/n_flag" 
    FROM 
      (
        (
          (
            (
              itg_ph_clobotics_survey survey 
              LEFT JOIN (
                SELECT 
                  DISTINCT (
                    upper(
                      (itg_mds_ph_pos_customers.chnl):: text
                    )
                  ):: character varying AS chnl, 
                  (
                    upper(
                      trim(
                        (
                          itg_mds_ph_pos_customers.store_mtrx
                        ):: text
                      )
                    )
                  ):: character varying AS store_mtrx, 
                  (
                    upper(
                      (
                        itg_mds_ph_pos_customers.sales_grp
                      ):: text
                    )
                  ):: character varying AS sales_grp, 
                  (
                    upper(
                      (itg_mds_ph_pos_customers.code):: text
                    )
                  ):: character varying AS code, 
                  (
                    (
                      (
                        upper(
                          (itg_mds_ph_pos_customers.code):: text
                        ) || (' | ' :: character varying):: text
                      ) || upper(
                        (
                          itg_mds_ph_pos_customers.brnch_nm
                        ):: text
                      )
                    )
                  ):: character varying AS brnch_nm 
                FROM 
                  itg_mds_ph_pos_customers 
                WHERE 
                  (
                    (
                      itg_mds_ph_pos_customers.active
                    ):: text = ('Y' :: character varying):: text
                  ) 
                UNION ALL 
                SELECT 
                  DISTINCT 'GENERAL TRADE' :: character varying AS chnl, 
                  (
                    upper(
                      trim(
                        (
                          itg_mds_ph_gt_customer.rpt_grp11_desc
                        ):: text
                      )
                    )
                  ):: character varying AS store_mtrx, 
                  (
                    upper(
                      (
                        itg_mds_ph_gt_customer.dstrbtr_grp_nm
                      ):: text
                    )
                  ):: character varying AS sales_grp, 
                  (
                    upper(
                      (
                        itg_mds_ph_gt_customer.dstrbtr_cust_id
                      ):: text
                    )
                  ):: character varying AS code, 
                  (
                    (
                      (
                        upper(
                          ltrim(
                            (
                              itg_mds_ph_gt_customer.dstrbtr_cust_id
                            ):: text, 
                            (
                              (0):: character varying
                            ):: text
                          )
                        ) || (' | ' :: character varying):: text
                      ) || upper(
                        (
                          itg_mds_ph_gt_customer.dstrbtr_cust_nm
                        ):: text
                      )
                    )
                  ):: character varying AS brnch_nm 
                FROM 
                  itg_mds_ph_gt_customer 
                WHERE 
                  (
                    (itg_mds_ph_gt_customer.active):: text = ('Y' :: character varying):: text
                  )
              ) store ON (
                (
                  upper(
                    trim(
                      (survey.store_code):: text
                    )
                  ) = upper(
                    (store.code):: text
                  )
                )
              )
            ) 
            LEFT JOIN (
              SELECT 
                DISTINCT edw_vw_os_time_dim."year", 
                edw_vw_os_time_dim.mnth_id, 
                edw_vw_os_time_dim.cal_date 
              FROM 
                edw_vw_os_time_dim
            ) time_dim ON (
              (
                to_date(survey.create_time) = time_dim.cal_date
              )
            )
          ) 
          JOIN itg_mds_ph_ps_weights weights ON (
            (
              (
                (
                  (
                    upper(
                      (weights.kpi):: text
                    ) = upper(
                      (
                        split_part(
                          (survey.task_name):: text, 
                          (' ' :: character varying):: text, 
                          1
                        ) || (
                          ' Compliance' :: character varying
                        ):: text
                      )
                    )
                  ) 
                  AND (
                    upper(
                      (weights.retail_env):: text
                    ) = upper(
                      (store.store_mtrx):: text
                    )
                  )
                ) 
                AND (
                  weights.valid_from <= to_date(survey.create_time)
                )
              ) 
              AND (
                weights.valid_to >= to_date(survey.create_time)
              )
            )
          )
        ) 
        LEFT JOIN itg_mds_ph_ps_targets targets ON (
          (
            (
              upper(
                (targets.kpi):: text
              ) = upper(
                (
                  split_part(
                    (survey.task_name):: text, 
                    (' ' :: character varying):: text, 
                    1
                  ) || (
                    ' Compliance' :: character varying
                  ):: text
                )
              )
            ) 
            AND (
              upper(
                (targets.retail_env):: text
              ) = upper(
                (store.store_mtrx):: text
              )
            )
          )
        )
      ) 
    WHERE 
      (
        (
          upper(
            (survey.plan_status):: text
          ) = ('COMPLETED' :: character varying):: text
        ) 
        AND (
          (
            (
              (
                upper(
                  trim(
                    (survey.task_name):: text
                  )
                ) = (
                  'PLANOGRAM SURVEY' :: character varying
                ):: text
              ) 
              OR (
                upper(
                  trim(
                    (survey.task_name):: text
                  )
                ) = (
                  'PROMO SURVEY' :: character varying
                ):: text
              )
            ) 
            OR (
              upper(
                trim(
                  (survey.task_name):: text
                )
              ) = ('PLANOGRAM' :: character varying):: text
            )
          ) 
          OR (
            upper(
              trim(
                (survey.task_name):: text
              )
            ) = ('PROMO' :: character varying):: text
          )
        )
      )
  ) 
  UNION ALL 
  SELECT 
    'Clobotics' AS dataset, 
    task.store_code AS customerid, 
    task.username AS salespersonid, 
    task.task_action_time AS mrch_resp_startdt, 
    task.plan_finish_time AS mrch_resp_enddt, 
    to_date(task.plan_finish_time) AS survey_enddate, 
    task.value, 
    NULL AS mustcarryitem, 
    NULL AS answerscore, 
    NULL AS presence, 
    NULL AS outofstock, 
    'Share of Shelf' AS kpi, 
    to_date(task.task_action_time) AS scheduleddate, 
    task.plan_status AS vst_status, 
    time_dim."year" AS fisc_yr, 
    (time_dim.mnth_id):: character varying AS fisc_per, 
    (
      (
        (
          '(Clobotics) ' :: character varying
        ):: text || (
          CASE WHEN (
            regexp_count(
              (task.display_username):: text, 
              (' ' :: character varying):: text, 
              1
            ) = 1
          ) THEN (
            split_part(
              (task.display_username):: text, 
              (' ' :: character varying):: text, 
              1
            )
          ):: character varying WHEN (
            regexp_count(
              (task.display_username):: text, 
              (' ' :: character varying):: text, 
              1
            ) = 2
          ) THEN (
            (
              (
                split_part(
                  (task.display_username):: text, 
                  (' ' :: character varying):: text, 
                  1
                ) || (' ' :: character varying):: text
              ) || split_part(
                (task.display_username):: text, 
                (' ' :: character varying):: text, 
                2
              )
            )
          ):: character varying ELSE NULL :: character varying END
        ):: text
      )
    ):: character varying AS firstname, 
    (
      reverse(
        split_part(
          reverse(
            (task.display_username):: text
          ), 
          (' ' :: character varying):: text, 
          1
        )
      )
    ):: character varying AS lastname, 
    COALESCE(
      store.brnch_nm, 'Not Available' :: character varying
    ) AS customername, 
    'Philippines' AS country, 
    COALESCE(
      store.sales_grp, 'Not Available' :: character varying
    ) AS storereference, 
    COALESCE(
      store.store_mtrx, 'Not Available' :: character varying
    ) AS store_type, 
    COALESCE(
      store.chnl, 'Not Available' :: character varying
    ) AS channel, 
    COALESCE(
      store.sales_grp, 'Not Available' :: character varying
    ) AS salesgroup, 
    task.category AS prod_hier_l3, 
    task.category AS prod_hier_l4, 
    task.category AS prod_hier_l5, 
    NULL AS prod_hier_l6, 
    task.task_name AS category, 
    CASE WHEN (
      (
        upper(
          (task.task_name):: text
        ) like ('BABY BATH%' :: character varying):: text
      ) 
      AND (
        upper(
          (task.category):: text
        ) = (
          'BABY/SKIN HEALTH' :: character varying
        ):: text
      )
    ) THEN 'Baby' :: character varying ELSE task.category END AS segment, 
    weights.weight AS kpi_chnl_wt, 
    targets.target AS mkt_share, 
    NULL AS question_text, 
    'NUMERATOR' AS ques_desc, 
    NULL AS "y/n_flag" 
  FROM 
    (
      (
        (
          (
            (
              SELECT 
                task_1.plan_status, 
                task_1.plan_finish_time, 
                task_1.create_time, 
                task_1.username, 
                task_1.display_username, 
                task_1.store_code, 
                task_1.store_name, 
                task_1.task_name, 
                task_1.task_action_time, 
                task_1.manufacturer, 
                task_1.category, 
                sum(task_1.value) AS value 
              FROM 
                (
                  (
                    SELECT 
                      task.plan_status, 
                      task.plan_finish_time, 
                      task.create_time, 
                      task.username, 
                      task.display_username, 
                      task.store_code, 
                      task.store_name, 
                      task.task_name, 
                      task.task_action_time, 
                      task.manufacturer, 
                      task.category, 
                      task.sub_category, 
                      task.brand, 
                      sum(task.value) AS value 
                    FROM 
                      itg_ph_clobotics_task task 
                    WHERE 
                      (
                        (
                          (
                            upper(
                              (task.kpi):: text
                            ) = ('FACING' :: character varying):: text
                          ) 
                          AND (
                            upper(
                              (task.manufacturer):: text
                            ) = (
                              'JOHNSON & JOHNSON' :: character varying
                            ):: text
                          )
                        ) 
                        AND (
                          upper(
                            (task.plan_status):: text
                          ) = ('COMPLETED' :: character varying):: text
                        )
                      ) 
                    GROUP BY 
                      task.plan_status, 
                      task.plan_finish_time, 
                      task.create_time, 
                      task.username, 
                      task.display_username, 
                      task.store_code, 
                      task.store_name, 
                      task.task_name, 
                      task.task_action_time, 
                      task.manufacturer, 
                      task.category, 
                      task.sub_category, 
                      task.brand
                  ) task_1 
                  JOIN (
                    SELECT 
                      sdl_mds_ph_clobotics_sos_ref.taskname, 
                      sdl_mds_ph_clobotics_sos_ref.manufacturer, 
                      sdl_mds_ph_clobotics_sos_ref.category, 
                      sdl_mds_ph_clobotics_sos_ref.subcategory, 
                      sdl_mds_ph_clobotics_sos_ref.brand, 
                      sdl_mds_ph_clobotics_sos_ref.includetag 
                    FROM 
                      sdl_mds_ph_clobotics_sos_ref 
                    WHERE 
                      (
                        trim(
                          upper(
                            (
                              sdl_mds_ph_clobotics_sos_ref.includetag
                            ):: text
                          )
                        ) = 'Y' :: text
                      )
                  ) sos_ref ON (
                    (
                      (
                        (
                          (
                            (
                              upper(
                                (task_1.task_name):: text
                              ) = trim(
                                upper(
                                  (sos_ref.taskname):: text
                                )
                              )
                            ) 
                            AND (
                              upper(
                                (task_1.manufacturer):: text
                              ) = trim(
                                upper(
                                  (sos_ref.manufacturer):: text
                                )
                              )
                            )
                          ) 
                          AND (
                            upper(
                              (task_1.category):: text
                            ) = trim(
                              upper(
                                (sos_ref.category):: text
                              )
                            )
                          )
                        ) 
                        AND (
                          upper(
                            (task_1.sub_category):: text
                          ) = trim(
                            upper(
                              (sos_ref.subcategory):: text
                            )
                          )
                        )
                      ) 
                      AND (
                        upper(
                          (task_1.brand):: text
                        ) = trim(
                          upper(
                            (sos_ref.brand):: text
                          )
                        )
                      )
                    )
                  )
                ) 
              GROUP BY 
                task_1.plan_status, 
                task_1.plan_finish_time, 
                task_1.create_time, 
                task_1.username, 
                task_1.display_username, 
                task_1.store_code, 
                task_1.store_name, 
                task_1.task_name, 
                task_1.task_action_time, 
                task_1.manufacturer, 
                task_1.category
            ) task 
            LEFT JOIN (
              SELECT 
                DISTINCT (
                  upper(
                    (itg_mds_ph_pos_customers.chnl):: text
                  )
                ):: character varying AS chnl, 
                (
                  upper(
                    trim(
                      (
                        itg_mds_ph_pos_customers.store_mtrx
                      ):: text
                    )
                  )
                ):: character varying AS store_mtrx, 
                (
                  upper(
                    (
                      itg_mds_ph_pos_customers.sales_grp
                    ):: text
                  )
                ):: character varying AS sales_grp, 
                (
                  upper(
                    (itg_mds_ph_pos_customers.code):: text
                  )
                ):: character varying AS code, 
                (
                  (
                    (
                      upper(
                        (itg_mds_ph_pos_customers.code):: text
                      ) || (' | ' :: character varying):: text
                    ) || upper(
                      (
                        itg_mds_ph_pos_customers.brnch_nm
                      ):: text
                    )
                  )
                ):: character varying AS brnch_nm 
              FROM 
                itg_mds_ph_pos_customers 
              WHERE 
                (
                  (
                    itg_mds_ph_pos_customers.active
                  ):: text = ('Y' :: character varying):: text
                ) 
              UNION ALL 
              SELECT 
                DISTINCT 'GENERAL TRADE' :: character varying AS chnl, 
                (
                  upper(
                    trim(
                      (
                        itg_mds_ph_gt_customer.rpt_grp11_desc
                      ):: text
                    )
                  )
                ):: character varying AS store_mtrx, 
                (
                  upper(
                    (
                      itg_mds_ph_gt_customer.dstrbtr_grp_nm
                    ):: text
                  )
                ):: character varying AS sales_grp, 
                (
                  upper(
                    (
                      itg_mds_ph_gt_customer.dstrbtr_cust_id
                    ):: text
                  )
                ):: character varying AS code, 
                (
                  (
                    (
                      upper(
                        ltrim(
                          (
                            itg_mds_ph_gt_customer.dstrbtr_cust_id
                          ):: text, 
                          (
                            (0):: character varying
                          ):: text
                        )
                      ) || (' | ' :: character varying):: text
                    ) || upper(
                      (
                        itg_mds_ph_gt_customer.dstrbtr_cust_nm
                      ):: text
                    )
                  )
                ):: character varying AS brnch_nm 
              FROM 
                itg_mds_ph_gt_customer 
              WHERE 
                (
                  (itg_mds_ph_gt_customer.active):: text = ('Y' :: character varying):: text
                )
            ) store ON (
              (
                upper(
                  trim(
                    (task.store_code):: text
                  )
                ) = upper(
                  (store.code):: text
                )
              )
            )
          ) 
          LEFT JOIN (
            SELECT 
              DISTINCT edw_vw_os_time_dim."year", 
              edw_vw_os_time_dim.mnth_id, 
              edw_vw_os_time_dim.cal_date 
            FROM 
              edw_vw_os_time_dim
          ) time_dim ON (
            (
              to_date(task.create_time) = time_dim.cal_date
            )
          )
        ) 
        JOIN itg_mds_ph_ps_weights weights ON (
          (
            (
              (
                (
                  upper(
                    (weights.kpi):: text
                  ) = (
                    'SOS COMPLIANCE' :: character varying
                  ):: text
                ) 
                AND (
                  upper(
                    (weights.retail_env):: text
                  ) = upper(
                    (store.store_mtrx):: text
                  )
                )
              ) 
              AND (
                weights.valid_from <= to_date(task.create_time)
              )
            ) 
            AND (
              weights.valid_to >= to_date(task.create_time)
            )
          )
        )
      ) 
      LEFT JOIN itg_mds_ph_ps_targets targets ON (
        (
          (
            upper(
              (targets.kpi):: text
            ) = (
              'SOS COMPLIANCE' :: character varying
            ):: text
          ) 
          AND (
            upper(
              (targets.attribute_2):: text
            ) = upper(
              trim(
                (task.category):: text
              )
            )
          )
        )
      )
    )
) 
UNION ALL 
SELECT 
  'Clobotics' AS dataset, 
  task.store_code AS customerid, 
  task.username AS salespersonid, 
  task.task_action_time AS mrch_resp_startdt, 
  task.plan_finish_time AS mrch_resp_enddt, 
  to_date(task.plan_finish_time) AS survey_enddate, 
  task.value, 
  NULL AS mustcarryitem, 
  NULL AS answerscore, 
  NULL AS presence, 
  NULL AS outofstock, 
  'Share of Shelf' AS kpi, 
  to_date(task.task_action_time) AS scheduleddate, 
  task.plan_status AS vst_status, 
  time_dim."year" AS fisc_yr, 
  (time_dim.mnth_id):: character varying AS fisc_per, 
  (
    (
      (
        '(Clobotics) ' :: character varying
      ):: text || (
        CASE WHEN (
          regexp_count(
            (task.display_username):: text, 
            (' ' :: character varying):: text, 
            1
          ) = 1
        ) THEN (
          split_part(
            (task.display_username):: text, 
            (' ' :: character varying):: text, 
            1
          )
        ):: character varying WHEN (
          regexp_count(
            (task.display_username):: text, 
            (' ' :: character varying):: text, 
            1
          ) = 2
        ) THEN (
          (
            (
              split_part(
                (task.display_username):: text, 
                (' ' :: character varying):: text, 
                1
              ) || (' ' :: character varying):: text
            ) || split_part(
              (task.display_username):: text, 
              (' ' :: character varying):: text, 
              2
            )
          )
        ):: character varying ELSE NULL :: character varying END
      ):: text
    )
  ):: character varying AS firstname, 
  (
    reverse(
      split_part(
        reverse(
          (task.display_username):: text
        ), 
        (' ' :: character varying):: text, 
        1
      )
    )
  ):: character varying AS lastname, 
  COALESCE(
    store.brnch_nm, 'Not Available' :: character varying
  ) AS customername, 
  'Philippines' AS country, 
  COALESCE(
    store.sales_grp, 'Not Available' :: character varying
  ) AS storereference, 
  COALESCE(
    store.store_mtrx, 'Not Available' :: character varying
  ) AS store_type, 
  COALESCE(
    store.chnl, 'Not Available' :: character varying
  ) AS channel, 
  COALESCE(
    store.sales_grp, 'Not Available' :: character varying
  ) AS salesgroup, 
  CASE WHEN (
    (
      upper(
        (task.task_name):: text
      ) like ('BABY BATH%' :: character varying):: text
    ) 
    AND (
      upper(
        (task.category):: text
      ) = (
        'BABY/SKIN HEALTH' :: character varying
      ):: text
    )
  ) THEN 'Baby' :: character varying ELSE task.category END AS prod_hier_l3, 
  CASE WHEN (
    (
      upper(
        (task.task_name):: text
      ) like ('BABY BATH%' :: character varying):: text
    ) 
    AND (
      upper(
        (task.category):: text
      ) = (
        'BABY/SKIN HEALTH' :: character varying
      ):: text
    )
  ) THEN 'Baby' :: character varying ELSE task.category END AS prod_hier_l4, 
  CASE WHEN (
    (
      upper(
        (task.task_name):: text
      ) like ('BABY BATH%' :: character varying):: text
    ) 
    AND (
      upper(
        (task.category):: text
      ) = (
        'BABY/SKIN HEALTH' :: character varying
      ):: text
    )
  ) THEN 'Baby' :: character varying ELSE task.category END AS prod_hier_l5, 
  NULL AS prod_hier_l6, 
  task.task_name AS category, 
  CASE WHEN (
    (
      upper(
        (task.task_name):: text
      ) like ('BABY BATH%' :: character varying):: text
    ) 
    AND (
      upper(
        (task.category):: text
      ) = (
        'BABY/SKIN HEALTH' :: character varying
      ):: text
    )
  ) THEN 'Baby' :: character varying ELSE task.category END AS segment, 
  weights.weight AS kpi_chnl_wt, 
  targets.target AS mkt_share, 
  NULL AS question_text, 
  'DENOMINATOR' AS ques_desc, 
  NULL AS "y/n_flag" 
FROM 
  (
    (
      (
        (
          (
            SELECT 
              task_1.plan_status, 
              task_1.plan_finish_time, 
              task_1.create_time, 
              task_1.username, 
              task_1.display_username, 
              task_1.store_code, 
              task_1.store_name, 
              task_1.task_name, 
              task_1.task_action_time, 
              task_1.manufacturer, 
              task_1.category, 
              sum(task_1.value) AS value 
            FROM 
              (
                (
                  SELECT 
                    task.plan_status, 
                    task.plan_finish_time, 
                    task.create_time, 
                    task.username, 
                    task.display_username, 
                    task.store_code, 
                    task.store_name, 
                    task.task_name, 
                    task.task_action_time, 
                    task.manufacturer, 
                    task.category, 
                    task.sub_category, 
                    task.brand, 
                    sum(task.value) AS value 
                  FROM 
                    itg_ph_clobotics_task task 
                  WHERE 
                    (
                      (
                        (
                          upper(
                            (task.kpi):: text
                          ) = ('FACING' :: character varying):: text
                        ) 
                        AND (
                          upper(
                            (task.plan_status):: text
                          ) = ('COMPLETED' :: character varying):: text
                        )
                      ) 
                      AND (task.category IS NOT NULL)
                    ) 
                  GROUP BY 
                    task.plan_status, 
                    task.plan_finish_time, 
                    task.create_time, 
                    task.username, 
                    task.display_username, 
                    task.store_code, 
                    task.store_name, 
                    task.task_name, 
                    task.task_action_time, 
                    task.manufacturer, 
                    task.category, 
                    task.sub_category, 
                    task.brand
                ) task_1 
                JOIN (
                  SELECT 
                    sdl_mds_ph_clobotics_sos_ref.taskname, 
                    sdl_mds_ph_clobotics_sos_ref.manufacturer, 
                    sdl_mds_ph_clobotics_sos_ref.category, 
                    sdl_mds_ph_clobotics_sos_ref.subcategory, 
                    sdl_mds_ph_clobotics_sos_ref.brand, 
                    sdl_mds_ph_clobotics_sos_ref.includetag 
                  FROM 
                    sdl_mds_ph_clobotics_sos_ref 
                  WHERE 
                    (
                      trim(
                        upper(
                          (
                            sdl_mds_ph_clobotics_sos_ref.includetag
                          ):: text
                        )
                      ) = 'Y' :: text
                    )
                ) sos_ref ON (
                  (
                    (
                      (
                        (
                          (
                            upper(
                              (task_1.task_name):: text
                            ) = trim(
                              upper(
                                (sos_ref.taskname):: text
                              )
                            )
                          ) 
                          AND (
                            upper(
                              (task_1.manufacturer):: text
                            ) = trim(
                              upper(
                                (sos_ref.manufacturer):: text
                              )
                            )
                          )
                        ) 
                        AND (
                          upper(
                            (task_1.category):: text
                          ) = trim(
                            upper(
                              (sos_ref.category):: text
                            )
                          )
                        )
                      ) 
                      AND (
                        upper(
                          (task_1.sub_category):: text
                        ) = trim(
                          upper(
                            (sos_ref.subcategory):: text
                          )
                        )
                      )
                    ) 
                    AND (
                      upper(
                        (task_1.brand):: text
                      ) = trim(
                        upper(
                          (sos_ref.brand):: text
                        )
                      )
                    )
                  )
                )
              ) 
            GROUP BY 
              task_1.plan_status, 
              task_1.plan_finish_time, 
              task_1.create_time, 
              task_1.username, 
              task_1.display_username, 
              task_1.store_code, 
              task_1.store_name, 
              task_1.task_name, 
              task_1.task_action_time, 
              task_1.manufacturer, 
              task_1.category
          ) task 
          LEFT JOIN (
            SELECT 
              DISTINCT (
                upper(
                  (itg_mds_ph_pos_customers.chnl):: text
                )
              ):: character varying AS chnl, 
              (
                upper(
                  trim(
                    (
                      itg_mds_ph_pos_customers.store_mtrx
                    ):: text
                  )
                )
              ):: character varying AS store_mtrx, 
              (
                upper(
                  (
                    itg_mds_ph_pos_customers.sales_grp
                  ):: text
                )
              ):: character varying AS sales_grp, 
              (
                upper(
                  (itg_mds_ph_pos_customers.code):: text
                )
              ):: character varying AS code, 
              (
                (
                  (
                    upper(
                      (itg_mds_ph_pos_customers.code):: text
                    ) || (' | ' :: character varying):: text
                  ) || upper(
                    (
                      itg_mds_ph_pos_customers.brnch_nm
                    ):: text
                  )
                )
              ):: character varying AS brnch_nm 
            FROM 
              itg_mds_ph_pos_customers 
            WHERE 
              (
                (
                  itg_mds_ph_pos_customers.active
                ):: text = ('Y' :: character varying):: text
              ) 
            UNION ALL 
            SELECT 
              DISTINCT 'GENERAL TRADE' :: character varying AS chnl, 
              (
                upper(
                  trim(
                    (
                      itg_mds_ph_gt_customer.rpt_grp11_desc
                    ):: text
                  )
                )
              ):: character varying AS store_mtrx, 
              (
                upper(
                  (
                    itg_mds_ph_gt_customer.dstrbtr_grp_nm
                  ):: text
                )
              ):: character varying AS sales_grp, 
              (
                upper(
                  (
                    itg_mds_ph_gt_customer.dstrbtr_cust_id
                  ):: text
                )
              ):: character varying AS code, 
              (
                (
                  (
                    upper(
                      ltrim(
                        (
                          itg_mds_ph_gt_customer.dstrbtr_cust_id
                        ):: text, 
                        (
                          (0):: character varying
                        ):: text
                      )
                    ) || (' | ' :: character varying):: text
                  ) || upper(
                    (
                      itg_mds_ph_gt_customer.dstrbtr_cust_nm
                    ):: text
                  )
                )
              ):: character varying AS brnch_nm 
            FROM 
              itg_mds_ph_gt_customer 
            WHERE 
              (
                (itg_mds_ph_gt_customer.active):: text = ('Y' :: character varying):: text
              )
          ) store ON (
            (
              upper(
                trim(
                  (task.store_code):: text
                )
              ) = upper(
                trim(
                  (store.code):: text
                )
              )
            )
          )
        ) 
        LEFT JOIN (
          SELECT 
            DISTINCT edw_vw_os_time_dim."year", 
            edw_vw_os_time_dim.mnth_id, 
            edw_vw_os_time_dim.cal_date 
          FROM 
            edw_vw_os_time_dim
        ) time_dim ON (
          (
            to_date(task.create_time) = time_dim.cal_date
          )
        )
      ) 
      JOIN itg_mds_ph_ps_weights weights ON (
        (
          (
            (
              (
                upper(
                  (weights.kpi):: text
                ) = (
                  'SOS COMPLIANCE' :: character varying
                ):: text
              ) 
              AND (
                upper(
                  (weights.retail_env):: text
                ) = upper(
                  (store.store_mtrx):: text
                )
              )
            ) 
            AND (
              weights.valid_from <= to_date(task.create_time)
            )
          ) 
          AND (
            weights.valid_to >= to_date(task.create_time)
          )
        )
      )
    ) 
    LEFT JOIN itg_mds_ph_ps_targets targets ON (
      (
        (
          upper(
            (targets.kpi):: text
          ) = (
            'SOS COMPLIANCE' :: character varying
          ):: text
        ) 
        AND (
          upper(
            (targets.attribute_2):: text
          ) = upper(
            trim(
              (task.category):: text
            )
          )
        )
      )
    )
  )
    )
select * from final