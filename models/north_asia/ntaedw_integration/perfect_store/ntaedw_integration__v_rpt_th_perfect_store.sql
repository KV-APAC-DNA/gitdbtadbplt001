with itg_jnj_consumerreach_cvs as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_JNJ_CONSUMERREACH_CVS
),
itg_jnj_consumerreach_cvs as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_JNJ_CONSUMERREACH_CVS
),
itg_jnj_consumerreach_711 as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_JNJ_CONSUMERREACH_711
),
itg_jnj_consumerreach_sfm as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_JNJ_CONSUMERREACH_SFM
),
itg_mds_th_ps_store as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_TH_PS_STORE
),
edw_product_key_attributes as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_PRODUCT_KEY_ATTRIBUTES
),
edw_vw_ps_weights as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_VW_PS_WEIGHTS
),
edw_vw_ps_targets as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_VW_PS_TARGETS
),
itg_jnj_osa_oos_report as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_JNJ_OSA_OOS_REPORT
),
itg_jnj_mer_share_of_shelf as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_JNJ_MER_SHARE_OF_SHELF
),
itg_jnj_mer_cop as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_JNJ_MER_COP
),
itg_th_lookup_question as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_LOOKUP_QUESTION
),
adecco_msl as       (     SELECT 
              'Adecco' AS dataset, 
              COALESCE(
                a.store_code, '' :: character varying
              ) AS customerid, 
              b.emp_address_pc AS salespersonid, 
              'true' AS mustcarryitem, 
              NULL AS answerscore, 
              CASE WHEN (
                (
                  (b.msl_price_tag):: character varying
                ):: text = ('1' :: character varying):: text
              ) THEN 'TRUE' :: character varying ELSE 'FALSE' :: character varying END AS presence, 
              NULL AS outofstock, 
              'MSL COMPLIANCE' AS kpi, 
              to_date(
                (
                  (
                    to_date(
                      (b.osa_oos_date):: timestamp without time zone
                    )
                  ):: character varying
                ):: text, 
                ('YYYY-MM-DD' :: character varying):: text
              ) AS scheduleddate, 
              'completed' AS vst_status, 
              (
                "substring"(
                  "replace"(
                    (
                      (
                        to_date(
                          (b.osa_oos_date):: timestamp without time zone
                        )
                      ):: character varying
                    ):: text, 
                    ('-' :: character varying):: text, 
                    ('' :: character varying):: text
                  ), 
                  0, 
                  4
                )
              ):: character varying AS fisc_yr, 
              b.yearmo AS fisc_per, 
              (
                (
                  (
                    (b.emp_address_pc):: text || (' - ' :: character varying):: text
                  ) || (b.pc_name):: text
                )
              ):: character varying AS firstname, 
              '' AS lastname, 
              (
                (
                  (
                    (
                      COALESCE(
                        a.store_code, 'NA' :: character varying
                      )
                    ):: text || (' - ' :: character varying):: text
                  ) || (
                    COALESCE(
                      a.store_name, 'NA' :: character varying
                    )
                  ):: text
                )
              ):: character varying AS customername, 
              'Thailand' AS country, 
              a.state, 
              a.customer_name AS storereference, 
              (
                upper(
                  (a.retail_environment):: text
                )
              ):: character varying AS storetype, 
              (
                upper(
                  (a.channel):: text
                )
              ):: character varying AS channel, 
              NULL AS salesgroup, 
              (
                ltrim(
                  (b.barcode):: text, 
                  (
                    (0):: character varying
                  ):: text
                )
              ):: character varying AS eannumber, 
              pka.gcph_franchise AS prod_hier_l1, 
              pka.gcph_needstate AS prod_hier_l2, 
              pka.gcph_category AS prod_hier_l3, 
              pka.gcph_brand AS prod_hier_l4, 
              pka.gcph_subcategory AS prod_hier_l5, 
              pka.gcph_subbrand AS prod_hier_l6, 
              pka.gcph_variant AS prod_hier_l7, 
              pka.put_up_desc AS prod_hier_l8, 
              (
                (
                  (
                    (b.barcode):: text || (' - ' :: character varying):: text
                  ) || (b.sku):: text
                )
              ):: character varying AS prod_hier_l9, 
              (wt.weight):: double precision AS kpi_chnl_wt, 
              NULL AS "y/n_flag", 
              NULL AS posm_execution_flag, 
              'Y' AS priority_store_flag, 
              'Is the SKU present?' AS questiontext, 
              NULL AS ques_desc, 
              NULL AS value, 
              CASE WHEN (tgt.value IS NULL) THEN (
                (1):: numeric
              ):: numeric(18, 0) ELSE tgt.value END AS mkt_share, 
              NULL AS rej_reason, 
              NULL AS photo_url 
            FROM 
              (
                (
                  (
                    (
                      (
                        SELECT 
                          trim(itg_jnj_osa_oos_report.osa_oos_date) as osa_oos_date, 
                         trim(itg_jnj_osa_oos_report.week) as week , 
                          trim(itg_jnj_osa_oos_report.emp_address_pc) as emp_address_pc, 
                          trim(itg_jnj_osa_oos_report.pc_name) as pc_name, 
                          trim(itg_jnj_osa_oos_report.emp_address_supervisor) as emp_address_supervisor, 
                          trim(itg_jnj_osa_oos_report.supervisor_name) as supervisor_name, 
                          trim(itg_jnj_osa_oos_report.area) as area, 
                          trim(itg_jnj_osa_oos_report.channel) as channel, 
                          trim(ITG_JNJ_OSA_OOS_REPORT.account) as account, 
                          trim(itg_jnj_osa_oos_report.store_id) as store_id, 
                          trim(itg_jnj_osa_oos_report.store_name) as store_name, 
                          trim(itg_jnj_osa_oos_report.shop_type) as shop_type, 
                          trim(itg_jnj_osa_oos_report.brand) as brand, 
                          trim(itg_jnj_osa_oos_report.category) as category, 
                          trim(itg_jnj_osa_oos_report.barcode) as barcode, 
                          trim(itg_jnj_osa_oos_report.sku) as sku, 
                          trim(itg_jnj_osa_oos_report.yearmo) as yearmo,
                          "max"(
                            (
                              itg_jnj_osa_oos_report.msl_price_tag
                            ):: text
                          ) AS msl_price_tag 
                        FROM 
                          itg_jnj_osa_oos_report 
                        GROUP BY 
                         trim(itg_jnj_osa_oos_report.osa_oos_date), 
                         trim(itg_jnj_osa_oos_report.week), 
                          trim(itg_jnj_osa_oos_report.emp_address_pc), 
                          trim(itg_jnj_osa_oos_report.pc_name), 
                          trim(itg_jnj_osa_oos_report.emp_address_supervisor), 
                          trim(itg_jnj_osa_oos_report.supervisor_name), 
                          trim(itg_jnj_osa_oos_report.area), 
                          trim(itg_jnj_osa_oos_report.channel), 
                          trim(ITG_JNJ_OSA_OOS_REPORT.account), 
                          trim(itg_jnj_osa_oos_report.store_id), 
                          trim(itg_jnj_osa_oos_report.store_name), 
                          trim(itg_jnj_osa_oos_report.shop_type), 
                          trim(itg_jnj_osa_oos_report.brand), 
                          trim(itg_jnj_osa_oos_report.category), 
                          trim(itg_jnj_osa_oos_report.barcode), 
                          trim(itg_jnj_osa_oos_report.sku), 
                          trim(itg_jnj_osa_oos_report.yearmo)
                      ) b 
                      LEFT JOIN (
                        SELECT DISTINCT 
                        rtrim(itg_mds_th_ps_store.dataset) as dataset,
                        rtrim(itg_mds_th_ps_store.channel) as channel,
                        rtrim(itg_mds_th_ps_store.retail_environment) as retail_environment,
                        rtrim(itg_mds_th_ps_store.state) as state,
                        rtrim(itg_mds_th_ps_store.customer_code) as customer_code,
                        rtrim(itg_mds_th_ps_store.customer_name) as customer_name,
                        rtrim(itg_mds_th_ps_store.store_code) as store_code,
                        rtrim(itg_mds_th_ps_store.store_name) as store_name
                    FROM itg_mds_th_ps_store
                    WHERE (
                            upper(
                                (itg_mds_th_ps_store.dataset)::text
                            ) = ('ADECCO'::character varying)::text
                          )
                      ) a ON (
                        (
                          upper(
                            trim(
                              (b.store_name):: text
                            )
                          ) = upper(
                            trim(
                              (a.store_name):: text
                            )
                          )
                        )
                      )
                    ) 
                    LEFT JOIN (
                      SELECT 
                        a.ctry_nm, 
                        a.ean_upc, 
                        a.gcph_franchise, 
                        a.gcph_needstate, 
                        a.gcph_category, 
                        a.gcph_brand, 
                        a.gcph_subcategory, 
                        a.gcph_subbrand, 
                        a.gcph_variant, 
                        a.put_up_desc 
                      FROM 
                        (
                          (
                            SELECT 
                              edw_product_key_attributes.ctry_nm, 
                              edw_product_key_attributes.gcph_franchise, 
                              edw_product_key_attributes.gcph_needstate, 
                              edw_product_key_attributes.gcph_category, 
                              edw_product_key_attributes.gcph_brand, 
                              edw_product_key_attributes.gcph_subcategory, 
                              edw_product_key_attributes.gcph_subbrand, 
                              edw_product_key_attributes.gcph_variant, 
                              edw_product_key_attributes.put_up_desc, 
                              edw_product_key_attributes.ean_upc, 
                              "replace"(
                                (
                                  (
                                    edw_product_key_attributes.crt_on
                                  ):: character varying
                                ):: text, 
                                ('-' :: character varying):: text, 
                                ('' :: character varying):: text
                              ) AS crt_on, 
                              edw_product_key_attributes.lst_nts 
                            FROM 
                              edw_product_key_attributes 
                            WHERE 
                              (
                                (
                                  edw_product_key_attributes.matl_type_cd
                                ):: text = ('FERT' :: character varying):: text
                              ) 
                            GROUP BY 
                              edw_product_key_attributes.ctry_nm, 
                              edw_product_key_attributes.gcph_franchise, 
                              edw_product_key_attributes.gcph_needstate, 
                              edw_product_key_attributes.gcph_category, 
                              edw_product_key_attributes.gcph_brand, 
                              edw_product_key_attributes.gcph_subcategory, 
                              edw_product_key_attributes.gcph_subbrand, 
                              edw_product_key_attributes.gcph_variant, 
                              edw_product_key_attributes.put_up_desc, 
                              edw_product_key_attributes.ean_upc, 
                              "replace"(
                                (
                                  (
                                    edw_product_key_attributes.crt_on
                                  ):: character varying
                                ):: text, 
                                ('-' :: character varying):: text, 
                                ('' :: character varying):: text
                              ), 
                              edw_product_key_attributes.lst_nts
                          ) a 
                          JOIN (
                            SELECT 
                              edw_product_key_attributes.ctry_nm, 
                              edw_product_key_attributes.ean_upc, 
                              "replace"(
                                (
                                  (
                                    edw_product_key_attributes.crt_on
                                  ):: character varying
                                ):: text, 
                                ('-' :: character varying):: text, 
                                ('' :: character varying):: text
                              ) AS latest_crt_on, 
                              edw_product_key_attributes.lst_nts AS latest_lst_nts, 
                              row_number() OVER(
                                PARTITION BY trim(edw_product_key_attributes.ctry_nm), 
                                trim(edw_product_key_attributes.ean_upc) 
                                ORDER BY 
                                  edw_product_key_attributes.lst_nts, 
                                  "replace"(
                                    (
                                      (
                                        edw_product_key_attributes.crt_on
                                      ):: character varying
                                    ):: text, 
                                    ('-' :: character varying):: text, 
                                    ('' :: character varying):: text
                                  ) DESC
                              ) AS row_number 
                            FROM 
                              edw_product_key_attributes 
                            WHERE 
                              (
                                (
                                  (
                                    (
                                      (
                                        edw_product_key_attributes.matl_type_cd
                                      ):: text = ('FERT' :: character varying):: text
                                    ) 
                                    AND (
                                      upper(
                                        (
                                          edw_product_key_attributes.ctry_nm
                                        ):: text
                                      ) = ('THAILAND' :: character varying):: text
                                    )
                                  ) 
                                  AND (
                                    edw_product_key_attributes.ean_upc IS NOT NULL
                                  )
                                ) 
                                AND (
                                  (
                                    edw_product_key_attributes.ean_upc
                                  ):: text <> ('' :: character varying):: text
                                )
                              ) 
                            GROUP BY 
                              edw_product_key_attributes.ctry_nm, 
                              edw_product_key_attributes.ean_upc, 
                              "replace"(
                                (
                                  (
                                    edw_product_key_attributes.crt_on
                                  ):: character varying
                                ):: text, 
                                ('-' :: character varying):: text, 
                                ('' :: character varying):: text
                              ), 
                              edw_product_key_attributes.lst_nts
                          ) b ON (
                            (
                              (
                                (
                                  (
                                    (
                                      (a.ctry_nm):: text = (b.ctry_nm):: text
                                    ) 
                                    AND (
                                      (a.ean_upc):: text = (b.ean_upc):: text
                                    )
                                  ) 
                                  AND (b.latest_lst_nts = a.lst_nts)
                                ) 
                                AND (b.row_number = 1)
                              ) 
                              AND (b.latest_crt_on = a.crt_on)
                            )
                          )
                        ) 
                      GROUP BY 
                        a.ctry_nm, 
                        a.ean_upc, 
                        a.gcph_franchise, 
                        a.gcph_needstate, 
                        a.gcph_category, 
                        a.gcph_brand, 
                        a.gcph_subcategory, 
                        a.gcph_subbrand, 
                        a.gcph_variant, 
                        a.put_up_desc
                    ) pka ON (
                      (
                        (
                          (b.barcode):: text = (pka.ean_upc):: text
                        ) 
                        AND (
                          (pka.ctry_nm):: text = ('Thailand' :: character varying):: text
                        )
                      )
                    )
                  ) 
                  LEFT JOIN (
                    SELECT 
                      edw_vw_ps_weights.market, 
                      edw_vw_ps_weights.kpi, 
                      edw_vw_ps_weights.channel, 
                      edw_vw_ps_weights.retail_environment, 
                      edw_vw_ps_weights.weight 
                    FROM 
                      edw_vw_ps_weights 
                    WHERE 
                      (
                        (
                          upper(
                            (edw_vw_ps_weights.kpi):: text
                          ) = (
                            'MSL COMPLIANCE' :: character varying
                          ):: text
                        ) 
                        AND (
                          upper(
                            (edw_vw_ps_weights.market):: text
                          ) = ('THAILAND' :: character varying):: text
                        )
                      )
                  ) wt ON (
                    (
                      (
                        upper(
                          (wt.retail_environment):: text
                        ) = upper(
                          (a.retail_environment):: text
                        )
                      ) 
                      AND (
                        upper(
                          (wt.channel):: text
                        ) = upper(
                          (a.channel):: text
                        )
                      )
                    )
                  )
                ) 
                LEFT JOIN (
                  SELECT 
                    edw_vw_ps_targets.market, 
                    edw_vw_ps_targets.kpi, 
                    edw_vw_ps_targets.channel, 
                    edw_vw_ps_targets.retail_environment, 
                    edw_vw_ps_targets.value 
                  FROM 
                    edw_vw_ps_targets 
                  WHERE 
                    (
                      (
                        upper(
                          (edw_vw_ps_targets.kpi):: text
                        ) = (
                          'MSL COMPLIANCE' :: character varying
                        ):: text
                      ) 
                      AND (
                        upper(
                          (edw_vw_ps_targets.market):: text
                        ) = ('THAILAND' :: character varying):: text
                      )
                    )
                ) tgt ON (
                  (
                    (
                      upper(
                        (tgt.retail_environment):: text
                      ) = upper(
                        (a.retail_environment):: text
                      )
                    ) 
                    AND (
                      upper(
                        (tgt.channel):: text
                      ) = upper(
                        (a.channel):: text
                      )
                    )
                  )
                )
              ) 
            WHERE 
              (
                "left"(
                  "replace"(
                    (b.osa_oos_date):: text, 
                    ('-' :: character varying):: text, 
                    ('' :: character varying):: text
                  ), 
                  4
                ) > (
                  (
                    (
                      "date_part"(
                        year, 
                        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
                      ) -2
                    )
                  ):: character varying
                ):: text
              )
            ) ,
consumer_reach as (
            SELECT 
              'Consumer Reach' AS dataset, 
              b.retail AS customerid, 
              NULL AS salespersonid, 
              'TRUE' :: character varying AS mustcarryitem, 
              NULL AS answerscore, 
              CASE WHEN (
                (b.distribution):: text = ('1' :: character varying):: text
              ) THEN 'TRUE' :: character varying ELSE 'FALSE' :: character varying END AS presence, 
              NULL AS outofstock, 
              'MSL COMPLIANCE' AS kpi, 
              to_date(
                (
                  (
                    to_date(
                      (b.cdate):: timestamp without time zone
                    )
                  ):: character varying
                ):: text, 
                ('YYYY-MM-DD' :: character varying):: text
              ) AS scheduleddate, 
              'completed' AS vst_status, 
              (
                "substring"(
                  "replace"(
                    (
                      (
                        to_date(
                          (b.cdate):: timestamp without time zone
                        )
                      ):: character varying
                    ):: text, 
                    ('-' :: character varying):: text, 
                    ('' :: character varying):: text
                  ), 
                  0, 
                  4
                )
              ):: character varying AS fisc_yr, 
              b.yearmo AS fisc_per, 
              NULL AS firstname, 
              '' AS lastname, 
              (
                (
                  (
                    (
                      (
                        (
                          (b.retailname):: text || (' - ' :: character varying):: text
                        ) || (b.retailbranch):: text
                      ) || (' (' :: character varying):: text
                    ) || (b.retail):: text
                  ) || (')' :: character varying):: text
                )
              ):: character varying AS customername, 
              'Thailand' AS country, 
              b.retailprovince AS state, 
              b.retailname AS storereference, 
              (
                upper(
                  (a.retail_environment):: text
                )
              ):: character varying AS storetype, 
              (
                upper(
                  (a.channel):: text
                )
              ):: character varying AS channel, 
              NULL AS salesgroup, 
              (
                ltrim(
                  (b.jjskubarcode):: text, 
                  (
                    (0):: character varying
                  ):: text
                )
              ):: character varying AS eannumber, 
              pka.gcph_franchise AS prod_hier_l1, 
              pka.gcph_needstate AS prod_hier_l2, 
              pka.gcph_category AS prod_hier_l3, 
              pka.gcph_brand AS prod_hier_l4, 
              pka.gcph_subcategory AS prod_hier_l5, 
              pka.gcph_subbrand AS prod_hier_l6, 
              pka.gcph_variant AS prod_hier_l7, 
              pka.put_up_desc AS prod_hier_l8, 
              (
                (
                  (
                    (b.jjskubarcode):: text || (' - ' :: character varying):: text
                  ) || (b.jjskuname):: text
                )
              ):: character varying AS prod_hier_l9, 
              (wt.weight):: double precision AS kpi_chnl_wt, 
              NULL AS "y/n_flag", 
              NULL AS posm_execution_flag, 
              'Y' AS priority_store_flag, 
              'Is the SKU present?' AS questiontext, 
              NULL AS ques_desc, 
              NULL AS value, 
              tgt.value AS mkt_share, 
              b.status AS rej_reason, 
              NULL AS photo_url 
            FROM 
              (
                (
                  (
                    (
                      (
                        (
                          SELECT 
                            itg_jnj_consumerreach_cvs.id, 
                            itg_jnj_consumerreach_cvs.cdate, 
                            itg_jnj_consumerreach_cvs.retail, 
                            itg_jnj_consumerreach_cvs.retailname, 
                            itg_jnj_consumerreach_cvs.retailbranch, 
                            itg_jnj_consumerreach_cvs.retailprovince, 
                            itg_jnj_consumerreach_cvs.jjskubarcode, 
                            itg_jnj_consumerreach_cvs.jjskuname, 
                            itg_jnj_consumerreach_cvs.jjcore, 
                            itg_jnj_consumerreach_cvs.status, 
                            itg_jnj_consumerreach_cvs.run_id, 
                            itg_jnj_consumerreach_cvs.file_name, 
                            itg_jnj_consumerreach_cvs.yearmo, 
                            (
                              "max"(
                                (
                                  itg_jnj_consumerreach_cvs.distribution
                                ):: text
                              )
                            ):: character varying AS distribution 
                          FROM 
                            itg_jnj_consumerreach_cvs 
                          WHERE 
                            (
                              (
                                itg_jnj_consumerreach_cvs.jjcore
                              ):: text = ('1' :: character varying):: text
                            ) 
                          GROUP BY 
                            itg_jnj_consumerreach_cvs.id, 
                            itg_jnj_consumerreach_cvs.cdate, 
                            itg_jnj_consumerreach_cvs.retail, 
                            itg_jnj_consumerreach_cvs.retailname, 
                            itg_jnj_consumerreach_cvs.retailbranch, 
                            itg_jnj_consumerreach_cvs.retailprovince, 
                            itg_jnj_consumerreach_cvs.jjskubarcode, 
                            itg_jnj_consumerreach_cvs.jjskuname, 
                            itg_jnj_consumerreach_cvs.jjcore, 
                            itg_jnj_consumerreach_cvs.status, 
                            itg_jnj_consumerreach_cvs.run_id, 
                            itg_jnj_consumerreach_cvs.file_name, 
                            itg_jnj_consumerreach_cvs.yearmo 
                          UNION ALL 
                          SELECT 
                            itg_jnj_consumerreach_711.id, 
                            itg_jnj_consumerreach_711.cdate, 
                            itg_jnj_consumerreach_711.retail, 
                            itg_jnj_consumerreach_711.retailname, 
                            itg_jnj_consumerreach_711.retailbranch, 
                            itg_jnj_consumerreach_711.retailprovince, 
                            itg_jnj_consumerreach_711.jjskubarcode, 
                            itg_jnj_consumerreach_711.jjskuname, 
                            itg_jnj_consumerreach_711.jjcore, 
                            itg_jnj_consumerreach_711.status, 
                            itg_jnj_consumerreach_711.run_id, 
                            itg_jnj_consumerreach_711.file_name, 
                            itg_jnj_consumerreach_711.yearmo, 
                            (
                              "max"(
                                (
                                  itg_jnj_consumerreach_711.distribution
                                ):: text
                              )
                            ):: character varying AS distribution 
                          FROM 
                            itg_jnj_consumerreach_711 
                          WHERE 
                            (
                              (
                                itg_jnj_consumerreach_711.jjcore
                              ):: text = ('1' :: character varying):: text
                            ) 
                          GROUP BY 
                            itg_jnj_consumerreach_711.id, 
                            itg_jnj_consumerreach_711.cdate, 
                            itg_jnj_consumerreach_711.retail, 
                            itg_jnj_consumerreach_711.retailname, 
                            itg_jnj_consumerreach_711.retailbranch, 
                            itg_jnj_consumerreach_711.retailprovince, 
                            itg_jnj_consumerreach_711.jjskubarcode, 
                            itg_jnj_consumerreach_711.jjskuname, 
                            itg_jnj_consumerreach_711.jjcore, 
                            itg_jnj_consumerreach_711.status, 
                            itg_jnj_consumerreach_711.run_id, 
                            itg_jnj_consumerreach_711.file_name, 
                            itg_jnj_consumerreach_711.yearmo
                        ) 
                        UNION ALL 
                        SELECT 
                          itg_jnj_consumerreach_sfm.id, 
                          itg_jnj_consumerreach_sfm.cdate, 
                          itg_jnj_consumerreach_sfm.retail, 
                          itg_jnj_consumerreach_sfm.retailname, 
                          itg_jnj_consumerreach_sfm.retailbranch, 
                          itg_jnj_consumerreach_sfm.retailprovince, 
                          itg_jnj_consumerreach_sfm.jjskubarcode, 
                          itg_jnj_consumerreach_sfm.jjskuname, 
                          itg_jnj_consumerreach_sfm.jjcore, 
                          itg_jnj_consumerreach_sfm.status, 
                          itg_jnj_consumerreach_sfm.run_id, 
                          itg_jnj_consumerreach_sfm.file_name, 
                          itg_jnj_consumerreach_sfm.yearmo, 
                          (
                            "max"(
                              (
                                itg_jnj_consumerreach_sfm.distribution
                              ):: text
                            )
                          ):: character varying AS distribution 
                        FROM 
                          itg_jnj_consumerreach_sfm 
                        WHERE 
                          (
                            (
                              itg_jnj_consumerreach_sfm.jjcore
                            ):: text = ('1' :: character varying):: text
                          ) 
                        GROUP BY 
                          itg_jnj_consumerreach_sfm.id, 
                          itg_jnj_consumerreach_sfm.cdate, 
                          itg_jnj_consumerreach_sfm.retail, 
                          itg_jnj_consumerreach_sfm.retailname, 
                          itg_jnj_consumerreach_sfm.retailbranch, 
                          itg_jnj_consumerreach_sfm.retailprovince, 
                          itg_jnj_consumerreach_sfm.jjskubarcode, 
                          itg_jnj_consumerreach_sfm.jjskuname, 
                          itg_jnj_consumerreach_sfm.jjcore, 
                          itg_jnj_consumerreach_sfm.status, 
                          itg_jnj_consumerreach_sfm.run_id, 
                          itg_jnj_consumerreach_sfm.file_name, 
                          itg_jnj_consumerreach_sfm.yearmo
                      ) b 
                      JOIN (
                        SELECT 
                          DISTINCT itg_mds_th_ps_store.dataset, 
                          itg_mds_th_ps_store.channel, 
                          itg_mds_th_ps_store.retail_environment, 
                          itg_mds_th_ps_store.state, 
                          itg_mds_th_ps_store.customer_code, 
                          itg_mds_th_ps_store.customer_name, 
                          itg_mds_th_ps_store.store_code, 
                          itg_mds_th_ps_store.store_name 
                        FROM 
                          itg_mds_th_ps_store
                      ) a ON (
                        (
                          (
                            upper(
                              (b.retailname):: text
                            ) = upper(
                              (a.customer_name):: text
                            )
                          ) 
                          AND (
                            (a.dataset):: text = (
                              'Consumer Reach' :: character varying
                            ):: text
                          )
                        )
                      )
                    ) 
                    LEFT JOIN (
                      SELECT 
                        a.ctry_nm, 
                        a.ean_upc, 
                        a.gcph_franchise, 
                        a.gcph_needstate, 
                        a.gcph_category, 
                        a.gcph_brand, 
                        a.gcph_subcategory, 
                        a.gcph_subbrand, 
                        a.gcph_variant, 
                        a.put_up_desc 
                      FROM 
                        (
                          (
                            SELECT 
                              edw_product_key_attributes.ctry_nm, 
                              edw_product_key_attributes.gcph_franchise, 
                              edw_product_key_attributes.gcph_needstate, 
                              edw_product_key_attributes.gcph_category, 
                              edw_product_key_attributes.gcph_brand, 
                              edw_product_key_attributes.gcph_subcategory, 
                              edw_product_key_attributes.gcph_subbrand, 
                              edw_product_key_attributes.gcph_variant, 
                              edw_product_key_attributes.put_up_desc, 
                              edw_product_key_attributes.ean_upc, 
                              "replace"(
                                (
                                  (
                                    edw_product_key_attributes.crt_on
                                  ):: character varying
                                ):: text, 
                                ('-' :: character varying):: text, 
                                ('' :: character varying):: text
                              ) AS crt_on, 
                              edw_product_key_attributes.lst_nts 
                            FROM 
                              edw_product_key_attributes 
                            WHERE 
                              (
                                (
                                  edw_product_key_attributes.matl_type_cd
                                ):: text = ('FERT' :: character varying):: text
                              ) 
                            GROUP BY 
                              edw_product_key_attributes.ctry_nm, 
                              edw_product_key_attributes.gcph_franchise, 
                              edw_product_key_attributes.gcph_needstate, 
                              edw_product_key_attributes.gcph_category, 
                              edw_product_key_attributes.gcph_brand, 
                              edw_product_key_attributes.gcph_subcategory, 
                              edw_product_key_attributes.gcph_subbrand, 
                              edw_product_key_attributes.gcph_variant, 
                              edw_product_key_attributes.put_up_desc, 
                              edw_product_key_attributes.ean_upc, 
                              "replace"(
                                (
                                  (
                                    edw_product_key_attributes.crt_on
                                  ):: character varying
                                ):: text, 
                                ('-' :: character varying):: text, 
                                ('' :: character varying):: text
                              ), 
                              edw_product_key_attributes.lst_nts
                          ) a 
                          JOIN (
                            SELECT 
                              edw_product_key_attributes.ctry_nm, 
                              edw_product_key_attributes.ean_upc, 
                              "replace"(
                                (
                                  (
                                    edw_product_key_attributes.crt_on
                                  ):: character varying
                                ):: text, 
                                ('-' :: character varying):: text, 
                                ('' :: character varying):: text
                              ) AS latest_crt_on, 
                              edw_product_key_attributes.lst_nts AS latest_lst_nts, 
                              row_number() OVER(
                                PARTITION BY edw_product_key_attributes.ctry_nm, 
                                edw_product_key_attributes.ean_upc 
                                ORDER BY 
                                  edw_product_key_attributes.lst_nts, 
                                  "replace"(
                                    (
                                      (
                                        edw_product_key_attributes.crt_on
                                      ):: character varying
                                    ):: text, 
                                    ('-' :: character varying):: text, 
                                    ('' :: character varying):: text
                                  ) DESC
                              ) AS row_number 
                            FROM 
                              edw_product_key_attributes 
                            WHERE 
                              (
                                (
                                  (
                                    (
                                      (
                                        edw_product_key_attributes.matl_type_cd
                                      ):: text = ('FERT' :: character varying):: text
                                    ) 
                                    AND (
                                      upper(
                                        (
                                          edw_product_key_attributes.ctry_nm
                                        ):: text
                                      ) = ('THAILAND' :: character varying):: text
                                    )
                                  ) 
                                  AND (
                                    edw_product_key_attributes.ean_upc IS NOT NULL
                                  )
                                ) 
                                AND (
                                  (
                                    edw_product_key_attributes.ean_upc
                                  ):: text <> ('' :: character varying):: text
                                )
                              ) 
                            GROUP BY 
                              edw_product_key_attributes.ctry_nm, 
                              edw_product_key_attributes.ean_upc, 
                              "replace"(
                                (
                                  (
                                    edw_product_key_attributes.crt_on
                                  ):: character varying
                                ):: text, 
                                ('-' :: character varying):: text, 
                                ('' :: character varying):: text
                              ), 
                              edw_product_key_attributes.lst_nts
                          ) b ON (
                            (
                              (
                                (
                                  (
                                    (
                                      (a.ctry_nm):: text = (b.ctry_nm):: text
                                    ) 
                                    AND (
                                      (a.ean_upc):: text = (b.ean_upc):: text
                                    )
                                  ) 
                                  AND (b.latest_lst_nts = a.lst_nts)
                                ) 
                                AND (b.row_number = 1)
                              ) 
                              AND (b.latest_crt_on = a.crt_on)
                            )
                          )
                        ) 
                      GROUP BY 
                        a.ctry_nm, 
                        a.ean_upc, 
                        a.gcph_franchise, 
                        a.gcph_needstate, 
                        a.gcph_category, 
                        a.gcph_brand, 
                        a.gcph_subcategory, 
                        a.gcph_subbrand, 
                        a.gcph_variant, 
                        a.put_up_desc
                    ) pka ON (
                      (
                        (
                          (b.jjskubarcode):: text = (pka.ean_upc):: text
                        ) 
                        AND (
                          (pka.ctry_nm):: text = ('Thailand' :: character varying):: text
                        )
                      )
                    )
                  ) 
                  LEFT JOIN (
                    SELECT 
                      edw_vw_ps_weights.market, 
                      edw_vw_ps_weights.kpi, 
                      edw_vw_ps_weights.channel, 
                      edw_vw_ps_weights.retail_environment, 
                      edw_vw_ps_weights.weight 
                    FROM 
                      edw_vw_ps_weights 
                    WHERE 
                      (
                        (
                          (
                            upper(
                              (edw_vw_ps_weights.channel):: text
                            ) = ('MT' :: character varying):: text
                          ) 
                          AND (
                            upper(
                              (edw_vw_ps_weights.kpi):: text
                            ) = (
                              'MSL COMPLIANCE' :: character varying
                            ):: text
                          )
                        ) 
                        AND (
                          upper(
                            (edw_vw_ps_weights.market):: text
                          ) = ('THAILAND' :: character varying):: text
                        )
                      )
                  ) wt ON (
                    (
                      upper(
                        (wt.retail_environment):: text
                      ) = upper(
                        (a.retail_environment):: text
                      )
                    )
                  )
                ) 
                LEFT JOIN (
                  SELECT 
                    edw_vw_ps_targets.market, 
                    edw_vw_ps_targets.kpi, 
                    edw_vw_ps_targets.channel, 
                    edw_vw_ps_targets.retail_environment, 
                    edw_vw_ps_targets.value 
                  FROM 
                    edw_vw_ps_targets 
                  WHERE 
                    (
                      (
                        (
                          upper(
                            (edw_vw_ps_targets.channel):: text
                          ) = ('MT' :: character varying):: text
                        ) 
                        AND (
                          upper(
                            (edw_vw_ps_targets.kpi):: text
                          ) = (
                            'MSL COMPLIANCE' :: character varying
                          ):: text
                        )
                      ) 
                      AND (
                        upper(
                          (edw_vw_ps_targets.market):: text
                        ) = ('THAILAND' :: character varying):: text
                      )
                    )
                ) tgt ON (
                  (
                    upper(
                      (tgt.retail_environment):: text
                    ) = upper(
                      (a.retail_environment):: text
                    )
                  )
                )
              ) 
            WHERE 
              (
                (
                  (b.jjcore):: text = (
                    (1):: character varying
                  ):: text
                ) 
                AND (
                  "left"(
                    "replace"(
                      (b.cdate):: text, 
                      ('-' :: character varying):: text, 
                      ('' :: character varying):: text
                    ), 
                    4
                  ) > (
                    (
                      (
                        "date_part"(
                          year, 
                          convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
                        ) -2
                      )
                    ):: character varying
                  ):: text
                )
              ) 
            UNION ALL 
 select * from adecco_msl
          ) ,
adecco as  (select
            'Adecco' AS dataset, 
            COALESCE(
              a.store_code, '' :: character varying
            ) AS customerid, 
            b.emp_address_pc AS salespersonid, 
            'true' AS mustcarryitem, 
            NULL AS answerscore, 
            'TRUE' AS presence, 
            CASE WHEN (
              (
                (b.oos):: character varying
              ):: text = (
                (
                  (1):: double precision
                ):: character varying
              ):: text
            ) THEN 'true' :: character varying ELSE '' :: character varying END AS outofstock, 
            'OOS COMPLIANCE' AS kpi, 
            to_date(
              (
                (
                  to_date(
                    (b.osa_oos_date):: timestamp without time zone
                  )
                ):: character varying
              ):: text, 
              ('YYYY-MM-DD' :: character varying):: text
            ) AS scheduleddate, 
            'completed' AS vst_status, 
            (
              "substring"(
                "replace"(
                  (
                    (
                      to_date(
                        (b.osa_oos_date):: timestamp without time zone
                      )
                    ):: character varying
                  ):: text, 
                  ('-' :: character varying):: text, 
                  ('' :: character varying):: text
                ), 
                0, 
                4
              )
            ):: character varying AS fisc_yr, 
            b.yearmo AS fisc_per, 
            (
              (
                (
                  (b.emp_address_pc):: text || (' - ' :: character varying):: text
                ) || (b.pc_name):: text
              )
            ):: character varying AS firstname, 
            '' AS lastname, 
            (
              (
                (
                  (
                    COALESCE(
                      a.store_code, 'NA' :: character varying
                    )
                  ):: text || (' - ' :: character varying):: text
                ) || (
                  COALESCE(
                    a.store_name, 'NA' :: character varying
                  )
                ):: text
              )
            ):: character varying AS customername, 
            'Thailand' AS country, 
            a.state, 
            a.customer_name AS storereference, 
            (
              upper(
                (a.retail_environment):: text
              )
            ):: character varying AS storetype, 
            (
              upper(
                (a.channel):: text
              )
            ):: character varying AS channel, 
            NULL AS salesgroup, 
            (
              ltrim(
                (b.barcode):: text, 
                (
                  (0):: character varying
                ):: text
              )
            ):: character varying AS eannumber, 
            pka.gcph_franchise AS prod_hier_l1, 
            pka.gcph_needstate AS prod_hier_l2, 
            pka.gcph_category AS prod_hier_l3, 
            pka.gcph_brand AS prod_hier_l4, 
            pka.gcph_subcategory AS prod_hier_l5, 
            pka.gcph_subbrand AS prod_hier_l6, 
            pka.gcph_variant AS prod_hier_l7, 
            pka.put_up_desc AS prod_hier_l8, 
            (
              (
                (
                  (b.barcode):: text || (' - ' :: character varying):: text
                ) || (b.sku):: text
              )
            ):: character varying AS prod_hier_l9, 
            (wt.weight):: double precision AS kpi_chnl_wt, 
            NULL AS "y/n_flag", 
            NULL AS posm_execution_flag, 
            'Y' AS priority_store_flag, 
            'Is the product available on shelf?' AS questiontext, 
            NULL AS ques_desc, 
            NULL AS value, 
            CASE WHEN (tgt.value IS NULL) THEN (
              (1):: numeric
            ):: numeric(18, 0) ELSE tgt.value END AS mkt_share, 
            b.oos_reason AS rej_reason, 
            NULL AS photo_url 
          FROM 
            (
              (
                (
                  (
                    (
                      SELECT 
                        trim(itg_jnj_osa_oos_report.osa_oos_date) as osa_oos_date, 
                        trim(itg_jnj_osa_oos_report.week) as week, 
                        trim(itg_jnj_osa_oos_report.emp_address_pc) as emp_address_pc, 
                        trim(itg_jnj_osa_oos_report.pc_name) as pc_name, 
                        trim(itg_jnj_osa_oos_report.emp_address_supervisor) as emp_address_supervisor, 
                        trim(itg_jnj_osa_oos_report.supervisor_name) as supervisor_name, 
                        trim(itg_jnj_osa_oos_report.area) as area, 
                        trim(itg_jnj_osa_oos_report.channel) as channel, 
                        trim(ITG_JNJ_OSA_OOS_REPORT.account) as account, 
                        trim(itg_jnj_osa_oos_report.store_id) as store_id, 
                        trim(itg_jnj_osa_oos_report.store_name) as store_name, 
                        trim(itg_jnj_osa_oos_report.shop_type) as shop_type, 
                        trim(itg_jnj_osa_oos_report.brand) as brand, 
                        trim(itg_jnj_osa_oos_report.category) as category, 
                        trim(itg_jnj_osa_oos_report.barcode) as barcode, 
                        trim(itg_jnj_osa_oos_report.sku) as sku, 
                        trim(itg_jnj_osa_oos_report.msl_price_tag) as msl_price_tag, 
                        trim(itg_jnj_osa_oos_report.oos_reason) as oos_reason, 
                        trim(itg_jnj_osa_oos_report.yearmo) as yearmo ,
                        min(
                          (itg_jnj_osa_oos_report.oos):: text
                        ) AS oos 
                      FROM 
                        itg_jnj_osa_oos_report 
                      GROUP BY 
                        trim(itg_jnj_osa_oos_report.osa_oos_date), 
                        trim(itg_jnj_osa_oos_report.week), 
                        trim(itg_jnj_osa_oos_report.emp_address_pc), 
                        trim(itg_jnj_osa_oos_report.pc_name), 
                        trim(itg_jnj_osa_oos_report.emp_address_supervisor), 
                        trim(itg_jnj_osa_oos_report.supervisor_name), 
                        trim(itg_jnj_osa_oos_report.area), 
                        trim(itg_jnj_osa_oos_report.channel), 
                        trim(ITG_JNJ_OSA_OOS_REPORT.account), 
                        trim(itg_jnj_osa_oos_report.store_id), 
                        trim(itg_jnj_osa_oos_report.store_name), 
                        trim(itg_jnj_osa_oos_report.shop_type), 
                        trim(itg_jnj_osa_oos_report.brand), 
                        trim(itg_jnj_osa_oos_report.category), 
                        trim(itg_jnj_osa_oos_report.barcode), 
                        trim(itg_jnj_osa_oos_report.sku), 
                        trim(itg_jnj_osa_oos_report.msl_price_tag), 
                        trim(itg_jnj_osa_oos_report.oos_reason), 
                        trim(itg_jnj_osa_oos_report.yearmo)
                    ) b 
                    LEFT JOIN (
                      SELECT 
                        DISTINCT itg_mds_th_ps_store.dataset, 
                        itg_mds_th_ps_store.channel, 
                        itg_mds_th_ps_store.retail_environment, 
                        itg_mds_th_ps_store.state, 
                        itg_mds_th_ps_store.customer_code, 
                        itg_mds_th_ps_store.customer_name, 
                        itg_mds_th_ps_store.store_code, 
                        itg_mds_th_ps_store.store_name 
                      FROM 
                        itg_mds_th_ps_store 
                      WHERE 
                        (
                          upper(
                            (itg_mds_th_ps_store.dataset):: text
                          ) = ('ADECCO' :: character varying):: text
                        )
                    ) a ON (
                      (
                        upper(
                          (b.store_name):: text
                        ) = upper(
                          (a.store_name):: text
                        )
                      )
                    )
                  ) 
                  LEFT JOIN (
                    SELECT 
                      a.ctry_nm, 
                      a.ean_upc, 
                      a.gcph_franchise, 
                      a.gcph_needstate, 
                      a.gcph_category, 
                      a.gcph_brand, 
                      a.gcph_subcategory, 
                      a.gcph_subbrand, 
                      a.gcph_variant, 
                      a.put_up_desc 
                    FROM 
                      (
                        (
                          SELECT 
                            edw_product_key_attributes.ctry_nm, 
                            edw_product_key_attributes.gcph_franchise, 
                            edw_product_key_attributes.gcph_needstate, 
                            edw_product_key_attributes.gcph_category, 
                            edw_product_key_attributes.gcph_brand, 
                            edw_product_key_attributes.gcph_subcategory, 
                            edw_product_key_attributes.gcph_subbrand, 
                            edw_product_key_attributes.gcph_variant, 
                            edw_product_key_attributes.put_up_desc, 
                            edw_product_key_attributes.ean_upc, 
                            "replace"(
                              (
                                (
                                  edw_product_key_attributes.crt_on
                                ):: character varying
                              ):: text, 
                              ('-' :: character varying):: text, 
                              ('' :: character varying):: text
                            ) AS crt_on, 
                            edw_product_key_attributes.lst_nts 
                          FROM 
                            edw_product_key_attributes 
                          WHERE 
                            (
                              (
                                edw_product_key_attributes.matl_type_cd
                              ):: text = ('FERT' :: character varying):: text
                            ) 
                          GROUP BY 
                            edw_product_key_attributes.ctry_nm, 
                            edw_product_key_attributes.gcph_franchise, 
                            edw_product_key_attributes.gcph_needstate, 
                            edw_product_key_attributes.gcph_category, 
                            edw_product_key_attributes.gcph_brand, 
                            edw_product_key_attributes.gcph_subcategory, 
                            edw_product_key_attributes.gcph_subbrand, 
                            edw_product_key_attributes.gcph_variant, 
                            edw_product_key_attributes.put_up_desc, 
                            edw_product_key_attributes.ean_upc, 
                            "replace"(
                              (
                                (
                                  edw_product_key_attributes.crt_on
                                ):: character varying
                              ):: text, 
                              ('-' :: character varying):: text, 
                              ('' :: character varying):: text
                            ), 
                            edw_product_key_attributes.lst_nts
                        ) a 
                        JOIN (
                          SELECT 
                            edw_product_key_attributes.ctry_nm, 
                            edw_product_key_attributes.ean_upc, 
                            "replace"(
                              (
                                (
                                  edw_product_key_attributes.crt_on
                                ):: character varying
                              ):: text, 
                              ('-' :: character varying):: text, 
                              ('' :: character varying):: text
                            ) AS latest_crt_on, 
                            edw_product_key_attributes.lst_nts AS latest_lst_nts, 
                            row_number() OVER(
                              PARTITION BY edw_product_key_attributes.ctry_nm, 
                              edw_product_key_attributes.ean_upc 
                              ORDER BY 
                                edw_product_key_attributes.lst_nts, 
                                "replace"(
                                  (
                                    (
                                      edw_product_key_attributes.crt_on
                                    ):: character varying
                                  ):: text, 
                                  ('-' :: character varying):: text, 
                                  ('' :: character varying):: text
                                ) DESC
                            ) AS row_number 
                          FROM 
                            edw_product_key_attributes 
                          WHERE 
                            (
                              (
                                (
                                  (
                                    (
                                      edw_product_key_attributes.matl_type_cd
                                    ):: text = ('FERT' :: character varying):: text
                                  ) 
                                  AND (
                                    upper(
                                      (
                                        edw_product_key_attributes.ctry_nm
                                      ):: text
                                    ) = ('THAILAND' :: character varying):: text
                                  )
                                ) 
                                AND (
                                  edw_product_key_attributes.ean_upc IS NOT NULL
                                )
                              ) 
                              AND (
                                (
                                  edw_product_key_attributes.ean_upc
                                ):: text <> ('' :: character varying):: text
                              )
                            ) 
                          GROUP BY 
                            edw_product_key_attributes.ctry_nm, 
                            edw_product_key_attributes.ean_upc, 
                            "replace"(
                              (
                                (
                                  edw_product_key_attributes.crt_on
                                ):: character varying
                              ):: text, 
                              ('-' :: character varying):: text, 
                              ('' :: character varying):: text
                            ), 
                            edw_product_key_attributes.lst_nts
                        ) b ON (
                          (
                            (
                              (
                                (
                                  (
                                    (a.ctry_nm):: text = (b.ctry_nm):: text
                                  ) 
                                  AND (
                                    (a.ean_upc):: text = (b.ean_upc):: text
                                  )
                                ) 
                                AND (b.latest_lst_nts = a.lst_nts)
                              ) 
                              AND (b.row_number = 1)
                            ) 
                            AND (b.latest_crt_on = a.crt_on)
                          )
                        )
                      ) 
                    GROUP BY 
                      a.ctry_nm, 
                      a.ean_upc, 
                      a.gcph_franchise, 
                      a.gcph_needstate, 
                      a.gcph_category, 
                      a.gcph_brand, 
                      a.gcph_subcategory, 
                      a.gcph_subbrand, 
                      a.gcph_variant, 
                      a.put_up_desc
                  ) pka ON (
                    (
                      (
                        (b.barcode):: text = (pka.ean_upc):: text
                      ) 
                      AND (
                        (pka.ctry_nm):: text = ('Thailand' :: character varying):: text
                      )
                    )
                  )
                ) 
                LEFT JOIN (
                  SELECT 
                    edw_vw_ps_weights.market, 
                    edw_vw_ps_weights.kpi, 
                    edw_vw_ps_weights.channel, 
                    edw_vw_ps_weights.retail_environment, 
                    edw_vw_ps_weights.weight 
                  FROM 
                    edw_vw_ps_weights 
                  WHERE 
                    (
                      (
                        upper(
                          (edw_vw_ps_weights.kpi):: text
                        ) = (
                          'OSA COMPLIANCE' :: character varying
                        ):: text
                      ) 
                      AND (
                        upper(
                          (edw_vw_ps_weights.market):: text
                        ) = ('THAILAND' :: character varying):: text
                      )
                    )
                ) wt ON (
                  (
                    (
                      upper(
                        (wt.retail_environment):: text
                      ) = upper(
                        (a.retail_environment):: text
                      )
                    ) 
                    AND (
                      upper(
                        (wt.channel):: text
                      ) = upper(
                        (a.channel):: text
                      )
                    )
                  )
                )
              ) 
              LEFT JOIN (
                SELECT 
                  edw_vw_ps_targets.market, 
                  edw_vw_ps_targets.kpi, 
                  edw_vw_ps_targets.channel, 
                  edw_vw_ps_targets.retail_environment, 
                  edw_vw_ps_targets.value 
                FROM 
                  edw_vw_ps_targets 
                WHERE 
                  (
                    (
                      upper(
                        (edw_vw_ps_targets.kpi):: text
                      ) = (
                        'OSA COMPLIANCE' :: character varying
                      ):: text
                    ) 
                    AND (
                      upper(
                        (edw_vw_ps_targets.market):: text
                      ) = ('THAILAND' :: character varying):: text
                    )
                  )
              ) tgt ON (
                (
                  (
                    upper(
                      (tgt.retail_environment):: text
                    ) = upper(
                      (a.retail_environment):: text
                    )
                  ) 
                  AND (
                    upper(
                      (tgt.channel):: text
                    ) = upper(
                      (a.channel):: text
                    )
                  )
                )
              )
            ) 
          WHERE 
            (
              (
                (b.msl_price_tag):: text = ('1' :: character varying):: text
              ) 
              AND (
                "left"(
                  "replace"(
                    (b.osa_oos_date):: text, 
                    ('-' :: character varying):: text, 
                    ('' :: character varying):: text
                  ), 
                  4
                ) > (
                  (
                    (
                      "date_part"(
                        year, 
                        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
                      ) -2
                    )
                  ):: character varying
                ):: text
              )
            )
        ) ,
union_1 as (
        SELECT 
          derived_table1.dataset, 
          derived_table1.customerid, 
          derived_table1.salespersonid, 
          derived_table1.mustcarryitem, 
          derived_table1.answerscore, 
          derived_table1.presence, 
          derived_table1.outofstock, 
          derived_table1.kpi, 
          derived_table1.scheduleddate, 
          derived_table1.vst_status, 
          derived_table1.fisc_yr, 
          derived_table1.fisc_per, 
          derived_table1.firstname, 
          derived_table1.lastname, 
          derived_table1.customername, 
          derived_table1.country, 
          derived_table1.state, 
          derived_table1.storereference, 
          derived_table1.storetype, 
          derived_table1.channel, 
          derived_table1.salesgroup, 
          derived_table1.eannumber, 
          derived_table1.prod_hier_l1, 
          derived_table1.prod_hier_l2, 
          derived_table1.prod_hier_l3, 
          derived_table1.prod_hier_l4, 
          derived_table1.prod_hier_l5, 
          derived_table1.prod_hier_l6, 
          derived_table1.prod_hier_l7, 
          derived_table1.prod_hier_l8, 
          derived_table1.prod_hier_l9, 
          derived_table1.kpi_chnl_wt, 
          derived_table1."y/n_flag", 
          derived_table1.posm_execution_flag, 
          derived_table1.priority_store_flag, 
          derived_table1.questiontext, 
          derived_table1.ques_desc, 
          derived_table1.value, 
          derived_table1.mkt_share, 
          derived_table1.rej_reason, 
          derived_table1.photo_url 
        FROM 
          (
            SELECT 
              'Adecco' :: character varying AS dataset, 
              COALESCE(
                a.store_code, '' :: character varying
              ) AS customerid, 
              NULL :: character varying AS salespersonid, 
              NULL :: character varying AS mustcarryitem, 
              NULL :: character varying AS answerscore, 
              NULL :: character varying AS presence, 
              NULL :: character varying AS outofstock, 
              'SHARE OF SHELF' :: character varying AS kpi, 
              to_date(
                (
                  (
                    to_date(
                      (b.sos_date):: timestamp without time zone
                    )
                  ):: character varying
                ):: text, 
                ('YYYY-MM-DD' :: character varying):: text
              ) AS scheduleddate, 
              'COMPLETED' :: character varying AS vst_status, 
              (
                "substring"(
                  "replace"(
                    (
                      (
                        to_date(
                          (b.sos_date):: timestamp without time zone
                        )
                      ):: character varying
                    ):: text, 
                    ('-' :: character varying):: text, 
                    ('' :: character varying):: text
                  ), 
                  0, 
                  4
                )
              ):: character varying AS fisc_yr, 
              (
                ltrim(
                  (b.yearmo):: text, 
                  ('_' :: character varying):: text
                )
              ):: character varying AS fisc_per, 
              b.merchandiser_name AS firstname, 
              NULL :: character varying AS lastname, 
              (
                (
                  (
                    (
                      COALESCE(
                        a.store_code, 'NA' :: character varying
                      )
                    ):: text || (' - ' :: character varying):: text
                  ) || (
                    COALESCE(
                      a.store_name, 'NA' :: character varying
                    )
                  ):: text
                )
              ):: character varying AS customername, 
              'Thailand' :: character varying AS country, 
              a.state, 
              a.customer_name AS storereference, 
              (
                upper(
                  (a.retail_environment):: text
                )
              ):: character varying AS storetype, 
              (
                upper(
                  (a.channel):: text
                )
              ):: character varying AS channel, 
              NULL :: character varying AS salesgroup, 
              NULL :: character varying AS eannumber, 
              NULL :: character varying AS prod_hier_l1, 
              NULL :: character varying AS prod_hier_l2, 
              b.category AS prod_hier_l3, 
              NULL :: character varying AS prod_hier_l4, 
              NULL :: character varying AS prod_hier_l5, 
              NULL :: character varying AS prod_hier_l6, 
              NULL :: character varying AS prod_hier_l7, 
              NULL :: character varying AS prod_hier_l8, 
              NULL :: character varying AS prod_hier_l9, 
              (wt.weight):: double precision AS kpi_chnl_wt, 
              NULL :: character varying AS "y/n_flag", 
              NULL :: character varying AS posm_execution_flag, 
              'Y' :: character varying AS priority_store_flag, 
              'What is the J&J facing?' :: character varying AS questiontext, 
              'NUMERATOR' :: character varying AS ques_desc, 
              CASE WHEN (
                (b.agency):: text = ('Johnson' :: character varying):: text
              ) THEN b.size ELSE NULL :: character varying END AS value, 
              CASE WHEN (tgt.value IS NULL) THEN (
                (1):: numeric
              ):: numeric(18, 0) ELSE tgt.value END AS mkt_share, 
              NULL :: character varying AS rej_reason, 
              NULL :: character varying AS photo_url 
            FROM 
              (
                (
                  (
                    (
                      SELECT 
                        itg_jnj_mer_share_of_shelf.sos_date, 
                        itg_jnj_mer_share_of_shelf.merchandiser_name, 
                        itg_jnj_mer_share_of_shelf.supervisor_name, 
                        itg_jnj_mer_share_of_shelf.area, 
                        itg_jnj_mer_share_of_shelf.channel, 
                        ITG_JNJ_MER_SHARE_OF_SHELF.account, 
                        itg_jnj_mer_share_of_shelf.store_id, 
                        itg_jnj_mer_share_of_shelf.store_name, 
                        itg_jnj_mer_share_of_shelf.category, 
                        itg_jnj_mer_share_of_shelf.agency, 
                        itg_jnj_mer_share_of_shelf.brand, 
                        itg_jnj_mer_share_of_shelf.size, 
                        itg_jnj_mer_share_of_shelf.yearmo 
                      FROM 
                        itg_jnj_mer_share_of_shelf 
                      WHERE 
                        (
                          upper(
                            (
                              itg_jnj_mer_share_of_shelf.agency
                            ):: text
                          ) = ('JOHNSON' :: character varying):: text
                        ) 
                      GROUP BY 
                        itg_jnj_mer_share_of_shelf.sos_date, 
                        itg_jnj_mer_share_of_shelf.merchandiser_name, 
                        itg_jnj_mer_share_of_shelf.supervisor_name, 
                        itg_jnj_mer_share_of_shelf.area, 
                        itg_jnj_mer_share_of_shelf.channel, 
                        ITG_JNJ_MER_SHARE_OF_SHELF.account, 
                        itg_jnj_mer_share_of_shelf.store_id, 
                        itg_jnj_mer_share_of_shelf.store_name, 
                        itg_jnj_mer_share_of_shelf.category, 
                        itg_jnj_mer_share_of_shelf.agency, 
                        itg_jnj_mer_share_of_shelf.brand, 
                        itg_jnj_mer_share_of_shelf.size, 
                        itg_jnj_mer_share_of_shelf.yearmo
                    ) b 
                    LEFT JOIN (
                      SELECT 
                        DISTINCT itg_mds_th_ps_store.dataset, 
                        itg_mds_th_ps_store.channel, 
                        itg_mds_th_ps_store.retail_environment, 
                        itg_mds_th_ps_store.state, 
                        itg_mds_th_ps_store.customer_code, 
                        itg_mds_th_ps_store.customer_name, 
                        itg_mds_th_ps_store.store_code, 
                        itg_mds_th_ps_store.store_name 
                      FROM 
                        itg_mds_th_ps_store 
                      WHERE 
                        (
                          (
                            upper(
                              (itg_mds_th_ps_store.dataset):: text
                            ) = ('ADECCO' :: character varying):: text
                          ) 
                          AND (
                            upper(
                              (itg_mds_th_ps_store.channel):: text
                            ) = ('MT' :: character varying):: text
                          )
                        )
                    ) a ON (
                      (
                        upper(
                          (b.store_name):: text
                        ) = upper(
                          (a.store_name):: text
                        )
                      )
                    )
                  ) 
                  LEFT JOIN (
                    SELECT 
                      edw_vw_ps_weights.market, 
                      edw_vw_ps_weights.kpi, 
                      edw_vw_ps_weights.channel, 
                      edw_vw_ps_weights.retail_environment, 
                      edw_vw_ps_weights.weight 
                    FROM 
                      edw_vw_ps_weights 
                    WHERE 
                      (
                        (
                          upper(
                            (edw_vw_ps_weights.kpi):: text
                          ) = (
                            'SOS COMPLIANCE' :: character varying
                          ):: text
                        ) 
                        AND (
                          upper(
                            (edw_vw_ps_weights.market):: text
                          ) = ('THAILAND' :: character varying):: text
                        )
                      )
                  ) wt ON (
                    (
                      (
                        upper(
                          (wt.retail_environment):: text
                        ) = upper(
                          (a.retail_environment):: text
                        )
                      ) 
                      AND (
                        upper(
                          (wt.channel):: text
                        ) = upper(
                          (a.channel):: text
                        )
                      )
                    )
                  )
                ) 
                LEFT JOIN (
                  SELECT 
                    edw_vw_ps_targets.market, 
                    edw_vw_ps_targets.kpi, 
                    edw_vw_ps_targets.channel, 
                    edw_vw_ps_targets.attribute_1, 
                    edw_vw_ps_targets.retail_environment, 
                    edw_vw_ps_targets.value, 
                    edw_vw_ps_targets.attribute_2 
                  FROM 
                    edw_vw_ps_targets 
                  WHERE 
                    (
                      (
                        upper(
                          (edw_vw_ps_targets.kpi):: text
                        ) = (
                          'SOS COMPLIANCE' :: character varying
                        ):: text
                      ) 
                      AND (
                        upper(
                          (edw_vw_ps_targets.market):: text
                        ) = ('THAILAND' :: character varying):: text
                      )
                    )
                ) tgt ON (
                  (
                    (
                      (
                        (
                          upper(
                            (tgt.attribute_1):: text
                          ) = upper(
                            (b.category):: text
                          )
                        ) 
                        AND (
                          upper(
                            (tgt.retail_environment):: text
                          ) = upper(
                            (a.retail_environment):: text
                          )
                        )
                      ) 
                      AND (
                        upper(
                          (tgt.channel):: text
                        ) = upper(
                          (a.channel):: text
                        )
                      )
                    ) 
                    AND (
                      upper(
                        (tgt.attribute_2):: text
                      ) = upper(
                        (a.customer_name):: text
                      )
                    )
                  )
                )
              ) 
            WHERE 
              (
                (
                  upper(
                    (b.agency):: text
                  ) = ('JOHNSON' :: character varying):: text
                ) 
                AND (
                  "left"(
                    "replace"(
                      (b.sos_date):: text, 
                      ('-' :: character varying):: text, 
                      ('' :: character varying):: text
                    ), 
                    4
                  ) > (
                    (
                      (
                        "date_part"(
                          year, 
                          convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
                        ) -2
                      )
                    ):: character varying
                  ):: text
                )
              )
          ) derived_table1 
        WHERE 
          (
            (
              (
                (derived_table1.storereference):: text <> ('Foodland' :: character varying):: text
              ) 
              AND (
                (derived_table1.storereference):: text <> ('Max Valu' :: character varying):: text
              )
            ) 
            AND (
              (derived_table1.storereference):: text <> ('Makro' :: character varying):: text
            )
          )
      ) ,
union_2 as  (
      SELECT 
        derived_table2.dataset, 
        derived_table2.customerid, 
        derived_table2.salespersonid, 
        derived_table2.mustcarryitem, 
        derived_table2.answerscore, 
        derived_table2.presence, 
        derived_table2.outofstock, 
        derived_table2.kpi, 
        derived_table2.scheduleddate, 
        derived_table2.vst_status, 
        derived_table2.fisc_yr, 
        derived_table2.fisc_per, 
        derived_table2.firstname, 
        derived_table2.lastname, 
        derived_table2.customername, 
        derived_table2.country, 
        derived_table2.state, 
        derived_table2.storereference, 
        derived_table2.storetype, 
        derived_table2.channel, 
        derived_table2.salesgroup, 
        derived_table2.eannumber, 
        derived_table2.prod_hier_l1, 
        derived_table2.prod_hier_l2, 
        derived_table2.prod_hier_l3, 
        derived_table2.prod_hier_l4, 
        derived_table2.prod_hier_l5, 
        derived_table2.prod_hier_l6, 
        derived_table2.prod_hier_l7, 
        derived_table2.prod_hier_l8, 
        derived_table2.prod_hier_l9, 
        derived_table2.kpi_chnl_wt, 
        derived_table2."y/n_flag", 
        derived_table2.posm_execution_flag, 
        derived_table2.priority_store_flag, 
        derived_table2.questiontext, 
        derived_table2.ques_desc, 
        derived_table2.value, 
        derived_table2.mkt_share, 
        derived_table2.rej_reason, 
        derived_table2.photo_url 
      FROM 
        (
          SELECT 
            'Adecco' :: character varying AS dataset, 
            COALESCE(
              a.store_code, '' :: character varying
            ) AS customerid, 
            NULL :: character varying AS salespersonid, 
            NULL :: character varying AS mustcarryitem, 
            NULL :: character varying AS answerscore, 
            NULL :: character varying AS presence, 
            NULL :: character varying AS outofstock, 
            'SHARE OF SHELF' :: character varying AS kpi, 
            to_date(
              (
                (
                  to_date(
                    (b.sos_date):: timestamp without time zone
                  )
                ):: character varying
              ):: text, 
              ('YYYY-MM-DD' :: character varying):: text
            ) AS scheduleddate, 
            'COMPLETED' :: character varying AS vst_status, 
            (
              "substring"(
                "replace"(
                  (
                    (
                      to_date(
                        (b.sos_date):: timestamp without time zone
                      )
                    ):: character varying
                  ):: text, 
                  ('-' :: character varying):: text, 
                  ('' :: character varying):: text
                ), 
                0, 
                4
              )
            ):: character varying AS fisc_yr, 
            (
              ltrim(
                (b.yearmo):: text, 
                ('_' :: character varying):: text
              )
            ):: character varying AS fisc_per, 
            b.merchandiser_name AS firstname, 
            NULL :: character varying AS lastname, 
            (
              (
                (
                  (
                    COALESCE(
                      a.store_code, 'NA' :: character varying
                    )
                  ):: text || (' - ' :: character varying):: text
                ) || (
                  COALESCE(
                    a.store_name, 'NA' :: character varying
                  )
                ):: text
              )
            ):: character varying AS customername, 
            'Thailand' :: character varying AS country, 
            a.state, 
            a.customer_name AS storereference, 
            (
              upper(
                (a.retail_environment):: text
              )
            ):: character varying AS storetype, 
            (
              upper(
                (a.channel):: text
              )
            ):: character varying AS channel, 
            NULL :: character varying AS salesgroup, 
            NULL :: character varying AS eannumber, 
            NULL :: character varying AS prod_hier_l1, 
            NULL :: character varying AS prod_hier_l2, 
            b.category AS prod_hier_l3, 
            NULL :: character varying AS prod_hier_l4, 
            NULL :: character varying AS prod_hier_l5, 
            NULL :: character varying AS prod_hier_l6, 
            NULL :: character varying AS prod_hier_l7, 
            NULL :: character varying AS prod_hier_l8, 
            NULL :: character varying AS prod_hier_l9, 
            (wt.weight):: double precision AS kpi_chnl_wt, 
            NULL :: character varying AS "y/n_flag", 
            NULL :: character varying AS posm_execution_flag, 
            'Y' :: character varying AS priority_store_flag, 
            'What is the total shelf facing?' :: character varying AS questiontext, 
            'DENOMINATOR' :: character varying AS ques_desc, 
            CASE WHEN (
              (b.agency):: text = ('Total' :: character varying):: text
            ) THEN b.size ELSE NULL :: character varying END AS value, 
            CASE WHEN (tgt.value IS NULL) THEN (
              (1):: numeric
            ):: numeric(18, 0) ELSE tgt.value END AS mkt_share, 
            NULL :: character varying AS rej_reason, 
            NULL :: character varying AS photo_url 
          FROM 
            (
              (
                (
                  (
                    SELECT 
                      itg_jnj_mer_share_of_shelf.sos_date, 
                      itg_jnj_mer_share_of_shelf.merchandiser_name, 
                      itg_jnj_mer_share_of_shelf.supervisor_name, 
                      itg_jnj_mer_share_of_shelf.area, 
                      itg_jnj_mer_share_of_shelf.channel, 
                      ITG_JNJ_MER_SHARE_OF_SHELF.account, 
                      itg_jnj_mer_share_of_shelf.store_id, 
                      itg_jnj_mer_share_of_shelf.store_name, 
                      itg_jnj_mer_share_of_shelf.category, 
                      itg_jnj_mer_share_of_shelf.agency, 
                      itg_jnj_mer_share_of_shelf.brand, 
                      itg_jnj_mer_share_of_shelf.size, 
                      itg_jnj_mer_share_of_shelf.yearmo 
                    FROM 
                      itg_jnj_mer_share_of_shelf 
                    WHERE 
                      (
                        upper(
                          (
                            itg_jnj_mer_share_of_shelf.agency
                          ):: text
                        ) = ('TOTAL' :: character varying):: text
                      ) 
                    GROUP BY 
                      itg_jnj_mer_share_of_shelf.sos_date, 
                      itg_jnj_mer_share_of_shelf.merchandiser_name, 
                      itg_jnj_mer_share_of_shelf.supervisor_name, 
                      itg_jnj_mer_share_of_shelf.area, 
                      itg_jnj_mer_share_of_shelf.channel, 
                      ITG_JNJ_MER_SHARE_OF_SHELF.account, 
                      itg_jnj_mer_share_of_shelf.store_id, 
                      itg_jnj_mer_share_of_shelf.store_name, 
                      itg_jnj_mer_share_of_shelf.category, 
                      itg_jnj_mer_share_of_shelf.agency, 
                      itg_jnj_mer_share_of_shelf.brand, 
                      itg_jnj_mer_share_of_shelf.size, 
                      itg_jnj_mer_share_of_shelf.yearmo
                  ) b 
                  LEFT JOIN (
                    SELECT 
                      DISTINCT itg_mds_th_ps_store.dataset, 
                      itg_mds_th_ps_store.channel, 
                      itg_mds_th_ps_store.retail_environment, 
                      itg_mds_th_ps_store.state, 
                      itg_mds_th_ps_store.customer_code, 
                      itg_mds_th_ps_store.customer_name, 
                      itg_mds_th_ps_store.store_code, 
                      itg_mds_th_ps_store.store_name 
                    FROM 
                      itg_mds_th_ps_store 
                    WHERE 
                      (
                        (
                          upper(
                            (itg_mds_th_ps_store.dataset):: text
                          ) = ('ADECCO' :: character varying):: text
                        ) 
                        AND (
                          upper(
                            (itg_mds_th_ps_store.channel):: text
                          ) = ('MT' :: character varying):: text
                        )
                      )
                  ) a ON (
                    (
                      upper(
                        (b.store_name):: text
                      ) = upper(
                        (a.store_name):: text
                      )
                    )
                  )
                ) 
                LEFT JOIN (
                  SELECT 
                    edw_vw_ps_weights.market, 
                    edw_vw_ps_weights.kpi, 
                    edw_vw_ps_weights.channel, 
                    edw_vw_ps_weights.retail_environment, 
                    edw_vw_ps_weights.weight 
                  FROM 
                    edw_vw_ps_weights 
                  WHERE 
                    (
                      (
                        upper(
                          (edw_vw_ps_weights.kpi):: text
                        ) = (
                          'SOS COMPLIANCE' :: character varying
                        ):: text
                      ) 
                      AND (
                        upper(
                          (edw_vw_ps_weights.market):: text
                        ) = ('THAILAND' :: character varying):: text
                      )
                    )
                ) wt ON (
                  (
                    (
                      upper(
                        (wt.retail_environment):: text
                      ) = upper(
                        (a.retail_environment):: text
                      )
                    ) 
                    AND (
                      upper(
                        (wt.channel):: text
                      ) = upper(
                        (a.channel):: text
                      )
                    )
                  )
                )
              ) 
              LEFT JOIN (
                SELECT 
                  edw_vw_ps_targets.market, 
                  edw_vw_ps_targets.kpi, 
                  edw_vw_ps_targets.channel, 
                  edw_vw_ps_targets.attribute_1, 
                  edw_vw_ps_targets.retail_environment, 
                  edw_vw_ps_targets.value, 
                  edw_vw_ps_targets.attribute_2 
                FROM 
                  edw_vw_ps_targets 
                WHERE 
                  (
                    (
                      upper(
                        (edw_vw_ps_targets.kpi):: text
                      ) = (
                        'SOS COMPLIANCE' :: character varying
                      ):: text
                    ) 
                    AND (
                      upper(
                        (edw_vw_ps_targets.market):: text
                      ) = ('THAILAND' :: character varying):: text
                    )
                  )
              ) tgt ON (
                (
                  (
                    (
                      (
                        upper(
                          (tgt.attribute_1):: text
                        ) = upper(
                          (b.category):: text
                        )
                      ) 
                      AND (
                        upper(
                          (tgt.retail_environment):: text
                        ) = upper(
                          (a.retail_environment):: text
                        )
                      )
                    ) 
                    AND (
                      upper(
                        (tgt.channel):: text
                      ) = upper(
                        (a.channel):: text
                      )
                    )
                  ) 
                  AND (
                    upper(
                      (tgt.attribute_2):: text
                    ) = upper(
                      (B.account):: text
                    )
                  )
                )
              )
            ) 
          WHERE 
            (
              (
                upper(
                  (b.agency):: text
                ) = ('TOTAL' :: character varying):: text
              ) 
              AND (
                "left"(
                  "replace"(
                    (b.sos_date):: text, 
                    ('-' :: character varying):: text, 
                    ('' :: character varying):: text
                  ), 
                  4
                ) > (
                  (
                    (
                      "date_part"(
                        year, 
                        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
                      ) -2
                    )
                  ):: character varying
                ):: text
              )
            )
        ) derived_table2 
      WHERE 
        (
          (
            (
              (derived_table2.storereference):: text <> ('Foodland' :: character varying):: text
            ) 
            AND (
              (derived_table2.storereference):: text <> ('Max Valu' :: character varying):: text
            )
          ) 
          AND (
            (derived_table2.storereference):: text <> ('Makro' :: character varying):: text
          )
        )
    ) ,
union_3 as  (
       SELECT 
      derived_table3.dataset, 
      derived_table3.customerid, 
      derived_table3.salespersonid, 
      derived_table3.mustcarryitem, 
      derived_table3.answerscore, 
      derived_table3.presence, 
      derived_table3.outofstock, 
      derived_table3.kpi, 
      derived_table3.scheduleddate, 
      derived_table3.vst_status, 
      derived_table3.fisc_yr, 
      derived_table3.fisc_per, 
      derived_table3.firstname, 
      derived_table3.lastname, 
      derived_table3.customername, 
      derived_table3.country, 
      derived_table3.state, 
      derived_table3.storereference, 
      derived_table3.storetype, 
      derived_table3.channel, 
      derived_table3.salesgroup, 
      derived_table3.eannumber, 
      derived_table3.prod_hier_l1, 
      derived_table3.prod_hier_l2, 
      derived_table3.prod_hier_l3, 
      derived_table3.prod_hier_l4, 
      derived_table3.prod_hier_l5, 
      derived_table3.prod_hier_l6, 
      derived_table3.prod_hier_l7, 
      derived_table3.prod_hier_l8, 
      derived_table3.prod_hier_l9, 
      derived_table3.kpi_chnl_wt, 
      derived_table3."y/n_flag", 
      derived_table3.posm_execution_flag, 
      derived_table3.priority_store_flag, 
      derived_table3.questiontext, 
      derived_table3.ques_desc, 
      derived_table3.value, 
      derived_table3.mkt_share, 
      derived_table3.rej_reason, 
      derived_table3.photo_url 
    FROM 
      (
        SELECT 
          'Adecco' :: character varying AS dataset, 
          COALESCE(
            a.store_code, '' :: character varying
          ) AS customerid, 
          b.emp_address_pc AS salespersonid, 
          NULL :: character varying AS mustcarryitem, 
          NULL :: character varying AS answerscore, 
          NULL :: character varying AS presence, 
          NULL :: character varying AS outofstock, 
          d.kpi, 
          to_date(
            (
              (
                to_date(
                  (b.cop_date):: timestamp without time zone
                )
              ):: character varying
            ):: text, 
            ('YYYY-MM-DD' :: character varying):: text
          ) AS scheduleddate, 
          'completed' :: character varying AS vst_status, 
          (
            "substring"(
              "replace"(
                (
                  (
                    to_date(
                      (b.cop_date):: timestamp without time zone
                    )
                  ):: character varying
                ):: text, 
                ('-' :: character varying):: text, 
                ('' :: character varying):: text
              ), 
              0, 
              4
            )
          ):: character varying AS fisc_yr, 
          b.yearmo AS fisc_per, 
          (
            (
              (
                (b.emp_address_pc):: text || (' - ' :: character varying):: text
              ) || (b.pc_name):: text
            )
          ):: character varying AS firstname, 
          '' :: character varying AS lastname, 
          (
            (
              (
                (
                  COALESCE(
                    a.store_code, 'NA' :: character varying
                  )
                ):: text || (' - ' :: character varying):: text
              ) || (
                COALESCE(
                  a.store_name, 'NA' :: character varying
                )
              ):: text
            )
          ):: character varying AS customername, 
          'Thailand' :: character varying AS country, 
          a.state, 
          a.customer_name AS storereference, 
          (
            upper(
              (a.retail_environment):: text
            )
          ):: character varying AS storetype, 
          (
            upper(
              (a.channel):: text
            )
          ):: character varying AS channel, 
          NULL :: character varying AS salesgroup, 
          NULL :: character varying AS eannumber, 
          NULL :: character varying AS prod_hier_l1, 
          NULL :: character varying AS prod_hier_l2, 
          NULL :: character varying AS prod_hier_l3, 
          NULL :: character varying AS prod_hier_l4, 
          NULL :: character varying AS prod_hier_l5, 
          NULL :: character varying AS prod_hier_l6, 
          NULL :: character varying AS prod_hier_l7, 
          NULL :: character varying AS prod_hier_l8, 
          NULL :: character varying AS prod_hier_l9, 
          (wt.weight):: double precision AS kpi_chnl_wt, 
          CASE WHEN (
            (
              (b.answer):: text = ('ใช่' :: character varying):: text
            ) 
            OR (
              (b.answer):: text = ('ได้' :: character varying):: text
            )
          ) THEN 'YES' :: character varying ELSE 'NO' :: character varying END AS "y/n_flag", 
          NULL :: character varying AS posm_execution_flag, 
          'Y' :: character varying AS priority_store_flag, 
          b.question AS questiontext, 
          NULL :: character varying AS ques_desc, 
          NULL :: character varying AS value, 
          CASE WHEN (tgt.value IS NULL) THEN (
            (1):: numeric
          ):: numeric(18, 0) ELSE tgt.value END AS mkt_share, 
          b.answer AS rej_reason, 
          NULL :: character varying AS photo_url 
        FROM 
          (
            (
              (
                (
                  (
                    SELECT 
                      itg_jnj_mer_cop.cop_date, 
                      itg_jnj_mer_cop.emp_address_pc, 
                      itg_jnj_mer_cop.pc_name, 
                      itg_jnj_mer_cop.survey_name, 
                      itg_jnj_mer_cop.emp_address_supervisor, 
                      itg_jnj_mer_cop.supervisor_name, 
                      itg_jnj_mer_cop.cop_priority, 
                      itg_jnj_mer_cop.start_date, 
                      itg_jnj_mer_cop.end_date, 
                      itg_jnj_mer_cop.area, 
                      itg_jnj_mer_cop.channel, 
                      ITG_JNJ_MER_COP.account, 
                      itg_jnj_mer_cop.store_id, 
                      itg_jnj_mer_cop.store_name, 
                      itg_jnj_mer_cop.question, 
                      itg_jnj_mer_cop.answer, 
                      itg_jnj_mer_cop.yearmo 
                    FROM 
                      itg_jnj_mer_cop 
                    GROUP BY 
                      itg_jnj_mer_cop.cop_date, 
                      itg_jnj_mer_cop.emp_address_pc, 
                      itg_jnj_mer_cop.pc_name, 
                      itg_jnj_mer_cop.survey_name, 
                      itg_jnj_mer_cop.emp_address_supervisor, 
                      itg_jnj_mer_cop.supervisor_name, 
                      itg_jnj_mer_cop.cop_priority, 
                      itg_jnj_mer_cop.start_date, 
                      itg_jnj_mer_cop.end_date, 
                      itg_jnj_mer_cop.area, 
                      itg_jnj_mer_cop.channel, 
                      ITG_JNJ_MER_COP.account, 
                      itg_jnj_mer_cop.store_id, 
                      itg_jnj_mer_cop.store_name, 
                      itg_jnj_mer_cop.question, 
                      itg_jnj_mer_cop.answer, 
                      itg_jnj_mer_cop.yearmo
                  ) b 
                  LEFT JOIN (
                    SELECT 
                      DISTINCT itg_mds_th_ps_store.dataset, 
                      itg_mds_th_ps_store.channel, 
                      itg_mds_th_ps_store.retail_environment, 
                      itg_mds_th_ps_store.state, 
                      itg_mds_th_ps_store.customer_code, 
                      itg_mds_th_ps_store.customer_name, 
                      itg_mds_th_ps_store.store_code, 
                      itg_mds_th_ps_store.store_name 
                    FROM 
                      itg_mds_th_ps_store 
                    WHERE 
                      (
                        upper(
                          (itg_mds_th_ps_store.dataset):: text
                        ) = ('ADECCO' :: character varying):: text
                      )
                  ) a ON (
                    (
                      upper(
                        (b.store_name):: text
                      ) = upper(
                        (a.store_name):: text
                      )
                    )
                  )
                ) 
                JOIN itg_th_lookup_question d ON (
                  (
                    (d.question):: text = (b.question):: text
                  )
                )
              ) 
              LEFT JOIN (
                SELECT 
                  edw_vw_ps_weights.market, 
                  edw_vw_ps_weights.kpi, 
                  edw_vw_ps_weights.channel, 
                  edw_vw_ps_weights.retail_environment, 
                  edw_vw_ps_weights.weight 
                FROM 
                  edw_vw_ps_weights 
                WHERE 
                  (
                    upper(
                      (edw_vw_ps_weights.market):: text
                    ) = ('THAILAND' :: character varying):: text
                  )
              ) wt ON (
                (
                  (
                    (
                      upper(
                        (wt.retail_environment):: text
                      ) = upper(
                        (a.retail_environment):: text
                      )
                    ) 
                    AND (
                      upper(
                        (wt.kpi):: text
                      ) = upper(
                        (d.kpi):: text
                      )
                    )
                  ) 
                  AND (
                    upper(
                      (wt.channel):: text
                    ) = upper(
                      (a.channel):: text
                    )
                  )
                )
              )
            ) 
            LEFT JOIN (
              SELECT 
                edw_vw_ps_targets.market, 
                edw_vw_ps_targets.kpi, 
                edw_vw_ps_targets.channel, 
                edw_vw_ps_targets.retail_environment, 
                edw_vw_ps_targets.value 
              FROM 
                edw_vw_ps_targets 
              WHERE 
                (
                  upper(
                    (edw_vw_ps_targets.market):: text
                  ) = ('THAILAND' :: character varying):: text
                )
            ) tgt ON (
              (
                (
                  (
                    upper(
                      (tgt.retail_environment):: text
                    ) = upper(
                      (a.retail_environment):: text
                    )
                  ) 
                  AND (
                    upper(
                      (tgt.kpi):: text
                    ) = upper(
                      (d.kpi):: text
                    )
                  )
                ) 
                AND (
                  upper(
                    (tgt.channel):: text
                  ) = upper(
                    (a.channel):: text
                  )
                )
              )
            )
          ) 
        WHERE 
          (
            "left"(
              "replace"(
                (b.cop_date):: text, 
                ('-' :: character varying):: text, 
                ('' :: character varying):: text
              ), 
              4
            ) > (
              (
                (
                  "date_part"(
                    year, 
                    convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
                  ) -2
                )
              ):: character varying
            ):: text
          )
      ) derived_table3 
    WHERE 
      (
        (
          (
            (derived_table3.storereference):: text <> ('Foodland' :: character varying):: text
          ) 
          AND (
            (derived_table3.storereference):: text <> ('Max Valu' :: character varying):: text
          )
        ) 
        AND (
          (derived_table3.storereference):: text <> ('Makro' :: character varying):: text
        )
      )
  ) ,
union_4 as (
 
  SELECT 
    derived_table4.dataset, 
    derived_table4.customerid, 
    derived_table4.salespersonid, 
    derived_table4.mustcarryitem, 
    derived_table4.answerscore, 
    derived_table4.presence, 
    derived_table4.outofstock, 
    derived_table4.kpi, 
    derived_table4.scheduleddate, 
    derived_table4.vst_status, 
    derived_table4.fisc_yr, 
    derived_table4.fisc_per, 
    derived_table4.firstname, 
    derived_table4.lastname, 
    derived_table4.customername, 
    derived_table4.country, 
    derived_table4.state, 
    derived_table4.storereference, 
    derived_table4.storetype, 
    derived_table4.channel, 
    derived_table4.salesgroup, 
    derived_table4.eannumber, 
    derived_table4.prod_hier_l1, 
    derived_table4.prod_hier_l2, 
    derived_table4.prod_hier_l3, 
    derived_table4.prod_hier_l4, 
    derived_table4.prod_hier_l5, 
    derived_table4.prod_hier_l6, 
    derived_table4.prod_hier_l7, 
    derived_table4.prod_hier_l8, 
    derived_table4.prod_hier_l9, 
    derived_table4.kpi_chnl_wt, 
    derived_table4."y/n_flag", 
    derived_table4.posm_execution_flag, 
    derived_table4.priority_store_flag, 
    derived_table4.questiontext, 
    derived_table4.ques_desc, 
    derived_table4.value, 
    derived_table4.mkt_share, 
    derived_table4.rej_reason, 
    derived_table4.photo_url 
  FROM 
    (
      SELECT 
        'Adecco' :: character varying AS dataset, 
        COALESCE(
          a.store_code, '' :: character varying
        ) AS customerid, 
        NULL :: character varying AS salespersonid, 
        NULL :: character varying AS mustcarryitem, 
        NULL :: character varying AS answerscore, 
        NULL :: character varying AS presence, 
        NULL :: character varying AS outofstock, 
        'SHARE OF SHELF' :: character varying AS kpi, 
        to_date(
          (
            (
              to_date(
                (b.sos_date):: timestamp without time zone
              )
            ):: character varying
          ):: text, 
          ('YYYY-MM-DD' :: character varying):: text
        ) AS scheduleddate, 
        'COMPLETED' :: character varying AS vst_status, 
        (
          "substring"(
            "replace"(
              (
                (
                  to_date(
                    (b.sos_date):: timestamp without time zone
                  )
                ):: character varying
              ):: text, 
              ('-' :: character varying):: text, 
              ('' :: character varying):: text
            ), 
            0, 
            4
          )
        ):: character varying AS fisc_yr, 
        (
          ltrim(
            (b.yearmo):: text, 
            ('_' :: character varying):: text
          )
        ):: character varying AS fisc_per, 
        b.merchandiser_name AS firstname, 
        NULL :: character varying AS lastname, 
        (
          (
            (
              (
                COALESCE(
                  a.store_code, 'NA' :: character varying
                )
              ):: text || (' - ' :: character varying):: text
            ) || (
              COALESCE(
                a.store_name, 'NA' :: character varying
              )
            ):: text
          )
        ):: character varying AS customername, 
        'Thailand' :: character varying AS country, 
        a.state, 
        a.customer_name AS storereference, 
        (
          upper(
            (a.retail_environment):: text
          )
        ):: character varying AS storetype, 
        (
          upper(
            (a.channel):: text
          )
        ):: character varying AS channel, 
        NULL :: character varying AS salesgroup, 
        NULL :: character varying AS eannumber, 
        NULL :: character varying AS prod_hier_l1, 
        NULL :: character varying AS prod_hier_l2, 
        b.category AS prod_hier_l3, 
        NULL :: character varying AS prod_hier_l4, 
        NULL :: character varying AS prod_hier_l5, 
        NULL :: character varying AS prod_hier_l6, 
        NULL :: character varying AS prod_hier_l7, 
        NULL :: character varying AS prod_hier_l8, 
        NULL :: character varying AS prod_hier_l9, 
        (wt.weight):: double precision AS kpi_chnl_wt, 
        NULL :: character varying AS "y/n_flag", 
        NULL :: character varying AS posm_execution_flag, 
        'Y' :: character varying AS priority_store_flag, 
        'What is the J&J facing?' :: character varying AS questiontext, 
        'NUMERATOR' :: character varying AS ques_desc, 
        CASE WHEN (
          (b.agency):: text = ('Johnson' :: character varying):: text
        ) THEN b.size ELSE NULL :: character varying END AS value, 
        CASE WHEN (tgt.value IS NULL) THEN (
          (1):: numeric
        ):: numeric(18, 0) ELSE tgt.value END AS mkt_share, 
        NULL :: character varying AS rej_reason, 
        NULL :: character varying AS photo_url 
      FROM 
        (
          (
            (
              (
                SELECT 
                  itg_jnj_mer_share_of_shelf.sos_date, 
                  itg_jnj_mer_share_of_shelf.merchandiser_name, 
                  itg_jnj_mer_share_of_shelf.supervisor_name, 
                  itg_jnj_mer_share_of_shelf.area, 
                  itg_jnj_mer_share_of_shelf.channel, 
                  ITG_JNJ_MER_SHARE_OF_SHELF.account, 
                  itg_jnj_mer_share_of_shelf.store_id, 
                  itg_jnj_mer_share_of_shelf.store_name, 
                  itg_jnj_mer_share_of_shelf.category, 
                  itg_jnj_mer_share_of_shelf.agency, 
                  itg_jnj_mer_share_of_shelf.brand, 
                  itg_jnj_mer_share_of_shelf.size, 
                  itg_jnj_mer_share_of_shelf.yearmo 
                FROM 
                  itg_jnj_mer_share_of_shelf 
                WHERE 
                  (
                    upper(
                      (
                        itg_jnj_mer_share_of_shelf.agency
                      ):: text
                    ) = ('JOHNSON' :: character varying):: text
                  ) 
                GROUP BY 
                  itg_jnj_mer_share_of_shelf.sos_date, 
                  itg_jnj_mer_share_of_shelf.merchandiser_name, 
                  itg_jnj_mer_share_of_shelf.supervisor_name, 
                  itg_jnj_mer_share_of_shelf.area, 
                  itg_jnj_mer_share_of_shelf.channel, 
                  ITG_JNJ_MER_SHARE_OF_SHELF.account, 
                  itg_jnj_mer_share_of_shelf.store_id, 
                  itg_jnj_mer_share_of_shelf.store_name, 
                  itg_jnj_mer_share_of_shelf.category, 
                  itg_jnj_mer_share_of_shelf.agency, 
                  itg_jnj_mer_share_of_shelf.brand, 
                  itg_jnj_mer_share_of_shelf.size, 
                  itg_jnj_mer_share_of_shelf.yearmo
              ) b 
              LEFT JOIN (
                SELECT 
                  DISTINCT itg_mds_th_ps_store.dataset, 
                  itg_mds_th_ps_store.channel, 
                  itg_mds_th_ps_store.retail_environment, 
                  itg_mds_th_ps_store.state, 
                  itg_mds_th_ps_store.customer_code, 
                  itg_mds_th_ps_store.customer_name, 
                  itg_mds_th_ps_store.store_code, 
                  itg_mds_th_ps_store.store_name 
                FROM 
                  itg_mds_th_ps_store 
                WHERE 
                  (
                    (
                      upper(
                        (itg_mds_th_ps_store.dataset):: text
                      ) = ('ADECCO' :: character varying):: text
                    ) 
                    AND (
                      upper(
                        (itg_mds_th_ps_store.channel):: text
                      ) = ('GT' :: character varying):: text
                    )
                  )
              ) a ON (
                (
                  upper(
                    (b.store_name):: text
                  ) = upper(
                    (a.store_name):: text
                  )
                )
              )
            ) 
            LEFT JOIN (
              SELECT 
                edw_vw_ps_weights.market, 
                edw_vw_ps_weights.kpi, 
                edw_vw_ps_weights.channel, 
                edw_vw_ps_weights.retail_environment, 
                edw_vw_ps_weights.weight 
              FROM 
                edw_vw_ps_weights 
              WHERE 
                (
                  (
                    upper(
                      (edw_vw_ps_weights.kpi):: text
                    ) = (
                      'SOS COMPLIANCE' :: character varying
                    ):: text
                  ) 
                  AND (
                    upper(
                      (edw_vw_ps_weights.market):: text
                    ) = ('THAILAND' :: character varying):: text
                  )
                )
            ) wt ON (
              (
                (
                  upper(
                    (wt.retail_environment):: text
                  ) = upper(
                    (a.retail_environment):: text
                  )
                ) 
                AND (
                  upper(
                    (wt.channel):: text
                  ) = upper(
                    (a.channel):: text
                  )
                )
              )
            )
          ) 
          LEFT JOIN (
            SELECT 
              edw_vw_ps_targets.market, 
              edw_vw_ps_targets.kpi, 
              edw_vw_ps_targets.channel, 
              edw_vw_ps_targets.attribute_1, 
              edw_vw_ps_targets.retail_environment, 
              edw_vw_ps_targets.value 
            FROM 
              edw_vw_ps_targets 
            WHERE 
              (
                (
                  upper(
                    (edw_vw_ps_targets.kpi):: text
                  ) = (
                    'SOS COMPLIANCE' :: character varying
                  ):: text
                ) 
                AND (
                  upper(
                    (edw_vw_ps_targets.market):: text
                  ) = ('THAILAND' :: character varying):: text
                )
              )
          ) tgt ON (
            (
              (
                (
                  upper(
                    (tgt.attribute_1):: text
                  ) = upper(
                    (b.category):: text
                  )
                ) 
                AND (
                  upper(
                    (tgt.retail_environment):: text
                  ) = upper(
                    (a.retail_environment):: text
                  )
                )
              ) 
              AND (
                upper(
                  (tgt.channel):: text
                ) = upper(
                  (a.channel):: text
                )
              )
            )
          )
        ) 
      WHERE 
        (
          (
            upper(
              (b.agency):: text
            ) = ('JOHNSON' :: character varying):: text
          ) 
          AND (
            "left"(
              "replace"(
                (b.sos_date):: text, 
                ('-' :: character varying):: text, 
                ('' :: character varying):: text
              ), 
              4
            ) > (
              (
                (
                  "date_part"(
                    year, 
                    convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
                  ) -2
                )
              ):: character varying
            ):: text
          )
        )
    ) derived_table4 
  WHERE 
    (
      (
        (
          (derived_table4.storereference):: text <> ('Foodland' :: character varying):: text
        ) 
        AND (
          (derived_table4.storereference):: text <> ('Max Valu' :: character varying):: text
        )
      ) 
      AND (
        (derived_table4.storereference):: text <> ('Makro' :: character varying):: text
      )
    )
) ,
union_5 as (SELECT 
  derived_table5.dataset, 
  derived_table5.customerid, 
  derived_table5.salespersonid, 
  derived_table5.mustcarryitem, 
  derived_table5.answerscore, 
  derived_table5.presence, 
  derived_table5.outofstock, 
  derived_table5.kpi, 
  derived_table5.scheduleddate, 
  derived_table5.vst_status, 
  derived_table5.fisc_yr, 
  derived_table5.fisc_per, 
  derived_table5.firstname, 
  derived_table5.lastname, 
  derived_table5.customername, 
  derived_table5.country, 
  derived_table5.state, 
  derived_table5.storereference, 
  derived_table5.storetype, 
  derived_table5.channel, 
  derived_table5.salesgroup, 
  derived_table5.eannumber, 
  derived_table5.prod_hier_l1, 
  derived_table5.prod_hier_l2, 
  derived_table5.prod_hier_l3, 
  derived_table5.prod_hier_l4, 
  derived_table5.prod_hier_l5, 
  derived_table5.prod_hier_l6, 
  derived_table5.prod_hier_l7, 
  derived_table5.prod_hier_l8, 
  derived_table5.prod_hier_l9, 
  derived_table5.kpi_chnl_wt, 
  derived_table5."y/n_flag", 
  derived_table5.posm_execution_flag, 
  derived_table5.priority_store_flag, 
  derived_table5.questiontext, 
  derived_table5.ques_desc, 
  derived_table5.value, 
  derived_table5.mkt_share, 
  derived_table5.rej_reason, 
  derived_table5.photo_url 
FROM 
  (
    SELECT 
      'Adecco' :: character varying AS dataset, 
      COALESCE(
        a.store_code, '' :: character varying
      ) AS customerid, 
      NULL :: character varying AS salespersonid, 
      NULL :: character varying AS mustcarryitem, 
      NULL :: character varying AS answerscore, 
      NULL :: character varying AS presence, 
      NULL :: character varying AS outofstock, 
      'SHARE OF SHELF' :: character varying AS kpi, 
      to_date(
        (
          (
            to_date(
              (b.sos_date):: timestamp without time zone
            )
          ):: character varying
        ):: text, 
        ('YYYY-MM-DD' :: character varying):: text
      ) AS scheduleddate, 
      'COMPLETED' :: character varying AS vst_status, 
      (
        "substring"(
          "replace"(
            (
              (
                to_date(
                  (b.sos_date):: timestamp without time zone
                )
              ):: character varying
            ):: text, 
            ('-' :: character varying):: text, 
            ('' :: character varying):: text
          ), 
          0, 
          4
        )
      ):: character varying AS fisc_yr, 
      (
        ltrim(
          (b.yearmo):: text, 
          ('_' :: character varying):: text
        )
      ):: character varying AS fisc_per, 
      b.merchandiser_name AS firstname, 
      NULL :: character varying AS lastname, 
      (
        (
          (
            (
              COALESCE(
                a.store_code, 'NA' :: character varying
              )
            ):: text || (' - ' :: character varying):: text
          ) || (
            COALESCE(
              a.store_name, 'NA' :: character varying
            )
          ):: text
        )
      ):: character varying AS customername, 
      'Thailand' :: character varying AS country, 
      a.state, 
      a.customer_name AS storereference, 
      (
        upper(
          (a.retail_environment):: text
        )
      ):: character varying AS storetype, 
      (
        upper(
          (a.channel):: text
        )
      ):: character varying AS channel, 
      NULL :: character varying AS salesgroup, 
      NULL :: character varying AS eannumber, 
      NULL :: character varying AS prod_hier_l1, 
      NULL :: character varying AS prod_hier_l2, 
      b.category AS prod_hier_l3, 
      NULL :: character varying AS prod_hier_l4, 
      NULL :: character varying AS prod_hier_l5, 
      NULL :: character varying AS prod_hier_l6, 
      NULL :: character varying AS prod_hier_l7, 
      NULL :: character varying AS prod_hier_l8, 
      NULL :: character varying AS prod_hier_l9, 
      (wt.weight):: double precision AS kpi_chnl_wt, 
      NULL :: character varying AS "y/n_flag", 
      NULL :: character varying AS posm_execution_flag, 
      'Y' :: character varying AS priority_store_flag, 
      'What is the total shelf facing?' :: character varying AS questiontext, 
      'DENOMINATOR' :: character varying AS ques_desc, 
      CASE WHEN (
        (b.agency):: text = ('Total' :: character varying):: text
      ) THEN b.size ELSE NULL :: character varying END AS value, 
      CASE WHEN (tgt.value IS NULL) THEN (
        (1):: numeric
      ):: numeric(18, 0) ELSE tgt.value END AS mkt_share, 
      NULL :: character varying AS rej_reason, 
      NULL :: character varying AS photo_url 
    FROM 
      (
        (
          (
            (
              SELECT 
                itg_jnj_mer_share_of_shelf.sos_date, 
                itg_jnj_mer_share_of_shelf.merchandiser_name, 
                itg_jnj_mer_share_of_shelf.supervisor_name, 
                itg_jnj_mer_share_of_shelf.area, 
                itg_jnj_mer_share_of_shelf.channel, 
                ITG_JNJ_MER_SHARE_OF_SHELF.account, 
                itg_jnj_mer_share_of_shelf.store_id, 
                itg_jnj_mer_share_of_shelf.store_name, 
                itg_jnj_mer_share_of_shelf.category, 
                itg_jnj_mer_share_of_shelf.agency, 
                itg_jnj_mer_share_of_shelf.brand, 
                itg_jnj_mer_share_of_shelf.size, 
                itg_jnj_mer_share_of_shelf.yearmo 
              FROM 
                itg_jnj_mer_share_of_shelf 
              WHERE 
                (
                  upper(
                    (
                      itg_jnj_mer_share_of_shelf.agency
                    ):: text
                  ) = ('TOTAL' :: character varying):: text
                ) 
              GROUP BY 
                itg_jnj_mer_share_of_shelf.sos_date, 
                itg_jnj_mer_share_of_shelf.merchandiser_name, 
                itg_jnj_mer_share_of_shelf.supervisor_name, 
                itg_jnj_mer_share_of_shelf.area, 
                itg_jnj_mer_share_of_shelf.channel, 
                ITG_JNJ_MER_SHARE_OF_SHELF.account, 
                itg_jnj_mer_share_of_shelf.store_id, 
                itg_jnj_mer_share_of_shelf.store_name, 
                itg_jnj_mer_share_of_shelf.category, 
                itg_jnj_mer_share_of_shelf.agency, 
                itg_jnj_mer_share_of_shelf.brand, 
                itg_jnj_mer_share_of_shelf.size, 
                itg_jnj_mer_share_of_shelf.yearmo
            ) b 
            LEFT JOIN (
              SELECT 
                DISTINCT itg_mds_th_ps_store.dataset, 
                itg_mds_th_ps_store.channel, 
                itg_mds_th_ps_store.retail_environment, 
                itg_mds_th_ps_store.state, 
                itg_mds_th_ps_store.customer_code, 
                itg_mds_th_ps_store.customer_name, 
                itg_mds_th_ps_store.store_code, 
                itg_mds_th_ps_store.store_name 
              FROM 
                itg_mds_th_ps_store 
              WHERE 
                (
                  (
                    upper(
                      (itg_mds_th_ps_store.dataset):: text
                    ) = ('ADECCO' :: character varying):: text
                  ) 
                  AND (
                    upper(
                      (itg_mds_th_ps_store.channel):: text
                    ) = ('GT' :: character varying):: text
                  )
                )
            ) a ON (
              (
                upper(
                  (b.store_name):: text
                ) = upper(
                  (a.store_name):: text
                )
              )
            )
          ) 
          LEFT JOIN (
            SELECT 
              edw_vw_ps_weights.market, 
              edw_vw_ps_weights.kpi, 
              edw_vw_ps_weights.channel, 
              edw_vw_ps_weights.retail_environment, 
              edw_vw_ps_weights.weight 
            FROM 
              edw_vw_ps_weights 
            WHERE 
              (
                (
                  upper(
                    (edw_vw_ps_weights.kpi):: text
                  ) = (
                    'SOS COMPLIANCE' :: character varying
                  ):: text
                ) 
                AND (
                  upper(
                    (edw_vw_ps_weights.market):: text
                  ) = ('THAILAND' :: character varying):: text
                )
              )
          ) wt ON (
            (
              (
                upper(
                  (wt.retail_environment):: text
                ) = upper(
                  (a.retail_environment):: text
                )
              ) 
              AND (
                upper(
                  (wt.channel):: text
                ) = upper(
                  (a.channel):: text
                )
              )
            )
          )
        ) 
        LEFT JOIN (
          SELECT 
            edw_vw_ps_targets.market, 
            edw_vw_ps_targets.kpi, 
            edw_vw_ps_targets.channel, 
            edw_vw_ps_targets.attribute_1, 
            edw_vw_ps_targets.retail_environment, 
            edw_vw_ps_targets.value 
          FROM 
            edw_vw_ps_targets 
          WHERE 
            (
              (
                upper(
                  (edw_vw_ps_targets.kpi):: text
                ) = (
                  'SOS COMPLIANCE' :: character varying
                ):: text
              ) 
              AND (
                upper(
                  (edw_vw_ps_targets.market):: text
                ) = ('THAILAND' :: character varying):: text
              )
            )
        ) tgt ON (
          (
            (
              (
                upper(
                  (tgt.attribute_1):: text
                ) = upper(
                  (b.category):: text
                )
              ) 
              AND (
                upper(
                  (tgt.retail_environment):: text
                ) = upper(
                  (a.retail_environment):: text
                )
              )
            ) 
            AND (
              upper(
                (tgt.channel):: text
              ) = upper(
                (a.channel):: text
              )
            )
          )
        )
      ) 
    WHERE 
      (
        (
          upper(
            (b.agency):: text
          ) = ('TOTAL' :: character varying):: text
        ) 
        AND (
          "left"(
            "replace"(
              (b.sos_date):: text, 
              ('-' :: character varying):: text, 
              ('' :: character varying):: text
            ), 
            4
          ) > (
            (
              (
                "date_part"(
                  year, 
                  convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
                ) -2
              )
            ):: character varying
          ):: text
        )
      )
  ) derived_table5 
WHERE 
  (
    (
      (
        (derived_table5.storereference):: text <> ('Foodland' :: character varying):: text
      ) 
      AND (
        (derived_table5.storereference):: text <> ('Max Valu' :: character varying):: text
      )
    ) 
    AND (
      (derived_table5.storereference):: text <> ('Makro' :: character varying):: text
    )
  )
),
final as (
    select * from consumer_reach
    union all
    select * from adecco
    union all
    select * from union_1
    union all
    select * from union_2
    union all
    select * from union_3
    union all
    select * from union_4
    union all
    select * from union_5
)
select * from final