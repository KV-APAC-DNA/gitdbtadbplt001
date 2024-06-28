
{{ config(
  sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
) }}

with edw_vw_ps_targets as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_VW_PS_TARGETS
),
pop6_kpi2data_mapping as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.POP6_KPI2DATA_MAPPING
),
edw_vw_pop6_visits_sku_audits as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_VISITS_SKU_AUDITS
),
edw_vw_pop6_visits_prod_attribute_audits as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.edw_vw_pop6_visits_prod_attribute_audits
),
edw_vw_pop6_visits_display as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_VISITS_DISPLAY
),
edw_vw_pop6_products as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_PRODUCTS
),
edw_vw_pop6_promotions as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_PROMOTIONS
),
edw_vw_pop6_tasks as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_TASKS
),
edw_vw_pop6_visits_general_audits as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_VISITS_GENERAL_AUDITS
),
edw_vw_pop6_salesperson as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_SALESPERSON
),
edw_vw_pop6_store as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_STORE
),
edw_vw_pop6_planned_visits as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_PLANNED_VISITS
),
edw_vw_pop6_visits_rir_data as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_VISITS_RIR_DATA
),
itg_pop6_rir_data as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_POP6_RIR_DATA
),
edw_product_attr_dim as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_PRODUCT_ATTR_DIM
),
itg_pop6_executed_visits as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_POP6_EXECUTED_VISITS
),
itg_pop6_product_lists_pops as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_POP6_PRODUCT_LISTS_POPS
),
itg_pop6_product_lists_products as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_POP6_PRODUCT_LISTS_PRODUCTS
),
sku_audit as ((
                        SELECT 
                          'SKU_AUDIT' AS data_type, 
                          vst.cntry_cd, 
                          vst.visit_id, 
                          NULL :: character varying AS task_group, 
                          NULL :: integer AS task_id, 
                          NULL :: character varying AS task_name, 
                          vst.audit_form_id, 
                          vst.audit_form, 
                          vst.section_id, 
                          vst.section, 
                          NULL :: character varying AS subsection_id, 
                          NULL :: character varying AS subsection, 
                          vst.field_id, 
                          vst.field_code, 
                          vst.field_label, 
                          vst.field_type, 
                          vst.dependent_on_field_id, 
                          vst.sku_id, 
                          vst.sku, 
                          vst.response, 
                          vst.visit_date, 
                          vst.check_in_datetime, 
                          vst.check_out_datetime, 
                          vst.popdb_id, 
                          vst.pop_code, 
                          vst.pop_name, 
                          vst.address, 
                          vst.check_in_longitude, 
                          vst.check_in_latitude, 
                          vst.check_out_longitude, 
                          vst.check_out_latitude, 
                          vst.check_in_photo, 
                          vst.check_out_photo, 
                          vst.username, 
                          vst.user_full_name, 
                          vst.superior_username, 
                          vst.superior_name, 
                          vst.planned_visit, 
                          vst.cancelled_visit, 
                          vst.cancellation_reason, 
                          vst.cancellation_note, 
                          NULL :: character varying AS promotion_plan_id, 
                          NULL :: character varying AS promotion_code, 
                          NULL :: character varying AS promotion_name, 
                          NULL :: character varying AS promotion_mechanics, 
                          NULL :: character varying AS promotion_type, 
                          (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
                          NULL :: character varying AS promotion_compliance, 
                          (NULL :: numeric):: numeric(18, 0) AS actual_price, 
                          NULL :: character varying AS non_compliance_reason, 
                          NULL :: character varying AS photo, 
                          NULL  AS product_attribute_id, 
                          NULL  AS product_attribute, 
                          NULL  AS product_attribute_value_id, 
                          NULL  AS product_attribute_value, 
                          pop.status AS pop_status, 
                          pop.longitude AS pop_longitude, 
                          pop.latitude AS pop_latitude, 
                          pop.country, 
                          pop.channel, 
                          pop.retail_environment_ps, 
                          pop.customer, 
                          pop.sales_group_code, 
                          pop.sales_group_name, 
                          pop.customer_grade, 
                          pop.external_pop_code, 
                          pop.business_unit_name, 
                          pop.territory_or_region, 
                          prd.status AS prod_status, 
                          prd.productdb_id, 
                          prd.barcode, 
                          prd.unit_price, 
                          prd.display_order, 
                          prd.launch_date, 
                          prd.largest_uom_quantity, 
                          prd.middle_uom_quantity, 
                          prd.smallest_uom_quantity, 
                          prd.company, 
                          prd.sku_english, 
                          prd.sku_code, 
                          prd.ps_category, 
                          prd.ps_segment, 
                          prd.ps_category_segment, 
                          prd.country_l1, 
                          prd.regional_franchise_l2, 
                          prd.franchise_l3, 
                          prd.brand_l4, 
                          prd.sub_category_l5, 
                          prd.platform_l6, 
                          prd.variance_l7, 
                          prd.pack_size_l8, 
                          prd.sap_matl_num, 
                          plp.msl_ranking AS msl_rank, 
                          usr.status AS user_status, 
                          usr.userdb_id, 
                          usr.first_name, 
                          usr.last_name, 
                          usr.team, 
                          usr.authorisation_group, 
                          usr.email_address, 
                          usr.longitude AS user_longitude, 
                          usr.latitude AS user_latitude, 
                          NULL  AS display_plan_id, 
                          NULL  AS display_type, 
                          NULL  AS display_code, 
                          NULL  AS display_name, 
                          NULL :: date AS display_start_date, 
                          NULL :: date AS display_end_date, 
                          NULL  AS checklist_method, 
                          NULL :: integer AS display_number, 
                          NULL  AS display_comments, 
                          NULL  AS "y/n_flag", 
                          NULL  AS mkt_share, 
                          NULL  AS planned_visit_date, 
                          NULL  AS visited_flag, 
                          NULL  AS facing, 
                          NULL  AS is_eyelevel 
                        FROM 
                          (
                            (
                              (
                                (
                                  edw_vw_pop6_visits_sku_audits vst 
                                  LEFT JOIN edw_vw_pop6_store pop ON (
                                    (
                                      (vst.popdb_id):: text = (pop.popdb_id):: text
                                    )
                                  )
                                ) 
                                LEFT JOIN edw_vw_pop6_salesperson usr ON (
                                  (
                                    (vst.username):: text = (usr.username):: text
                                  )
                                )
                              ) 
                              LEFT JOIN (
                                SELECT 
                                  p.cntry_cd, 
                                  p.src_file_date, 
                                  p.status, 
                                  p.productdb_id, 
                                  p.barcode, 
                                  p.sku, 
                                  p.unit_price, 
                                  p.display_order, 
                                  p.launch_date, 
                                  p.largest_uom_quantity, 
                                  p.middle_uom_quantity, 
                                  p.smallest_uom_quantity, 
                                  p.company, 
                                  p.sku_english, 
                                  p.sku_code, 
                                  p.ps_category, 
                                  p.ps_segment, 
                                  p.ps_category_segment, 
                                  p.country_l1, 
                                  p.regional_franchise_l2, 
                                  p.franchise_l3, 
                                  p.brand_l4, 
                                  p.sub_category_l5, 
                                  p.platform_l6, 
                                  p.variance_l7, 
                                  p.pack_size_l8, 
                                  pa.sap_matl_num 
                                FROM 
                                  (
                                    edw_vw_pop6_products p 
                                    LEFT JOIN edw_product_attr_dim pa ON (
                                      (
                                        (
                                          (p.barcode):: text = (pa.ean):: text
                                        ) 
                                        AND (
                                          (p.cntry_cd):: text = (pa.cntry):: text
                                        )
                                      )
                                    )
                                  )
                              ) prd ON (
                                (
                                  (vst.sku_id):: text = (prd.productdb_id):: text
                                )
                              )
                            ) 
                            LEFT JOIN (
                              SELECT 
                                DISTINCT plp.cntry_cd, 
                                plp.src_file_date, 
                                plp.product_list, 
                                plp.productdb_id, 
                                plp.sku, 
                                plp.msl_ranking, 
                                plp.prod_grp_date, 
                                plp.effective_from, 
                                plp.effective_to, 
                                plps.popdb_id, 
                                plps.pop_code, 
                                plps.pop_name 
                              FROM 
                                itg_pop6_product_lists_products plp, 
                                itg_pop6_product_lists_pops plps 
                              WHERE 
                                (
                                  (
                                    (plp.product_list):: text = (plps.product_list):: text
                                  ) 
                                  AND (
                                    plp.prod_grp_date = plps.prod_grp_date
                                  )
                                )
                            ) plp ON (
                              (
                                (
                                  (
                                    (vst.sku_id):: text = (plp.productdb_id):: text
                                  ) 
                                  AND (
                                    vst.visit_date = plp.prod_grp_date
                                  )
                                ) 
                                AND (
                                  (vst.popdb_id):: text = (plp.popdb_id):: text
                                )
                              )
                            )
                          ) 
                        UNION 
                        SELECT 
                          'GENERAL_AUDIT' AS data_type, 
                          vst.cntry_cd, 
                          vst.visit_id, 
                          NULL :: character varying AS task_group, 
                          NULL :: integer AS task_id, 
                          NULL :: character varying AS task_name, 
                          vst.audit_form_id, 
                          vst.audit_form, 
                          vst.section_id, 
                          vst.section, 
                          vst.subsection_id, 
                          vst.subsection, 
                          vst.field_id, 
                          vst.field_code, 
                          vst.field_label, 
                          vst.field_type, 
                          vst.dependent_on_field_id, 
                          NULL  AS sku_id, 
                          NULL  AS sku, 
                          vst.response, 
                          vst.visit_date, 
                          vst.check_in_datetime, 
                          vst.check_out_datetime, 
                          vst.popdb_id, 
                          vst.pop_code, 
                          vst.pop_name, 
                          vst.address, 
                          vst.check_in_longitude, 
                          vst.check_in_latitude, 
                          vst.check_out_longitude, 
                          vst.check_out_latitude, 
                          vst.check_in_photo, 
                          vst.check_out_photo, 
                          vst.username, 
                          vst.user_full_name, 
                          vst.superior_username, 
                          vst.superior_name, 
                          vst.planned_visit, 
                          vst.cancelled_visit, 
                          vst.cancellation_reason, 
                          vst.cancellation_note, 
                          NULL :: character varying AS promotion_plan_id, 
                          NULL :: character varying AS promotion_code, 
                          NULL :: character varying AS promotion_name, 
                          NULL :: character varying AS promotion_mechanics, 
                          NULL :: character varying AS promotion_type, 
                          (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
                          NULL :: character varying AS promotion_compliance, 
                          (NULL :: numeric):: numeric(18, 0) AS actual_price, 
                          NULL :: character varying AS non_compliance_reason, 
                          NULL :: character varying AS photo, 
                          NULL  AS product_attribute_id, 
                          NULL  AS product_attribute, 
                          NULL  AS product_attribute_value_id, 
                          NULL  AS product_attribute_value, 
                          pop.status AS pop_status, 
                          pop.longitude AS pop_longitude, 
                          pop.latitude AS pop_latitude, 
                          pop.country, 
                          pop.channel, 
                          pop.retail_environment_ps, 
                          pop.customer, 
                          pop.sales_group_code, 
                          pop.sales_group_name, 
                          pop.customer_grade, 
                          pop.external_pop_code, 
                          pop.business_unit_name, 
                          pop.territory_or_region, 
                          NULL :: integer AS prod_status, 
                          NULL  AS productdb_id, 
                          NULL  AS barcode, 
                          (NULL :: numeric):: numeric(18, 0) AS unit_price, 
                          NULL :: integer AS display_order, 
                          NULL  AS launch_date, 
                          NULL :: integer AS largest_uom_quantity, 
                          NULL :: integer AS middle_uom_quantity, 
                          NULL :: integer AS smallest_uom_quantity, 
                          NULL  AS company, 
                          NULL  AS sku_english, 
                          NULL  AS sku_code, 
                          NULL  AS ps_category, 
                          NULL  AS ps_segment, 
                          NULL  AS ps_category_segment, 
                          NULL  AS country_l1, 
                          NULL  AS regional_franchise_l2, 
                          NULL  AS franchise_l3, 
                          NULL  AS brand_l4, 
                          NULL  AS sub_category_l5, 
                          NULL  AS platform_l6, 
                          NULL  AS variance_l7, 
                          NULL  AS pack_size_l8, 
                          NULL :: character varying AS sap_matl_num, 
                          NULL  AS msl_rank, 
                          usr.status AS user_status, 
                          usr.userdb_id, 
                          usr.first_name, 
                          usr.last_name, 
                          usr.team, 
                          usr.authorisation_group, 
                          usr.email_address, 
                          usr.longitude AS user_longitude, 
                          usr.latitude AS user_latitude, 
                          NULL  AS display_plan_id, 
                          NULL  AS display_type, 
                          NULL  AS display_code, 
                          NULL  AS display_name, 
                          NULL :: date AS display_start_date, 
                          NULL :: date AS display_end_date, 
                          NULL  AS checklist_method, 
                          NULL :: integer AS display_number, 
                          NULL  AS display_comments, 
                          NULL  AS "y/n_flag", 
                          NULL  AS mkt_share, 
                          NULL  AS planned_visit_date, 
                          NULL  AS visited_flag, 
                          NULL  AS facing, 
                          NULL  AS is_eyelevel 
                        FROM 
                          (
                            (
                              edw_vw_pop6_visits_general_audits vst 
                              LEFT JOIN edw_vw_pop6_store pop ON (
                                (
                                  (vst.popdb_id):: text = (pop.popdb_id):: text
                                )
                              )
                            ) 
                            LEFT JOIN edw_vw_pop6_salesperson usr ON (
                              (
                                (vst.username):: text = (usr.username):: text
                              )
                            )
                          )
                      ) ),
tasks as (SELECT 
                        'TASKS' AS data_type, 
                        vst.cntry_cd, 
                        vst.visit_id, 
                        vst.task_group, 
                        vst.task_id, 
                        vst.task_name, 
                        NULL  AS audit_form_id, 
                        NULL  AS audit_form, 
                        NULL  AS section_id, 
                        NULL  AS section, 
                        NULL :: character varying AS subsection_id, 
                        NULL :: character varying AS subsection, 
                        vst.field_id, 
                        vst.field_code, 
                        vst.field_label, 
                        vst.field_type, 
                        vst.dependent_on_field_id, 
                        NULL  AS sku_id, 
                        NULL  AS sku, 
                        vst.response, 
                        vst.visit_date, 
                        vst.check_in_datetime, 
                        vst.check_out_datetime, 
                        vst.popdb_id, 
                        vst.pop_code, 
                        vst.pop_name, 
                        vst.address, 
                        vst.check_in_longitude, 
                        vst.check_in_latitude, 
                        vst.check_out_longitude, 
                        vst.check_out_latitude, 
                        vst.check_in_photo, 
                        vst.check_out_photo, 
                        vst.username, 
                        vst.user_full_name, 
                        vst.superior_username, 
                        vst.superior_name, 
                        vst.planned_visit, 
                        vst.cancelled_visit, 
                        vst.cancellation_reason, 
                        vst.cancellation_note, 
                        NULL :: character varying AS promotion_plan_id, 
                        NULL :: character varying AS promotion_code, 
                        NULL :: character varying AS promotion_name, 
                        NULL :: character varying AS promotion_mechanics, 
                        NULL :: character varying AS promotion_type, 
                        (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
                        NULL :: character varying AS promotion_compliance, 
                        (NULL :: numeric):: numeric(18, 0) AS actual_price, 
                        NULL :: character varying AS non_compliance_reason, 
                        NULL :: character varying AS photo, 
                        NULL  AS product_attribute_id, 
                        NULL  AS product_attribute, 
                        NULL  AS product_attribute_value_id, 
                        NULL  AS product_attribute_value, 
                        pop.status AS pop_status, 
                        pop.longitude AS pop_longitude, 
                        pop.latitude AS pop_latitude, 
                        pop.country, 
                        pop.channel, 
                        pop.retail_environment_ps, 
                        pop.customer, 
                        pop.sales_group_code, 
                        pop.sales_group_name, 
                        pop.customer_grade, 
                        pop.external_pop_code, 
                        pop.business_unit_name, 
                        pop.territory_or_region, 
                        NULL :: integer AS prod_status, 
                        NULL  AS productdb_id, 
                        NULL  AS barcode, 
                        (NULL :: numeric):: numeric(18, 0) AS unit_price, 
                        NULL :: integer AS display_order, 
                        NULL  AS launch_date, 
                        NULL :: integer AS largest_uom_quantity, 
                        NULL :: integer AS middle_uom_quantity, 
                        NULL :: integer AS smallest_uom_quantity, 
                        NULL  AS company, 
                        NULL  AS sku_english, 
                        NULL  AS sku_code, 
                        NULL  AS ps_category, 
                        NULL  AS ps_segment, 
                        NULL  AS ps_category_segment, 
                        NULL  AS country_l1, 
                        NULL  AS regional_franchise_l2, 
                        NULL  AS franchise_l3, 
                        NULL  AS brand_l4, 
                        NULL  AS sub_category_l5, 
                        NULL  AS platform_l6, 
                        NULL  AS variance_l7, 
                        NULL  AS pack_size_l8, 
                        NULL :: character varying AS sap_matl_num, 
                        NULL  AS msl_rank, 
                        usr.status AS user_status, 
                        usr.userdb_id, 
                        usr.first_name, 
                        usr.last_name, 
                        usr.team, 
                        usr.authorisation_group, 
                        usr.email_address, 
                        usr.longitude AS user_longitude, 
                        usr.latitude AS user_latitude, 
                        NULL  AS display_plan_id, 
                        NULL  AS display_type, 
                        NULL  AS display_code, 
                        NULL  AS display_name, 
                        NULL :: date AS display_start_date, 
                        NULL :: date AS display_end_date, 
                        NULL  AS checklist_method, 
                        NULL :: integer AS display_number, 
                        NULL  AS display_comments, 
                        NULL  AS "y/n_flag", 
                        NULL  AS mkt_share, 
                        NULL  AS planned_visit_date, 
                        NULL  AS visited_flag, 
                        NULL  AS facing, 
                        NULL  AS is_eyelevel 
                      FROM 
                        (
                          (
                            edw_vw_pop6_tasks vst 
                            LEFT JOIN edw_vw_pop6_store pop ON (
                              (
                                (vst.popdb_id):: text = (pop.popdb_id):: text
                              )
                            )
                          ) 
                          LEFT JOIN edw_vw_pop6_salesperson usr ON (
                            (
                              (vst.username):: text = (usr.username):: text
                            )
                          )
                        )
                        ),
promotions as (SELECT 
                      'PROMOTIONS' AS data_type, 
                      vst.cntry_cd, 
                      vst.visit_id, 
                      NULL :: character varying AS task_group, 
                      NULL :: integer AS task_id, 
                      NULL :: character varying AS task_name, 
                      NULL  AS audit_form_id, 
                      NULL  AS audit_form, 
                      NULL  AS section_id, 
                      NULL  AS section, 
                      NULL :: character varying AS subsection_id, 
                      NULL :: character varying AS subsection, 
                      NULL  AS field_id, 
                      NULL  AS field_code, 
                      NULL  AS field_label, 
                      NULL  AS field_type, 
                      NULL  AS dependent_on_field_id, 
                      NULL  AS sku_id, 
                      NULL  AS sku, 
                      NULL  AS response, 
                      vst.visit_date, 
                      vst.check_in_datetime, 
                      vst.check_out_datetime, 
                      vst.popdb_id, 
                      vst.pop_code, 
                      vst.pop_name, 
                      vst.address, 
                      vst.check_in_longitude, 
                      vst.check_in_latitude, 
                      vst.check_out_longitude, 
                      vst.check_out_latitude, 
                      vst.check_in_photo, 
                      vst.check_out_photo, 
                      vst.username, 
                      vst.user_full_name, 
                      vst.superior_username, 
                      vst.superior_name, 
                      vst.planned_visit, 
                      vst.cancelled_visit, 
                      vst.cancellation_reason, 
                      vst.cancellation_note, 
                      vst.promotion_plan_id, 
                      vst.promotion_code, 
                      vst.promotion_name, 
                      vst.promotion_mechanics, 
                      vst.promotion_type, 
                      vst.promotion_price, 
                      vst.promotion_compliance, 
                      vst.actual_price, 
                      vst.non_compliance_reason, 
                      vst.photo, 
                      vst.product_attribute_id, 
                      vst.product_attribute, 
                      vst.product_attribute_value_id, 
                      vst.product_attribute_value, 
                      pop.status AS pop_status, 
                      pop.longitude AS pop_longitude, 
                      pop.latitude AS pop_latitude, 
                      pop.country, 
                      pop.channel, 
                      pop.retail_environment_ps, 
                      pop.customer, 
                      pop.sales_group_code, 
                      pop.sales_group_name, 
                      pop.customer_grade, 
                      pop.external_pop_code, 
                      pop.business_unit_name, 
                      pop.territory_or_region, 
                      NULL :: integer AS prod_status, 
                      NULL  AS productdb_id, 
                      NULL  AS barcode, 
                      (NULL :: numeric):: numeric(18, 0) AS unit_price, 
                      NULL :: integer AS display_order, 
                      NULL  AS launch_date, 
                      NULL :: integer AS largest_uom_quantity, 
                      NULL :: integer AS middle_uom_quantity, 
                      NULL :: integer AS smallest_uom_quantity, 
                      NULL  AS company, 
                      NULL  AS sku_english, 
                      NULL  AS sku_code, 
                      prd.ps_category, 
                      prd.ps_segment, 
                      prd.ps_category_segment, 
                      NULL  AS country_l1, 
                      NULL  AS regional_franchise_l2, 
                      NULL  AS franchise_l3, 
                      prd.brand_l4, 
                      NULL  AS sub_category_l5, 
                      NULL  AS platform_l6, 
                      NULL  AS variance_l7, 
                      NULL  AS pack_size_l8, 
                      NULL :: character varying AS sap_matl_num, 
                      NULL  AS msl_rank, 
                      usr.status AS user_status, 
                      usr.userdb_id, 
                      usr.first_name, 
                      usr.last_name, 
                      usr.team, 
                      usr.authorisation_group, 
                      usr.email_address, 
                      usr.longitude AS user_longitude, 
                      usr.latitude AS user_latitude, 
                      NULL  AS display_plan_id, 
                      NULL  AS display_type, 
                      NULL  AS display_code, 
                      NULL  AS display_name, 
                      NULL :: date AS display_start_date, 
                      NULL :: date AS display_end_date, 
                      NULL  AS checklist_method, 
                      NULL :: integer AS display_number, 
                      NULL  AS display_comments, 
                      NULL  AS "y/n_flag", 
                      NULL  AS mkt_share, 
                      NULL  AS planned_visit_date, 
                      NULL  AS visited_flag, 
                      NULL  AS facing, 
                      NULL  AS is_eyelevel 
                    FROM 
                      (
                        (
                          (
                            edw_vw_pop6_promotions vst 
                            LEFT JOIN edw_vw_pop6_store pop ON (
                              (
                                (vst.popdb_id):: text = (pop.popdb_id):: text
                              )
                            )
                          ) 
                          LEFT JOIN edw_vw_pop6_salesperson usr ON (
                            (
                              (vst.username):: text = (usr.username):: text
                            )
                          )
                        ) 
                        LEFT JOIN (
                          SELECT 
                            DISTINCT p.brand_l4, 
                            p.ps_category, 
                            p.ps_segment, 
                            p.ps_category_segment, 
                            (
                              (
                                (p.ps_category):: text || ('_' :: character varying):: text
                              ) || (p.ps_segment):: text
                            ) AS ps_categorysegement 
                          FROM 
                            edw_vw_pop6_products p
                        ) prd ON (
                          CASE WHEN (
                            (vst.product_attribute):: text = (
                              'PS Category Segment' :: character varying
                            ):: text
                          ) THEN (
                            (vst.product_attribute_value):: text = prd.ps_categorysegement
                          ) ELSE (
                            (vst.product_attribute_value):: text = (prd.brand_l4):: text
                          ) END
                        )
                      )),
product_attribute_audit as  (
                 
                  SELECT 
                    srv.data_type, 
                    srv.cntry_cd, 
                    srv.visit_id, 
                    srv.task_group, 
                    srv.task_id, 
                    srv.task_name, 
                    srv.audit_form_id, 
                    srv.audit_form, 
                    srv.section_id, 
                    srv.section, 
                    srv.subsection_id, 
                    srv.subsection, 
                    srv.field_id, 
                    srv.field_code, 
                    srv.field_label, 
                    srv.field_type, 
                    srv.dependent_on_field_id, 
                    srv.sku_id, 
                    srv.sku, 
                    srv.response, 
                    srv.visit_date, 
                    srv.check_in_datetime, 
                    srv.check_out_datetime, 
                    srv.popdb_id, 
                    srv.pop_code, 
                    srv.pop_name, 
                    srv.address, 
                    srv.check_in_longitude, 
                    srv.check_in_latitude, 
                    srv.check_out_longitude, 
                    srv.check_out_latitude, 
                    srv.check_in_photo, 
                    srv.check_out_photo, 
                    srv.username, 
                    srv.user_full_name, 
                    srv.superior_username, 
                    srv.superior_name, 
                    srv.planned_visit, 
                    srv.cancelled_visit, 
                    srv.cancellation_reason, 
                    srv.cancellation_note, 
                    srv.promotion_plan_id, 
                    srv.promotion_code, 
                    srv.promotion_name, 
                    srv.promotion_mechanics, 
                    srv.promotion_type, 
                    srv.promotion_price, 
                    srv.promotion_compliance, 
                    srv.actual_price, 
                    srv.non_compliance_reason, 
                    srv.photo, 
                    srv.product_attribute_id, 
                    srv.product_attribute, 
                    srv.product_attribute_value_id, 
                    srv.product_attribute_value, 
                    srv.pop_status, 
                    srv.pop_longitude, 
                    srv.pop_latitude, 
                    srv.country, 
                    srv.channel, 
                    srv.retail_environment_ps, 
                    srv.customer, 
                    srv.sales_group_code, 
                    srv.sales_group_name, 
                    srv.customer_grade, 
                    srv.external_pop_code, 
                    srv.business_unit_name, 
                    srv.territory_or_region, 
                    srv.prod_status, 
                    srv.productdb_id, 
                    srv.barcode, 
                    srv.unit_price, 
                    srv.display_order, 
                    srv.launch_date, 
                    srv.largest_uom_quantity, 
                    srv.middle_uom_quantity, 
                    srv.smallest_uom_quantity, 
                    srv.company, 
                    srv.sku_english, 
                    srv.sku_code, 
                    srv.ps_category, 
                    srv.ps_segment, 
                    srv.ps_category_segment, 
                    srv.country_l1, 
                    srv.regional_franchise_l2, 
                    srv.franchise_l3, 
                    srv.brand_l4, 
                    srv.sub_category_l5, 
                    srv.platform_l6, 
                    srv.variance_l7, 
                    srv.pack_size_l8, 
                    srv.sap_matl_num, 
                    srv.msl_rank, 
                    srv.user_status, 
                    srv.userdb_id, 
                    srv.first_name, 
                    srv.last_name, 
                    srv.team, 
                    srv.authorisation_group, 
                    srv.email_address, 
                    srv.user_longitude, 
                    srv.user_latitude, 
                    srv.display_plan_id, 
                    srv.display_type, 
                    srv.display_code, 
                    srv.display_name, 
                    srv.display_start_date, 
                    srv.display_end_date, 
                    srv.checklist_method, 
                    srv.display_number, 
                    srv.display_comments, 
                    flag.value AS "y/n_flag", 
                    mkt_shr.mkt_share, 
                    NULL AS planned_visit_date, 
                    NULL AS visited_flag, 
                    NULL AS facing, 
                    NULL AS is_eyelevel 
                  FROM 
                    (
                      (
                        (
                          SELECT 
                            'PRODUCT_ATTIBUTE_AUDIT' :: character varying AS data_type, 
                            vst.cntry_cd, 
                            vst.visit_id, 
                            NULL :: character varying AS task_group, 
                            NULL :: integer AS task_id, 
                            NULL :: character varying AS task_name, 
                            vst.audit_form_id, 
                            vst.audit_form, 
                            vst.section_id, 
                            vst.section, 
                            NULL :: character varying AS subsection_id, 
                            NULL :: character varying AS subsection, 
                            vst.field_id, 
                            vst.field_code, 
                            vst.field_label, 
                            vst.field_type, 
                            vst.dependent_on_field_id, 
                            NULL :: character varying AS sku_id, 
                            NULL :: character varying AS sku, 
                            vst.response, 
                            vst.visit_date, 
                            vst.check_in_datetime, 
                            vst.check_out_datetime, 
                            vst.popdb_id, 
                            vst.pop_code, 
                            vst.pop_name, 
                            vst.address, 
                            vst.check_in_longitude, 
                            vst.check_in_latitude, 
                            vst.check_out_longitude, 
                            vst.check_out_latitude, 
                            vst.check_in_photo, 
                            vst.check_out_photo, 
                            vst.username, 
                            vst.user_full_name, 
                            vst.superior_username, 
                            vst.superior_name, 
                            vst.planned_visit, 
                            vst.cancelled_visit, 
                            vst.cancellation_reason, 
                            vst.cancellation_note, 
                            NULL :: character varying AS promotion_plan_id, 
                            NULL :: character varying AS promotion_code, 
                            NULL :: character varying AS promotion_name, 
                            NULL :: character varying AS promotion_mechanics, 
                            NULL :: character varying AS promotion_type, 
                            (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
                            NULL :: character varying AS promotion_compliance, 
                            (NULL :: numeric):: numeric(18, 0) AS actual_price, 
                            NULL :: character varying AS non_compliance_reason, 
                            NULL :: character varying AS photo, 
                            vst.product_attribute_id, 
                            vst.product_attribute, 
                            vst.product_attribute_value_id, 
                            vst.product_attribute_value, 
                            pop.status AS pop_status, 
                            pop.longitude AS pop_longitude, 
                            pop.latitude AS pop_latitude, 
                            pop.country, 
                            pop.channel, 
                            pop.retail_environment_ps, 
                            pop.customer, 
                            pop.sales_group_code, 
                            pop.sales_group_name, 
                            pop.customer_grade, 
                            pop.external_pop_code, 
                            pop.business_unit_name, 
                            pop.territory_or_region, 
                            NULL :: integer AS prod_status, 
                            NULL :: character varying AS productdb_id, 
                            NULL :: character varying AS barcode, 
                            (NULL :: numeric):: numeric(18, 0) AS unit_price, 
                            NULL :: integer AS display_order, 
                            NULL :: character varying AS launch_date, 
                            NULL :: integer AS largest_uom_quantity, 
                            NULL :: integer AS middle_uom_quantity, 
                            NULL :: integer AS smallest_uom_quantity, 
                            NULL :: character varying AS company, 
                            NULL :: character varying AS sku_english, 
                            NULL :: character varying AS sku_code, 
                            prd.ps_category, 
                            prd.ps_segment, 
                            prd.ps_category_segment, 
                            NULL :: character varying AS country_l1, 
                            NULL :: character varying AS regional_franchise_l2, 
                            NULL :: character varying AS franchise_l3, 
                            prd.brand_l4, 
                            NULL :: character varying AS sub_category_l5, 
                            NULL :: character varying AS platform_l6, 
                            NULL :: character varying AS variance_l7, 
                            NULL :: character varying AS pack_size_l8, 
                            NULL :: character varying AS sap_matl_num, 
                            NULL :: character varying AS msl_rank, 
                            usr.status AS user_status, 
                            usr.userdb_id, 
                            usr.first_name, 
                            usr.last_name, 
                            usr.team, 
                            usr.authorisation_group, 
                            usr.email_address, 
                            usr.longitude AS user_longitude, 
                            usr.latitude AS user_latitude, 
                            NULL :: character varying AS display_plan_id, 
                            NULL :: character varying AS display_type, 
                            NULL :: character varying AS display_code, 
                            NULL :: character varying AS display_name, 
                            NULL :: date AS display_start_date, 
                            NULL :: date AS display_end_date, 
                            NULL :: character varying AS checklist_method, 
                            NULL :: integer AS display_number, 
                            NULL :: character varying AS display_comments 
                          FROM 
                            (
                              SELECT 
                                pop6_kpi2data_mapping.ctry, 
                                pop6_kpi2data_mapping.identifier, 
                                pop6_kpi2data_mapping.kpi_name 
                              FROM 
                                pop6_kpi2data_mapping 
                              WHERE 
                                (
                                  (
                                    pop6_kpi2data_mapping.data_type
                                  ):: text = ('KPI' :: character varying):: text
                                )
                            ) mapp, 
                            (
                              (
                                (
                                  edw_vw_pop6_visits_prod_attribute_audits vst 
                                  LEFT JOIN edw_vw_pop6_store pop ON (
                                    (
                                      (vst.popdb_id):: text = (pop.popdb_id):: text
                                    )
                                  )
                                ) 
                                LEFT JOIN edw_vw_pop6_salesperson usr ON (
                                  (
                                    (vst.username):: text = (usr.username):: text
                                  )
                                )
                              ) 
                              LEFT JOIN (
                                SELECT 
                                  DISTINCT p.brand_l4, 
                                  p.ps_category, 
                                  p.ps_segment, 
                                  p.ps_category_segment, 
                                  (
                                    (
                                      (p.ps_category):: text || ('_' :: character varying):: text
                                    ) || (p.ps_segment):: text
                                  ) AS ps_categorysegement 
                                FROM 
                                  edw_vw_pop6_products p
                              ) prd ON (
                                CASE WHEN (
                                  (vst.product_attribute):: text = (
                                    'PS Category Segment' :: character varying
                                  ):: text
                                ) THEN (
                                  (vst.product_attribute_value):: text = prd.ps_categorysegement
                                ) ELSE (
                                  (vst.product_attribute_value):: text = (prd.brand_l4):: text
                                ) END
                              )
                            ) 
                          WHERE 
                            (
                              (pop.country):: text = (mapp.ctry):: text
                            )
                        ) srv 
                        LEFT JOIN (
                          SELECT 
                            pop6_kpi2data_mapping.ctry, 
                            pop6_kpi2data_mapping.data_type, 
                            pop6_kpi2data_mapping.identifier, 
                            pop6_kpi2data_mapping.value, 
                            pop6_kpi2data_mapping.kpi_name, 
                            pop6_kpi2data_mapping.start_date, 
                            pop6_kpi2data_mapping.end_date 
                          FROM 
                            pop6_kpi2data_mapping 
                          WHERE 
                            (
                              (
                                pop6_kpi2data_mapping.data_type
                              ):: text = (
                                'Yes/No Flag' :: character varying
                              ):: text
                            )
                        ) flag ON (
                          (
                            (
                              (
                                (
                                  (flag.ctry):: text = (srv.country):: text
                                ) 
                                AND (
                                  upper(
                                    (srv.response):: text
                                  ) like (
                                    (
                                      ('%' :: character varying):: text || (flag.identifier):: text
                                    ) || ('%' :: character varying):: text
                                  )
                                )
                              ) 
                              AND (
                                srv.visit_date >= flag.start_date
                              )
                            ) 
                            AND (srv.visit_date <= flag.end_date)
                          )
                        )
                      ) 
                      LEFT JOIN (
                        SELECT 
                          pop6_kpi2data_mapping.ctry, 
                          pop6_kpi2data_mapping.data_type, 
                          pop6_kpi2data_mapping.identifier AS channel, 
                          pop6_kpi2data_mapping.kpi_name, 
                          pop6_kpi2data_mapping.store_type, 
                          pop6_kpi2data_mapping.category, 
                          pop6_kpi2data_mapping.segment, 
                          (pop6_kpi2data_mapping.value):: double precision AS mkt_share 
                        FROM 
                          pop6_kpi2data_mapping 
                        WHERE 
                          (
                            (
                              pop6_kpi2data_mapping.data_type
                            ):: text = (
                              'Market Share' :: character varying
                            ):: text
                          )
                      ) mkt_shr ON (
                        (
                          (
                            (
                              (
                                (
                                  (mkt_shr.ctry):: text = (srv.country):: text
                                ) 
                                AND (
                                  (mkt_shr.channel):: text = (srv.channel):: text
                                )
                              ) 
                              AND (
                                upper(
                                  (mkt_shr.store_type):: text
                                ) = upper(
                                  (srv.retail_environment_ps):: text
                                )
                              )
                            ) 
                            AND (
                              upper(
                                (mkt_shr.category):: text
                              ) = upper(
                                (srv.ps_category):: text
                              )
                            )
                          ) 
                          AND (
                            upper(
                              (mkt_shr.segment):: text
                            ) = upper(
                              (srv.ps_segment):: text
                            )
                          )
                        )
                      )
                    )
                ) ,
 display  as  (
               
                SELECT 
                  srv.data_type, 
                  srv.cntry_cd, 
                  srv.visit_id, 
                  srv.task_group, 
                  srv.task_id, 
                  srv.task_name, 
                  srv.audit_form_id, 
                  srv.audit_form, 
                  srv.section_id, 
                  srv.section, 
                  srv.subsection_id, 
                  srv.subsection, 
                  srv.field_id, 
                  srv.field_code, 
                  srv.questiontext AS field_label, 
                  srv.field_type, 
                  srv.dependent_on_field_id, 
                  srv.sku_id, 
                  srv.sku, 
                  srv.response, 
                  srv.visit_date, 
                  srv.check_in_datetime, 
                  srv.check_out_datetime, 
                  srv.popdb_id, 
                  srv.pop_code, 
                  srv.pop_name, 
                  srv.address, 
                  srv.check_in_longitude, 
                  srv.check_in_latitude, 
                  srv.check_out_longitude, 
                  srv.check_out_latitude, 
                  srv.check_in_photo, 
                  srv.check_out_photo, 
                  srv.username, 
                  srv.user_full_name, 
                  srv.superior_username, 
                  srv.superior_name, 
                  srv.planned_visit, 
                  srv.cancelled_visit, 
                  srv.cancellation_reason, 
                  srv.cancellation_note, 
                  srv.promotion_plan_id, 
                  srv.promotion_code, 
                  srv.promotion_name, 
                  srv.promotion_mechanics, 
                  srv.promotion_type, 
                  srv.promotion_price, 
                  srv.promotion_compliance, 
                  srv.actual_price, 
                  srv.non_compliance_reason, 
                  srv.photo, 
                  srv.product_attribute_id, 
                  srv.product_attribute, 
                  srv.product_attribute_value_id, 
                  srv.product_attribute_value, 
                  srv.pop_status, 
                  srv.pop_longitude, 
                  srv.pop_latitude, 
                  srv.country, 
                  srv.channel, 
                  srv.retail_environment_ps, 
                  srv.customer, 
                  srv.sales_group_code, 
                  srv.sales_group_name, 
                  srv.customer_grade, 
                  srv.external_pop_code, 
                  srv.business_unit_name, 
                  srv.territory_or_region, 
                  srv.prod_status, 
                  srv.productdb_id, 
                  srv.barcode, 
                  srv.unit_price, 
                  srv.display_order, 
                  srv.launch_date, 
                  srv.largest_uom_quantity, 
                  srv.middle_uom_quantity, 
                  srv.smallest_uom_quantity, 
                  srv.company, 
                  srv.sku_english, 
                  srv.sku_code, 
                  srv.ps_category, 
                  srv.ps_segment, 
                  srv.ps_category_segment, 
                  srv.country_l1, 
                  srv.regional_franchise_l2, 
                  srv.franchise_l3, 
                  srv.brand_l4, 
                  srv.sub_category_l5, 
                  srv.platform_l6, 
                  srv.variance_l7, 
                  srv.pack_size_l8, 
                  srv.sap_matl_num, 
                  srv.msl_rank, 
                  srv.user_status, 
                  srv.userdb_id, 
                  srv.first_name, 
                  srv.last_name, 
                  srv.team, 
                  srv.authorisation_group, 
                  srv.email_address, 
                  srv.user_longitude, 
                  srv.user_latitude, 
                  srv.display_plan_id, 
                  srv.display_type, 
                  srv.display_code, 
                  srv.display_name, 
                  srv.display_start_date, 
                  srv.display_end_date, 
                  srv.checklist_method, 
                  srv.display_number, 
                  srv.display_comments, 
                  flag.value AS "y/n_flag", 
                  mkt_shr.mkt_share, 
                  NULL AS planned_visit_date, 
                  NULL AS visited_flag, 
                  NULL AS facing, 
                  NULL AS is_eyelevel 
                FROM 
                  (
                    (
                      (
                        SELECT 
                          'DISPLAY' :: character varying AS data_type, 
                          vst.cntry_cd, 
                          vst.visit_id, 
                          NULL :: character varying AS task_group, 
                          NULL :: integer AS task_id, 
                          NULL :: character varying AS task_name, 
                          NULL :: character varying AS audit_form_id, 
                          NULL :: character varying AS audit_form, 
                          NULL :: character varying AS section_id, 
                          NULL :: character varying AS section, 
                          NULL :: character varying AS subsection_id, 
                          NULL :: character varying AS subsection, 
                          vst.field_id, 
                          vst.field_code, 
                          vst.field_label AS questiontext, 
                          vst.field_type, 
                          vst.dependent_on_field_id, 
                          NULL :: character varying AS sku_id, 
                          NULL :: character varying AS sku, 
                          vst.response, 
                          vst.visit_date, 
                          vst.check_in_datetime, 
                          vst.check_out_datetime, 
                          vst.popdb_id, 
                          vst.pop_code, 
                          vst.pop_name, 
                          vst.address, 
                          vst.check_in_longitude, 
                          vst.check_in_latitude, 
                          vst.check_out_longitude, 
                          vst.check_out_latitude, 
                          vst.check_in_photo, 
                          vst.check_out_photo, 
                          vst.username, 
                          vst.user_full_name, 
                          vst.superior_username, 
                          vst.superior_name, 
                          vst.planned_visit, 
                          vst.cancelled_visit, 
                          vst.cancellation_reason, 
                          vst.cancellation_note, 
                          NULL :: character varying AS promotion_plan_id, 
                          NULL :: character varying AS promotion_code, 
                          NULL :: character varying AS promotion_name, 
                          NULL :: character varying AS promotion_mechanics, 
                          NULL :: character varying AS promotion_type, 
                          (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
                          NULL :: character varying AS promotion_compliance, 
                          (NULL :: numeric):: numeric(18, 0) AS actual_price, 
                          NULL :: character varying AS non_compliance_reason, 
                          NULL :: character varying AS photo, 
                          vst.product_attribute_id, 
                          vst.product_attribute, 
                          vst.product_attribute_value_id, 
                          vst.product_attribute_value, 
                          pop.status AS pop_status, 
                          pop.longitude AS pop_longitude, 
                          pop.latitude AS pop_latitude, 
                          pop.country, 
                          pop.channel, 
                          pop.retail_environment_ps, 
                          pop.customer, 
                          pop.sales_group_code, 
                          pop.sales_group_name, 
                          pop.customer_grade, 
                          pop.external_pop_code, 
                          pop.business_unit_name, 
                          pop.territory_or_region, 
                          NULL :: integer AS prod_status, 
                          NULL :: character varying AS productdb_id, 
                          NULL :: character varying AS barcode, 
                          (NULL :: numeric):: numeric(18, 0) AS unit_price, 
                          NULL :: integer AS display_order, 
                          NULL :: character varying AS launch_date, 
                          NULL :: integer AS largest_uom_quantity, 
                          NULL :: integer AS middle_uom_quantity, 
                          NULL :: integer AS smallest_uom_quantity, 
                          NULL :: character varying AS company, 
                          NULL :: character varying AS sku_english, 
                          NULL :: character varying AS sku_code, 
                          prd.ps_category, 
                          prd.ps_segment, 
                          prd.ps_category_segment, 
                          NULL :: character varying AS country_l1, 
                          NULL :: character varying AS regional_franchise_l2, 
                          NULL :: character varying AS franchise_l3, 
                          prd.brand_l4, 
                          NULL :: character varying AS sub_category_l5, 
                          NULL :: character varying AS platform_l6, 
                          NULL :: character varying AS variance_l7, 
                          NULL :: character varying AS pack_size_l8, 
                          NULL :: character varying AS sap_matl_num, 
                          NULL :: character varying AS msl_rank, 
                          usr.status AS user_status, 
                          usr.userdb_id, 
                          usr.first_name, 
                          usr.last_name, 
                          usr.team, 
                          usr.authorisation_group, 
                          usr.email_address, 
                          usr.longitude AS user_longitude, 
                          usr.latitude AS user_latitude, 
                          vst.display_plan_id, 
                          vst.display_type, 
                          vst.display_code, 
                          vst.display_name, 
                          vst.start_date AS display_start_date, 
                          vst.end_date AS display_end_date, 
                          vst.checklist_method, 
                          vst.display_number, 
                          vst.comments AS display_comments 
                        FROM 
                          (
                            SELECT 
                              DISTINCT pop6_kpi2data_mapping.ctry, 
                              pop6_kpi2data_mapping.identifier, 
                              pop6_kpi2data_mapping.kpi_name 
                            FROM 
                              pop6_kpi2data_mapping 
                            WHERE 
                              (
                                (
                                  pop6_kpi2data_mapping.data_type
                                ):: text = ('KPI' :: character varying):: text
                              )
                          ) mapp, 
                          (
                            (
                              (
                                edw_vw_pop6_visits_display vst 
                                LEFT JOIN edw_vw_pop6_store pop ON (
                                  (
                                    (vst.popdb_id):: text = (pop.popdb_id):: text
                                  )
                                )
                              ) 
                              LEFT JOIN edw_vw_pop6_salesperson usr ON (
                                (
                                  (vst.username):: text = (usr.username):: text
                                )
                              )
                            ) 
                            LEFT JOIN (
                              SELECT 
                                DISTINCT p.brand_l4, 
                                p.ps_category, 
                                p.ps_segment, 
                                p.ps_category_segment, 
                                (
                                  (
                                    (p.ps_category):: text || ('_' :: character varying):: text
                                  ) || (p.ps_segment):: text
                                ) AS ps_categorysegement 
                              FROM 
                                edw_vw_pop6_products p
                            ) prd ON (
                              CASE WHEN (
                                (vst.product_attribute):: text = (
                                  'PS Category Segment' :: character varying
                                ):: text
                              ) THEN (
                                (vst.product_attribute_value):: text = prd.ps_categorysegement
                              ) ELSE (
                                (vst.product_attribute_value):: text = (prd.brand_l4):: text
                              ) END
                            )
                          ) 
                        WHERE 
                          (
                            (pop.country):: text = (mapp.ctry):: text
                          )
                      ) srv 
                      LEFT JOIN (
                        SELECT 
                          pop6_kpi2data_mapping.ctry, 
                          pop6_kpi2data_mapping.data_type, 
                          pop6_kpi2data_mapping.identifier, 
                          pop6_kpi2data_mapping.value, 
                          pop6_kpi2data_mapping.kpi_name, 
                          pop6_kpi2data_mapping.start_date, 
                          pop6_kpi2data_mapping.end_date 
                        FROM 
                          pop6_kpi2data_mapping 
                        WHERE 
                          (
                            (
                              pop6_kpi2data_mapping.data_type
                            ):: text = (
                              'Yes/No Flag' :: character varying
                            ):: text
                          )
                      ) flag ON (
                        (
                          (
                            (
                              (
                                (flag.ctry):: text = (srv.country):: text
                              ) 
                              AND (
                                upper(
                                  (srv.response):: text
                                ) like (
                                  (
                                    ('%' :: character varying):: text || (flag.identifier):: text
                                  ) || ('%' :: character varying):: text
                                )
                              )
                            ) 
                            AND (
                              srv.visit_date >= flag.start_date
                            )
                          ) 
                          AND (srv.visit_date <= flag.end_date)
                        )
                      )
                    ) 
                    LEFT JOIN (
                      SELECT 
                        pop6_kpi2data_mapping.ctry, 
                        pop6_kpi2data_mapping.data_type, 
                        pop6_kpi2data_mapping.identifier AS channel, 
                        pop6_kpi2data_mapping.kpi_name, 
                        pop6_kpi2data_mapping.store_type, 
                        pop6_kpi2data_mapping.category, 
                        pop6_kpi2data_mapping.segment, 
                        (pop6_kpi2data_mapping.value):: double precision AS mkt_share 
                      FROM 
                        pop6_kpi2data_mapping 
                      WHERE 
                        (
                          (
                            pop6_kpi2data_mapping.data_type
                          ):: text = (
                            'Market Share' :: character varying
                          ):: text
                        )
                    ) mkt_shr ON (
                      (
                        (
                          (
                            (
                              (
                                (mkt_shr.ctry):: text = (srv.country):: text
                              ) 
                              AND (
                                (mkt_shr.channel):: text = (srv.channel):: text
                              )
                            ) 
                            AND (
                              upper(
                                (mkt_shr.store_type):: text
                              ) = upper(
                                (srv.retail_environment_ps):: text
                              )
                            )
                          ) 
                          AND (
                            upper(
                              (mkt_shr.category):: text
                            ) = upper(
                              (srv.ps_category):: text
                            )
                          )
                        ) 
                        AND (
                          upper(
                            (mkt_shr.segment):: text
                          ) = upper(
                            (srv.ps_segment):: text
                          )
                        )
                      )
                    )
                  )
              ) ,
PRODUCT_ATTRIBUTE_AUDIT_2 as (
             
              SELECT 
                srv.data_type, 
                srv.cntry_cd, 
                srv.visit_id, 
                srv.task_group, 
                srv.task_id, 
                srv.task_name, 
                srv.audit_form_id, 
                srv.audit_form, 
                srv.section_id, 
                srv.section, 
                srv.subsection_id, 
                srv.subsection, 
                srv.field_id, 
                srv.field_code, 
                srv.field_label, 
                srv.field_type, 
                srv.dependent_on_field_id, 
                srv.sku_id, 
                srv.sku, 
                srv.response, 
                srv.visit_date, 
                srv.check_in_datetime, 
                srv.check_out_datetime, 
                srv.popdb_id, 
                srv.pop_code, 
                srv.pop_name, 
                srv.address, 
                srv.check_in_longitude, 
                srv.check_in_latitude, 
                srv.check_out_longitude, 
                srv.check_out_latitude, 
                srv.check_in_photo, 
                srv.check_out_photo, 
                srv.username, 
                srv.user_full_name, 
                srv.superior_username, 
                srv.superior_name, 
                srv.planned_visit, 
                srv.cancelled_visit, 
                srv.cancellation_reason, 
                srv.cancellation_note, 
                srv.promotion_plan_id, 
                srv.promotion_code, 
                srv.promotion_name, 
                srv.promotion_mechanics, 
                srv.promotion_type, 
                srv.promotion_price, 
                srv.promotion_compliance, 
                srv.actual_price, 
                srv.non_compliance_reason, 
                srv.photo, 
                srv.product_attribute_id, 
                srv.product_attribute, 
                srv.product_attribute_value_id, 
                srv.product_attribute_value, 
                srv.pop_status, 
                srv.pop_longitude, 
                srv.pop_latitude, 
                srv.country, 
                srv.channel, 
                srv.retail_environment_ps, 
                srv.customer, 
                srv.sales_group_code, 
                srv.sales_group_name, 
                srv.customer_grade, 
                srv.external_pop_code, 
                srv.business_unit_name, 
                srv.territory_or_region, 
                srv.prod_status, 
                srv.productdb_id, 
                srv.barcode, 
                srv.unit_price, 
                srv.display_order, 
                srv.launch_date, 
                srv.largest_uom_quantity, 
                srv.middle_uom_quantity, 
                srv.smallest_uom_quantity, 
                srv.company, 
                srv.sku_english, 
                srv.sku_code, 
                srv.ps_category, 
                srv.ps_segment, 
                srv.ps_category_segment, 
                srv.country_l1, 
                srv.regional_franchise_l2, 
                srv.franchise_l3, 
                srv.brand_l4, 
                srv.sub_category_l5, 
                srv.platform_l6, 
                srv.variance_l7, 
                srv.pack_size_l8, 
                srv.sap_matl_num, 
                srv.msl_rank, 
                srv.user_status, 
                srv.userdb_id, 
                srv.first_name, 
                srv.last_name, 
                srv.team, 
                srv.authorisation_group, 
                srv.email_address, 
                srv.user_longitude, 
                srv.user_latitude, 
                srv.display_plan_id, 
                srv.display_type, 
                srv.display_code, 
                srv.display_name, 
                srv.display_start_date, 
                srv.display_end_date, 
                srv.checklist_method, 
                srv.display_number, 
                srv.display_comments, 
                CASE WHEN (
                  upper(
                    (srv.response):: text
                  ) = ('YES' :: character varying):: text
                ) THEN 'YES' :: character varying WHEN (
                  upper(
                    (srv.response):: text
                  ) = ('NO' :: character varying):: text
                ) THEN 'NO' :: character varying ELSE srv.response END AS "y/n_flag", 
                mkt_shr."target" AS mkt_share, 
                NULL AS planned_visit_date, 
                NULL AS visited_flag, 
                NULL AS facing, 
                NULL AS is_eyelevel 
              FROM 
                (
                  (
                    SELECT 
                      'PRODUCT_ATTIBUTE_AUDIT' :: character varying AS data_type, 
                      vst.cntry_cd, 
                      vst.visit_id, 
                      NULL :: character varying AS task_group, 
                      NULL :: integer AS task_id, 
                      NULL :: character varying AS task_name, 
                      vst.audit_form_id, 
                      vst.audit_form, 
                      vst.section_id, 
                      vst.section, 
                      NULL :: character varying AS subsection_id, 
                      NULL :: character varying AS subsection, 
                      vst.field_id, 
                      vst.field_code, 
                      vst.field_label, 
                      vst.field_type, 
                      vst.dependent_on_field_id, 
                      NULL :: character varying AS sku_id, 
                      NULL :: character varying AS sku, 
                      vst.response, 
                      vst.visit_date, 
                      vst.check_in_datetime, 
                      vst.check_out_datetime, 
                      vst.popdb_id, 
                      vst.pop_code, 
                      vst.pop_name, 
                      vst.address, 
                      vst.check_in_longitude, 
                      vst.check_in_latitude, 
                      vst.check_out_longitude, 
                      vst.check_out_latitude, 
                      vst.check_in_photo, 
                      vst.check_out_photo, 
                      vst.username, 
                      vst.user_full_name, 
                      vst.superior_username, 
                      vst.superior_name, 
                      vst.planned_visit, 
                      vst.cancelled_visit, 
                      vst.cancellation_reason, 
                      vst.cancellation_note, 
                      NULL :: character varying AS promotion_plan_id, 
                      NULL :: character varying AS promotion_code, 
                      NULL :: character varying AS promotion_name, 
                      NULL :: character varying AS promotion_mechanics, 
                      NULL :: character varying AS promotion_type, 
                      (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
                      NULL :: character varying AS promotion_compliance, 
                      (NULL :: numeric):: numeric(18, 0) AS actual_price, 
                      NULL :: character varying AS non_compliance_reason, 
                      NULL :: character varying AS photo, 
                      vst.product_attribute_id, 
                      vst.product_attribute, 
                      vst.product_attribute_value_id, 
                      vst.product_attribute_value, 
                      pop.status AS pop_status, 
                      pop.longitude AS pop_longitude, 
                      pop.latitude AS pop_latitude, 
                      pop.country, 
                      pop.channel, 
                      pop.retail_environment_ps, 
                      pop.customer, 
                      pop.sales_group_code, 
                      pop.sales_group_name, 
                      pop.customer_grade, 
                      pop.external_pop_code, 
                      pop.business_unit_name, 
                      pop.territory_or_region, 
                      NULL :: integer AS prod_status, 
                      NULL :: character varying AS productdb_id, 
                      NULL :: character varying AS barcode, 
                      (NULL :: numeric):: numeric(18, 0) AS unit_price, 
                      NULL :: integer AS display_order, 
                      NULL :: character varying AS launch_date, 
                      NULL :: integer AS largest_uom_quantity, 
                      NULL :: integer AS middle_uom_quantity, 
                      NULL :: integer AS smallest_uom_quantity, 
                      NULL :: character varying AS company, 
                      NULL :: character varying AS sku_english, 
                      NULL :: character varying AS sku_code, 
                      prd.ps_category, 
                      prd.ps_segment, 
                      prd.ps_category_segment, 
                      NULL :: character varying AS country_l1, 
                      NULL :: character varying AS regional_franchise_l2, 
                      NULL :: character varying AS franchise_l3, 
                      prd.brand_l4, 
                      NULL :: character varying AS sub_category_l5, 
                      NULL :: character varying AS platform_l6, 
                      NULL :: character varying AS variance_l7, 
                      NULL :: character varying AS pack_size_l8, 
                      NULL :: character varying AS sap_matl_num, 
                      NULL :: character varying AS msl_rank, 
                      usr.status AS user_status, 
                      usr.userdb_id, 
                      usr.first_name, 
                      usr.last_name, 
                      usr.team, 
                      usr.authorisation_group, 
                      usr.email_address, 
                      usr.longitude AS user_longitude, 
                      usr.latitude AS user_latitude, 
                      NULL :: character varying AS display_plan_id, 
                      NULL :: character varying AS display_type, 
                      NULL :: character varying AS display_code, 
                      NULL :: character varying AS display_name, 
                      NULL :: date AS display_start_date, 
                      NULL :: date AS display_end_date, 
                      NULL :: character varying AS checklist_method, 
                      NULL :: integer AS display_number, 
                      NULL :: character varying AS display_comments 
                    FROM 
                      (
                        (
                          (
                            (
                              SELECT 
                                edw_vw_pop6_visits_prod_attribute_audits.cntry_cd, 
                                edw_vw_pop6_visits_prod_attribute_audits.visit_id, 
                                edw_vw_pop6_visits_prod_attribute_audits.audit_form_id, 
                                edw_vw_pop6_visits_prod_attribute_audits.audit_form, 
                                edw_vw_pop6_visits_prod_attribute_audits.section_id, 
                                edw_vw_pop6_visits_prod_attribute_audits.section, 
                                edw_vw_pop6_visits_prod_attribute_audits.field_id, 
                                edw_vw_pop6_visits_prod_attribute_audits.field_code, 
                                edw_vw_pop6_visits_prod_attribute_audits.field_label, 
                                edw_vw_pop6_visits_prod_attribute_audits.field_type, 
                                edw_vw_pop6_visits_prod_attribute_audits.dependent_on_field_id, 
                                edw_vw_pop6_visits_prod_attribute_audits.product_attribute_id, 
                                edw_vw_pop6_visits_prod_attribute_audits.product_attribute, 
                                edw_vw_pop6_visits_prod_attribute_audits.product_attribute_value_id, 
                                edw_vw_pop6_visits_prod_attribute_audits.product_attribute_value, 
                                edw_vw_pop6_visits_prod_attribute_audits.response, 
                                edw_vw_pop6_visits_prod_attribute_audits.visit_date, 
                                edw_vw_pop6_visits_prod_attribute_audits.check_in_datetime, 
                                edw_vw_pop6_visits_prod_attribute_audits.check_out_datetime, 
                                edw_vw_pop6_visits_prod_attribute_audits.popdb_id, 
                                edw_vw_pop6_visits_prod_attribute_audits.pop_code, 
                                edw_vw_pop6_visits_prod_attribute_audits.pop_name, 
                                edw_vw_pop6_visits_prod_attribute_audits.address, 
                                edw_vw_pop6_visits_prod_attribute_audits.check_in_longitude, 
                                edw_vw_pop6_visits_prod_attribute_audits.check_in_latitude, 
                                edw_vw_pop6_visits_prod_attribute_audits.check_out_longitude, 
                                edw_vw_pop6_visits_prod_attribute_audits.check_out_latitude, 
                                edw_vw_pop6_visits_prod_attribute_audits.check_in_photo, 
                                edw_vw_pop6_visits_prod_attribute_audits.check_out_photo, 
                                edw_vw_pop6_visits_prod_attribute_audits.username, 
                                edw_vw_pop6_visits_prod_attribute_audits.user_full_name, 
                                edw_vw_pop6_visits_prod_attribute_audits.superior_username, 
                                edw_vw_pop6_visits_prod_attribute_audits.superior_name, 
                                edw_vw_pop6_visits_prod_attribute_audits.planned_visit, 
                                edw_vw_pop6_visits_prod_attribute_audits.cancelled_visit, 
                                edw_vw_pop6_visits_prod_attribute_audits.cancellation_reason, 
                                edw_vw_pop6_visits_prod_attribute_audits.cancellation_note 
                              FROM 
                                edw_vw_pop6_visits_prod_attribute_audits 
                              WHERE 
                                (
                                  (
                                    edw_vw_pop6_visits_prod_attribute_audits.cntry_cd
                                  ):: text = ('JP' :: character varying):: text
                                )
                            ) vst 
                            LEFT JOIN edw_vw_pop6_store pop ON (
                              (
                                (vst.popdb_id):: text = (pop.popdb_id):: text
                              )
                            )
                          ) 
                          LEFT JOIN edw_vw_pop6_salesperson usr ON (
                            (
                              (vst.username):: text = (usr.username):: text
                            )
                          )
                        ) 
                        LEFT JOIN (
                          SELECT 
                            DISTINCT NULL :: character varying AS brand_l4, 
                            (
                              upper(
                                (
                                  edw_vw_pop6_products.ps_category
                                ):: text
                              )
                            ):: character varying AS ps_category, 
                            edw_vw_pop6_products.ps_segment, 
                            edw_vw_pop6_products.ps_category_segment, 
                            (
                              (
                                (
                                  upper(
                                    (
                                      edw_vw_pop6_products.ps_category
                                    ):: text
                                  ) || ('_' :: character varying):: text
                                ) || (
                                  edw_vw_pop6_products.ps_segment
                                ):: text
                              )
                            ):: character varying AS ps_categorysegement 
                          FROM 
                            edw_vw_pop6_products 
                          UNION 
                          SELECT 
                            DISTINCT edw_vw_pop6_products.brand_l4, 
                            NULL :: character varying AS ps_category, 
                            NULL :: character varying AS ps_segment, 
                            NULL :: character varying AS ps_category_segment, 
                            NULL :: character varying AS ps_categorysegement 
                          FROM 
                            edw_vw_pop6_products 
                          WHERE 
                            (
                              (edw_vw_pop6_products.cntry_cd):: text = ('JP' :: character varying):: text
                            )
                        ) prd ON (
                          CASE WHEN (
                            (vst.product_attribute):: text = (
                              'PS Category Segment' :: character varying
                            ):: text
                          ) THEN (
                            (vst.product_attribute_value):: text = (prd.ps_categorysegement):: text
                          ) ELSE (
                            (vst.product_attribute_value):: text = (prd.brand_l4):: text
                          ) END
                        )
                      )
                  ) srv 
                  LEFT JOIN (
                    SELECT 
                      edw_vw_ps_targets.kpi, 
                      edw_vw_ps_targets.attribute_1 AS category, 
                      edw_vw_ps_targets.attribute_2 AS segment, 
                      edw_vw_ps_targets.value AS "target" 
                    FROM 
                      edw_vw_ps_targets 
                    WHERE 
                      (
                        (
                          upper(
                            (edw_vw_ps_targets.market):: text
                          ) = ('JAPAN' :: character varying):: text
                        ) 
                        AND (
                          upper(
                            (edw_vw_ps_targets.channel):: text
                          ) = (
                            'MODERN TRADE' :: character varying
                          ):: text
                        )
                      )
                  ) mkt_shr ON (
                    (
                      (
                        (
                          upper(
                            (mkt_shr.category):: text
                          ) = upper(
                            (srv.ps_category):: text
                          )
                        ) 
                        AND (
                          upper(
                            (mkt_shr.segment):: text
                          ) = upper(
                            (srv.ps_segment):: text
                          )
                        )
                      ) 
                      AND (
                        upper(
                          (mkt_shr.kpi):: text
                        ) = (
                          CASE WHEN (
                            "substring"(
                              upper(
                                (srv.field_code):: text
                              ), 
                              0, 
                              6
                            ) = ('PS_SOS' :: character varying):: text
                          ) THEN 'SOS COMPLIANCE' :: character varying WHEN (
                            "substring"(
                              upper(
                                (srv.field_code):: text
                              ), 
                              0, 
                              6
                            ) = ('PS_SOA' :: character varying):: text
                          ) THEN 'SOA COMPLIANCE' :: character varying ELSE srv.field_code END
                        ):: text
                      )
                    )
                  )
                )
            ) ,
display_2 as (
            
            SELECT 
              srv.data_type, 
              srv.cntry_cd, 
              srv.visit_id, 
              srv.task_group, 
              srv.task_id, 
              srv.task_name, 
              srv.audit_form_id, 
              srv.audit_form, 
              srv.section_id, 
              srv.section, 
              srv.subsection_id, 
              srv.subsection, 
              srv.field_id, 
              srv.field_code, 
              srv.questiontext AS field_label, 
              srv.field_type, 
              srv.dependent_on_field_id, 
              srv.sku_id, 
              srv.sku, 
              srv.response, 
              srv.visit_date, 
              srv.check_in_datetime, 
              srv.check_out_datetime, 
              srv.popdb_id, 
              srv.pop_code, 
              srv.pop_name, 
              srv.address, 
              srv.check_in_longitude, 
              srv.check_in_latitude, 
              srv.check_out_longitude, 
              srv.check_out_latitude, 
              srv.check_in_photo, 
              srv.check_out_photo, 
              srv.username, 
              srv.user_full_name, 
              srv.superior_username, 
              srv.superior_name, 
              srv.planned_visit, 
              srv.cancelled_visit, 
              srv.cancellation_reason, 
              srv.cancellation_note, 
              srv.promotion_plan_id, 
              srv.promotion_code, 
              srv.promotion_name, 
              srv.promotion_mechanics, 
              srv.promotion_type, 
              srv.promotion_price, 
              srv.promotion_compliance, 
              srv.actual_price, 
              srv.non_compliance_reason, 
              srv.photo, 
              srv.product_attribute_id, 
              srv.product_attribute, 
              srv.product_attribute_value_id, 
              srv.product_attribute_value, 
              srv.pop_status, 
              srv.pop_longitude, 
              srv.pop_latitude, 
              srv.country, 
              srv.channel, 
              srv.retail_environment_ps, 
              srv.customer, 
              srv.sales_group_code, 
              srv.sales_group_name, 
              srv.customer_grade, 
              srv.external_pop_code, 
              srv.business_unit_name, 
              srv.territory_or_region, 
              srv.prod_status, 
              srv.productdb_id, 
              srv.barcode, 
              srv.unit_price, 
              srv.display_order, 
              srv.launch_date, 
              srv.largest_uom_quantity, 
              srv.middle_uom_quantity, 
              srv.smallest_uom_quantity, 
              srv.company, 
              srv.sku_english, 
              srv.sku_code, 
              srv.ps_category, 
              srv.ps_segment, 
              srv.ps_category_segment, 
              srv.country_l1, 
              srv.regional_franchise_l2, 
              srv.franchise_l3, 
              srv.brand_l4, 
              srv.sub_category_l5, 
              srv.platform_l6, 
              srv.variance_l7, 
              srv.pack_size_l8, 
              srv.sap_matl_num, 
              srv.msl_rank, 
              srv.user_status, 
              srv.userdb_id, 
              srv.first_name, 
              srv.last_name, 
              srv.team, 
              srv.authorisation_group, 
              srv.email_address, 
              srv.user_longitude, 
              srv.user_latitude, 
              srv.display_plan_id, 
              srv.display_type, 
              srv.display_code, 
              srv.display_name, 
              srv.display_start_date, 
              srv.display_end_date, 
              srv.checklist_method, 
              srv.display_number, 
              srv.display_comments, 
              CASE WHEN (
                upper(
                  (srv.response):: text
                ) = ('YES' :: character varying):: text
              ) THEN 'YES' :: character varying WHEN (
                upper(
                  (srv.response):: text
                ) = ('NO' :: character varying):: text
              ) THEN 'NO' :: character varying ELSE srv.response END AS "y/n_flag", 
              NULL AS mkt_share, 
              NULL AS planned_visit_date, 
              NULL AS visited_flag, 
              NULL AS facing, 
              NULL AS is_eyelevel 
            FROM 
              (
                SELECT 
                  'DISPLAY' :: character varying AS data_type, 
                  vst.cntry_cd, 
                  vst.visit_id, 
                  NULL :: character varying AS task_group, 
                  NULL :: integer AS task_id, 
                  NULL :: character varying AS task_name, 
                  NULL :: character varying AS audit_form_id, 
                  NULL :: character varying AS audit_form, 
                  NULL :: character varying AS section_id, 
                  NULL :: character varying AS section, 
                  NULL :: character varying AS subsection_id, 
                  NULL :: character varying AS subsection, 
                  vst.field_id, 
                  vst.field_code, 
                  vst.field_label AS questiontext, 
                  vst.field_type, 
                  vst.dependent_on_field_id, 
                  NULL :: character varying AS sku_id, 
                  NULL :: character varying AS sku, 
                  vst.response, 
                  vst.visit_date, 
                  vst.check_in_datetime, 
                  vst.check_out_datetime, 
                  vst.popdb_id, 
                  vst.pop_code, 
                  vst.pop_name, 
                  vst.address, 
                  vst.check_in_longitude, 
                  vst.check_in_latitude, 
                  vst.check_out_longitude, 
                  vst.check_out_latitude, 
                  vst.check_in_photo, 
                  vst.check_out_photo, 
                  vst.username, 
                  vst.user_full_name, 
                  vst.superior_username, 
                  vst.superior_name, 
                  vst.planned_visit, 
                  vst.cancelled_visit, 
                  vst.cancellation_reason, 
                  vst.cancellation_note, 
                  NULL :: character varying AS promotion_plan_id, 
                  NULL :: character varying AS promotion_code, 
                  NULL :: character varying AS promotion_name, 
                  NULL :: character varying AS promotion_mechanics, 
                  NULL :: character varying AS promotion_type, 
                  (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
                  NULL :: character varying AS promotion_compliance, 
                  (NULL :: numeric):: numeric(18, 0) AS actual_price, 
                  NULL :: character varying AS non_compliance_reason, 
                  NULL :: character varying AS photo, 
                  vst.product_attribute_id, 
                  vst.product_attribute, 
                  vst.product_attribute_value_id, 
                  vst.product_attribute_value, 
                  pop.status AS pop_status, 
                  pop.longitude AS pop_longitude, 
                  pop.latitude AS pop_latitude, 
                  pop.country, 
                  pop.channel, 
                  pop.retail_environment_ps, 
                  pop.customer, 
                  pop.sales_group_code, 
                  pop.sales_group_name, 
                  pop.customer_grade, 
                  pop.external_pop_code, 
                  pop.business_unit_name, 
                  pop.territory_or_region, 
                  NULL :: integer AS prod_status, 
                  NULL :: character varying AS productdb_id, 
                  NULL :: character varying AS barcode, 
                  (NULL :: numeric):: numeric(18, 0) AS unit_price, 
                  NULL :: integer AS display_order, 
                  NULL :: character varying AS launch_date, 
                  NULL :: integer AS largest_uom_quantity, 
                  NULL :: integer AS middle_uom_quantity, 
                  NULL :: integer AS smallest_uom_quantity, 
                  NULL :: character varying AS company, 
                  NULL :: character varying AS sku_english, 
                  NULL :: character varying AS sku_code, 
                  prd.ps_category, 
                  prd.ps_segment, 
                  prd.ps_category_segment, 
                  NULL :: character varying AS country_l1, 
                  NULL :: character varying AS regional_franchise_l2, 
                  NULL :: character varying AS franchise_l3, 
                  prd.brand_l4, 
                  NULL :: character varying AS sub_category_l5, 
                  NULL :: character varying AS platform_l6, 
                  NULL :: character varying AS variance_l7, 
                  NULL :: character varying AS pack_size_l8, 
                  NULL :: character varying AS sap_matl_num, 
                  NULL :: character varying AS msl_rank, 
                  usr.status AS user_status, 
                  usr.userdb_id, 
                  usr.first_name, 
                  usr.last_name, 
                  usr.team, 
                  usr.authorisation_group, 
                  usr.email_address, 
                  usr.longitude AS user_longitude, 
                  usr.latitude AS user_latitude, 
                  vst.display_plan_id, 
                  vst.display_type, 
                  vst.display_code, 
                  vst.display_name, 
                  vst.start_date AS display_start_date, 
                  vst.end_date AS display_end_date, 
                  vst.checklist_method, 
                  vst.display_number, 
                  vst.comments AS display_comments 
                FROM 
                  (
                    (
                      (
                        (
                          SELECT 
                            edw_vw_pop6_visits_display.cntry_cd, 
                            edw_vw_pop6_visits_display.visit_id, 
                            edw_vw_pop6_visits_display.display_plan_id, 
                            edw_vw_pop6_visits_display.display_type, 
                            edw_vw_pop6_visits_display.display_code, 
                            edw_vw_pop6_visits_display.display_name, 
                            edw_vw_pop6_visits_display.start_date, 
                            edw_vw_pop6_visits_display.end_date, 
                            edw_vw_pop6_visits_display.checklist_method, 
                            edw_vw_pop6_visits_display.display_number, 
                            edw_vw_pop6_visits_display.product_attribute_id, 
                            edw_vw_pop6_visits_display.product_attribute, 
                            edw_vw_pop6_visits_display.product_attribute_value_id, 
                            edw_vw_pop6_visits_display.product_attribute_value, 
                            edw_vw_pop6_visits_display.comments, 
                            edw_vw_pop6_visits_display.field_id, 
                            edw_vw_pop6_visits_display.field_code, 
                            edw_vw_pop6_visits_display.field_label, 
                            edw_vw_pop6_visits_display.field_type, 
                            edw_vw_pop6_visits_display.dependent_on_field_id, 
                            edw_vw_pop6_visits_display.response, 
                            edw_vw_pop6_visits_display.visit_date, 
                            edw_vw_pop6_visits_display.check_in_datetime, 
                            edw_vw_pop6_visits_display.check_out_datetime, 
                            edw_vw_pop6_visits_display.popdb_id, 
                            edw_vw_pop6_visits_display.pop_code, 
                            edw_vw_pop6_visits_display.pop_name, 
                            edw_vw_pop6_visits_display.address, 
                            edw_vw_pop6_visits_display.check_in_longitude, 
                            edw_vw_pop6_visits_display.check_in_latitude, 
                            edw_vw_pop6_visits_display.check_out_longitude, 
                            edw_vw_pop6_visits_display.check_out_latitude, 
                            edw_vw_pop6_visits_display.check_in_photo, 
                            edw_vw_pop6_visits_display.check_out_photo, 
                            edw_vw_pop6_visits_display.username, 
                            edw_vw_pop6_visits_display.user_full_name, 
                            edw_vw_pop6_visits_display.superior_username, 
                            edw_vw_pop6_visits_display.superior_name, 
                            edw_vw_pop6_visits_display.planned_visit, 
                            edw_vw_pop6_visits_display.cancelled_visit, 
                            edw_vw_pop6_visits_display.cancellation_reason, 
                            edw_vw_pop6_visits_display.cancellation_note 
                          FROM 
                            edw_vw_pop6_visits_display 
                          WHERE 
                            (
                              (
                                edw_vw_pop6_visits_display.cntry_cd
                              ):: text = ('JP' :: character varying):: text
                            )
                        ) vst 
                        LEFT JOIN edw_vw_pop6_store pop ON (
                          (
                            (vst.popdb_id):: text = (pop.popdb_id):: text
                          )
                        )
                      ) 
                      LEFT JOIN edw_vw_pop6_salesperson usr ON (
                        (
                          (vst.username):: text = (usr.username):: text
                        )
                      )
                    ) 
                    LEFT JOIN (
                      SELECT 
                        DISTINCT NULL :: character varying AS brand_l4, 
                        (
                          upper(
                            (
                              edw_vw_pop6_products.ps_category
                            ):: text
                          )
                        ):: character varying AS ps_category, 
                        edw_vw_pop6_products.ps_segment, 
                        edw_vw_pop6_products.ps_category_segment, 
                        (
                          (
                            (
                              upper(
                                (
                                  edw_vw_pop6_products.ps_category
                                ):: text
                              ) || ('_' :: character varying):: text
                            ) || (
                              edw_vw_pop6_products.ps_segment
                            ):: text
                          )
                        ):: character varying AS ps_categorysegement 
                      FROM 
                        edw_vw_pop6_products 
                      UNION 
                      SELECT 
                        DISTINCT edw_vw_pop6_products.brand_l4, 
                        NULL :: character varying AS ps_category, 
                        NULL :: character varying AS ps_segment, 
                        NULL :: character varying AS ps_category_segment, 
                        NULL :: character varying AS ps_categorysegement 
                      FROM 
                        edw_vw_pop6_products 
                      WHERE 
                        (
                          (edw_vw_pop6_products.cntry_cd):: text = ('JP' :: character varying):: text
                        )
                    ) prd ON (
                      CASE WHEN (
                        (vst.product_attribute):: text = (
                          'PS Category Segment' :: character varying
                        ):: text
                      ) THEN (
                        (vst.product_attribute_value):: text = (prd.ps_categorysegement):: text
                      ) ELSE (
                        (vst.product_attribute_value):: text = (prd.brand_l4):: text
                      ) END
                    )
                  )
              ) srv
          ) ,
  product_attribute_audit_3 as  (
         
          SELECT 
            srv.data_type, 
            srv.cntry_cd, 
            srv.visit_id, 
            srv.task_group, 
            srv.task_id, 
            srv.task_name, 
            srv.audit_form_id, 
            srv.audit_form, 
            srv.section_id, 
            srv.section, 
            srv.subsection_id, 
            srv.subsection, 
            srv.field_id, 
            srv.field_code, 
            srv.field_label, 
            srv.field_type, 
            srv.dependent_on_field_id, 
            srv.sku_id, 
            srv.sku, 
            srv.response, 
            srv.visit_date, 
            srv.check_in_datetime, 
            srv.check_out_datetime, 
            srv.popdb_id, 
            srv.pop_code, 
            srv.pop_name, 
            srv.address, 
            srv.check_in_longitude, 
            srv.check_in_latitude, 
            srv.check_out_longitude, 
            srv.check_out_latitude, 
            srv.check_in_photo, 
            srv.check_out_photo, 
            srv.username, 
            srv.user_full_name, 
            srv.superior_username, 
            srv.superior_name, 
            srv.planned_visit, 
            srv.cancelled_visit, 
            srv.cancellation_reason, 
            srv.cancellation_note, 
            srv.promotion_plan_id, 
            srv.promotion_code, 
            srv.promotion_name, 
            srv.promotion_mechanics, 
            srv.promotion_type, 
            srv.promotion_price, 
            srv.promotion_compliance, 
            srv.actual_price, 
            srv.non_compliance_reason, 
            srv.photo, 
            srv.product_attribute_id, 
            srv.product_attribute, 
            srv.product_attribute_value_id, 
            srv.product_attribute_value, 
            srv.pop_status, 
            srv.pop_longitude, 
            srv.pop_latitude, 
            srv.country, 
            srv.channel, 
            srv.retail_environment_ps, 
            srv.customer, 
            srv.sales_group_code, 
            srv.sales_group_name, 
            srv.customer_grade, 
            srv.external_pop_code, 
            srv.business_unit_name, 
            srv.territory_or_region, 
            srv.prod_status, 
            srv.productdb_id, 
            srv.barcode, 
            srv.unit_price, 
            srv.display_order, 
            srv.launch_date, 
            srv.largest_uom_quantity, 
            srv.middle_uom_quantity, 
            srv.smallest_uom_quantity, 
            srv.company, 
            srv.sku_english, 
            srv.sku_code, 
            srv.ps_category, 
            srv.ps_segment, 
            srv.ps_category_segment, 
            srv.country_l1, 
            srv.regional_franchise_l2, 
            srv.franchise_l3, 
            srv.brand_l4, 
            srv.sub_category_l5, 
            srv.platform_l6, 
            srv.variance_l7, 
            srv.pack_size_l8, 
            srv.sap_matl_num, 
            srv.msl_rank, 
            srv.user_status, 
            srv.userdb_id, 
            srv.first_name, 
            srv.last_name, 
            srv.team, 
            srv.authorisation_group, 
            srv.email_address, 
            srv.user_longitude, 
            srv.user_latitude, 
            srv.display_plan_id, 
            srv.display_type, 
            srv.display_code, 
            srv.display_name, 
            srv.display_start_date, 
            srv.display_end_date, 
            srv.checklist_method, 
            srv.display_number, 
            srv.display_comments, 
            CASE WHEN (
              upper(
                (srv.response):: text
              ) = ('YES' :: character varying):: text
            ) THEN 'YES' :: character varying WHEN (
              upper(
                (srv.response):: text
              ) = ('NO' :: character varying):: text
            ) THEN 'NO' :: character varying ELSE srv.response END AS "y/n_flag", 
            mkt_shr."target" AS mkt_share, 
            NULL AS planned_visit_date, 
            NULL AS visited_flag, 
            NULL AS facing, 
            NULL AS is_eyelevel 
          FROM 
            (
              (
                SELECT 
                  'PRODUCT_ATTIBUTE_AUDIT' :: character varying AS data_type, 
                  vst.cntry_cd, 
                  vst.visit_id, 
                  NULL :: character varying AS task_group, 
                  NULL :: integer AS task_id, 
                  NULL :: character varying AS task_name, 
                  vst.audit_form_id, 
                  vst.audit_form, 
                  vst.section_id, 
                  vst.section, 
                  NULL :: character varying AS subsection_id, 
                  NULL :: character varying AS subsection, 
                  vst.field_id, 
                  vst.field_code, 
                  vst.field_label, 
                  vst.field_type, 
                  vst.dependent_on_field_id, 
                  NULL :: character varying AS sku_id, 
                  NULL :: character varying AS sku, 
                  vst.response, 
                  vst.visit_date, 
                  vst.check_in_datetime, 
                  vst.check_out_datetime, 
                  vst.popdb_id, 
                  vst.pop_code, 
                  vst.pop_name, 
                  vst.address, 
                  vst.check_in_longitude, 
                  vst.check_in_latitude, 
                  vst.check_out_longitude, 
                  vst.check_out_latitude, 
                  vst.check_in_photo, 
                  vst.check_out_photo, 
                  vst.username, 
                  vst.user_full_name, 
                  vst.superior_username, 
                  vst.superior_name, 
                  vst.planned_visit, 
                  vst.cancelled_visit, 
                  vst.cancellation_reason, 
                  vst.cancellation_note, 
                  NULL :: character varying AS promotion_plan_id, 
                  NULL :: character varying AS promotion_code, 
                  NULL :: character varying AS promotion_name, 
                  NULL :: character varying AS promotion_mechanics, 
                  NULL :: character varying AS promotion_type, 
                  (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
                  NULL :: character varying AS promotion_compliance, 
                  (NULL :: numeric):: numeric(18, 0) AS actual_price, 
                  NULL :: character varying AS non_compliance_reason, 
                  NULL :: character varying AS photo, 
                  vst.product_attribute_id, 
                  vst.product_attribute, 
                  vst.product_attribute_value_id, 
                  vst.product_attribute_value, 
                  pop.status AS pop_status, 
                  pop.longitude AS pop_longitude, 
                  pop.latitude AS pop_latitude, 
                  pop.country, 
                  pop.channel, 
                  pop.retail_environment_ps, 
                  pop.customer, 
                  pop.sales_group_code, 
                  pop.sales_group_name, 
                  pop.customer_grade, 
                  pop.external_pop_code, 
                  pop.business_unit_name, 
                  pop.territory_or_region, 
                  NULL :: integer AS prod_status, 
                  NULL :: character varying AS productdb_id, 
                  NULL :: character varying AS barcode, 
                  (NULL :: numeric):: numeric(18, 0) AS unit_price, 
                  NULL :: integer AS display_order, 
                  NULL :: character varying AS launch_date, 
                  NULL :: integer AS largest_uom_quantity, 
                  NULL :: integer AS middle_uom_quantity, 
                  NULL :: integer AS smallest_uom_quantity, 
                  NULL :: character varying AS company, 
                  NULL :: character varying AS sku_english, 
                  NULL :: character varying AS sku_code, 
                  prd.ps_category, 
                  prd.ps_segment, 
                  prd.ps_category_segment, 
                  NULL :: character varying AS country_l1, 
                  NULL :: character varying AS regional_franchise_l2, 
                  NULL :: character varying AS franchise_l3, 
                  prd.brand_l4, 
                  NULL :: character varying AS sub_category_l5, 
                  NULL :: character varying AS platform_l6, 
                  NULL :: character varying AS variance_l7, 
                  NULL :: character varying AS pack_size_l8, 
                  NULL :: character varying AS sap_matl_num, 
                  NULL :: character varying AS msl_rank, 
                  usr.status AS user_status, 
                  usr.userdb_id, 
                  usr.first_name, 
                  usr.last_name, 
                  usr.team, 
                  usr.authorisation_group, 
                  usr.email_address, 
                  usr.longitude AS user_longitude, 
                  usr.latitude AS user_latitude, 
                  NULL :: character varying AS display_plan_id, 
                  NULL :: character varying AS display_type, 
                  NULL :: character varying AS display_code, 
                  NULL :: character varying AS display_name, 
                  NULL :: date AS display_start_date, 
                  NULL :: date AS display_end_date, 
                  NULL :: character varying AS checklist_method, 
                  NULL :: integer AS display_number, 
                  NULL :: character varying AS display_comments 
                FROM 
                  (
                    (
                      (
                        (
                          SELECT 
                            edw_vw_pop6_visits_prod_attribute_audits.cntry_cd, 
                            edw_vw_pop6_visits_prod_attribute_audits.visit_id, 
                            edw_vw_pop6_visits_prod_attribute_audits.audit_form_id, 
                            edw_vw_pop6_visits_prod_attribute_audits.audit_form, 
                            edw_vw_pop6_visits_prod_attribute_audits.section_id, 
                            edw_vw_pop6_visits_prod_attribute_audits.section, 
                            edw_vw_pop6_visits_prod_attribute_audits.field_id, 
                            edw_vw_pop6_visits_prod_attribute_audits.field_code, 
                            edw_vw_pop6_visits_prod_attribute_audits.field_label, 
                            edw_vw_pop6_visits_prod_attribute_audits.field_type, 
                            edw_vw_pop6_visits_prod_attribute_audits.dependent_on_field_id, 
                            edw_vw_pop6_visits_prod_attribute_audits.product_attribute_id, 
                            edw_vw_pop6_visits_prod_attribute_audits.product_attribute, 
                            edw_vw_pop6_visits_prod_attribute_audits.product_attribute_value_id, 
                            edw_vw_pop6_visits_prod_attribute_audits.product_attribute_value, 
                            edw_vw_pop6_visits_prod_attribute_audits.response, 
                            edw_vw_pop6_visits_prod_attribute_audits.visit_date, 
                            edw_vw_pop6_visits_prod_attribute_audits.check_in_datetime, 
                            edw_vw_pop6_visits_prod_attribute_audits.check_out_datetime, 
                            edw_vw_pop6_visits_prod_attribute_audits.popdb_id, 
                            edw_vw_pop6_visits_prod_attribute_audits.pop_code, 
                            edw_vw_pop6_visits_prod_attribute_audits.pop_name, 
                            edw_vw_pop6_visits_prod_attribute_audits.address, 
                            edw_vw_pop6_visits_prod_attribute_audits.check_in_longitude, 
                            edw_vw_pop6_visits_prod_attribute_audits.check_in_latitude, 
                            edw_vw_pop6_visits_prod_attribute_audits.check_out_longitude, 
                            edw_vw_pop6_visits_prod_attribute_audits.check_out_latitude, 
                            edw_vw_pop6_visits_prod_attribute_audits.check_in_photo, 
                            edw_vw_pop6_visits_prod_attribute_audits.check_out_photo, 
                            edw_vw_pop6_visits_prod_attribute_audits.username, 
                            edw_vw_pop6_visits_prod_attribute_audits.user_full_name, 
                            edw_vw_pop6_visits_prod_attribute_audits.superior_username, 
                            edw_vw_pop6_visits_prod_attribute_audits.superior_name, 
                            edw_vw_pop6_visits_prod_attribute_audits.planned_visit, 
                            edw_vw_pop6_visits_prod_attribute_audits.cancelled_visit, 
                            edw_vw_pop6_visits_prod_attribute_audits.cancellation_reason, 
                            edw_vw_pop6_visits_prod_attribute_audits.cancellation_note 
                          FROM 
                            edw_vw_pop6_visits_prod_attribute_audits 
                          WHERE 
                            (
                              (
                                edw_vw_pop6_visits_prod_attribute_audits.cntry_cd
                              ):: text = ('SG' :: character varying):: text
                            )
                        ) vst 
                        LEFT JOIN edw_vw_pop6_store pop ON (
                          (
                            (vst.popdb_id):: text = (pop.popdb_id):: text
                          )
                        )
                      ) 
                      LEFT JOIN edw_vw_pop6_salesperson usr ON (
                        (
                          (vst.username):: text = (usr.username):: text
                        )
                      )
                    ) 
                    LEFT JOIN (
                      SELECT 
                        DISTINCT NULL :: character varying AS brand_l4, 
                        (
                          upper(
                            (
                              edw_vw_pop6_products.ps_category
                            ):: text
                          )
                        ):: character varying AS ps_category, 
                        edw_vw_pop6_products.ps_segment, 
                        edw_vw_pop6_products.ps_category_segment, 
                        (
                          (
                            (
                              upper(
                                (
                                  edw_vw_pop6_products.ps_category
                                ):: text
                              ) || ('_' :: character varying):: text
                            ) || (
                              edw_vw_pop6_products.ps_segment
                            ):: text
                          )
                        ):: character varying AS ps_categorysegement 
                      FROM 
                        edw_vw_pop6_products 
                      UNION 
                      SELECT 
                        DISTINCT edw_vw_pop6_products.brand_l4, 
                        NULL :: character varying AS ps_category, 
                        NULL :: character varying AS ps_segment, 
                        NULL :: character varying AS ps_category_segment, 
                        NULL :: character varying AS ps_categorysegement 
                      FROM 
                        edw_vw_pop6_products 
                      WHERE 
                        (
                          (edw_vw_pop6_products.cntry_cd):: text = ('SG' :: character varying):: text
                        )
                    ) prd ON (
                      CASE WHEN (
                        (vst.product_attribute):: text = (
                          'PS Category Segment' :: character varying
                        ):: text
                      ) THEN (
                        (vst.product_attribute_value):: text = (prd.ps_categorysegement):: text
                      ) ELSE (
                        (vst.product_attribute_value):: text = (prd.brand_l4):: text
                      ) END
                    )
                  )
              ) srv 
              LEFT JOIN (
                SELECT 
                  edw_vw_ps_targets.kpi, 
                  edw_vw_ps_targets.attribute_1 AS category, 
                  edw_vw_ps_targets.attribute_2 AS segment, 
                  edw_vw_ps_targets.value AS "target" 
                FROM 
                  edw_vw_ps_targets 
                WHERE 
                  (
                    (
                      upper(
                        (edw_vw_ps_targets.market):: text
                      ) = ('SINGAPORE' :: character varying):: text
                    ) 
                    AND (
                      upper(
                        (edw_vw_ps_targets.channel):: text
                      ) = (
                        'MODERN TRADE' :: character varying
                      ):: text
                    )
                  )
              ) mkt_shr ON (
                (
                  (
                    (
                      upper(
                        (mkt_shr.category):: text
                      ) = upper(
                        (srv.ps_category):: text
                      )
                    ) 
                    AND (
                      upper(
                        (mkt_shr.segment):: text
                      ) = upper(
                        (srv.ps_segment):: text
                      )
                    )
                  ) 
                  AND (
                    upper(
                      (mkt_shr.kpi):: text
                    ) = (
                      CASE WHEN (
                        "substring"(
                          upper(
                            (srv.field_code):: text
                          ), 
                          0, 
                          6
                        ) = ('PS_SOS' :: character varying):: text
                      ) THEN 'SOS COMPLIANCE' :: character varying WHEN (
                        "substring"(
                          upper(
                            (srv.field_code):: text
                          ), 
                          0, 
                          6
                        ) = ('PS_SOA' :: character varying):: text
                      ) THEN 'SOA COMPLIANCE' :: character varying ELSE srv.field_code END
                    ):: text
                  )
                )
              )
            )
        ) ,
display_3 as  (
       
        SELECT 
          srv.data_type, 
          srv.cntry_cd, 
          srv.visit_id, 
          srv.task_group, 
          srv.task_id, 
          srv.task_name, 
          srv.audit_form_id, 
          srv.audit_form, 
          srv.section_id, 
          srv.section, 
          srv.subsection_id, 
          srv.subsection, 
          srv.field_id, 
          srv.field_code, 
          srv.questiontext AS field_label, 
          srv.field_type, 
          srv.dependent_on_field_id, 
          srv.sku_id, 
          srv.sku, 
          srv.response, 
          srv.visit_date, 
          srv.check_in_datetime, 
          srv.check_out_datetime, 
          srv.popdb_id, 
          srv.pop_code, 
          srv.pop_name, 
          srv.address, 
          srv.check_in_longitude, 
          srv.check_in_latitude, 
          srv.check_out_longitude, 
          srv.check_out_latitude, 
          srv.check_in_photo, 
          srv.check_out_photo, 
          srv.username, 
          srv.user_full_name, 
          srv.superior_username, 
          srv.superior_name, 
          srv.planned_visit, 
          srv.cancelled_visit, 
          srv.cancellation_reason, 
          srv.cancellation_note, 
          srv.promotion_plan_id, 
          srv.promotion_code, 
          srv.promotion_name, 
          srv.promotion_mechanics, 
          srv.promotion_type, 
          srv.promotion_price, 
          srv.promotion_compliance, 
          srv.actual_price, 
          srv.non_compliance_reason, 
          srv.photo, 
          srv.product_attribute_id, 
          srv.product_attribute, 
          srv.product_attribute_value_id, 
          srv.product_attribute_value, 
          srv.pop_status, 
          srv.pop_longitude, 
          srv.pop_latitude, 
          srv.country, 
          srv.channel, 
          srv.retail_environment_ps, 
          srv.customer, 
          srv.sales_group_code, 
          srv.sales_group_name, 
          srv.customer_grade, 
          srv.external_pop_code, 
          srv.business_unit_name, 
          srv.territory_or_region, 
          srv.prod_status, 
          srv.productdb_id, 
          srv.barcode, 
          srv.unit_price, 
          srv.display_order, 
          srv.launch_date, 
          srv.largest_uom_quantity, 
          srv.middle_uom_quantity, 
          srv.smallest_uom_quantity, 
          srv.company, 
          srv.sku_english, 
          srv.sku_code, 
          srv.ps_category, 
          srv.ps_segment, 
          srv.ps_category_segment, 
          srv.country_l1, 
          srv.regional_franchise_l2, 
          srv.franchise_l3, 
          srv.brand_l4, 
          srv.sub_category_l5, 
          srv.platform_l6, 
          srv.variance_l7, 
          srv.pack_size_l8, 
          srv.sap_matl_num, 
          srv.msl_rank, 
          srv.user_status, 
          srv.userdb_id, 
          srv.first_name, 
          srv.last_name, 
          srv.team, 
          srv.authorisation_group, 
          srv.email_address, 
          srv.user_longitude, 
          srv.user_latitude, 
          srv.display_plan_id, 
          srv.display_type, 
          srv.display_code, 
          srv.display_name, 
          srv.display_start_date, 
          srv.display_end_date, 
          srv.checklist_method, 
          srv.display_number, 
          srv.display_comments, 
          CASE WHEN (
            upper(
              (srv.response):: text
            ) = ('YES' :: character varying):: text
          ) THEN 'YES' :: character varying WHEN (
            upper(
              (srv.response):: text
            ) = ('NO' :: character varying):: text
          ) THEN 'NO' :: character varying ELSE srv.response END AS "y/n_flag", 
          NULL AS mkt_share, 
          NULL AS planned_visit_date, 
          NULL AS visited_flag, 
          NULL AS facing, 
          NULL AS is_eyelevel 
        FROM 
          (
            SELECT 
              'DISPLAY' :: character varying AS data_type, 
              vst.cntry_cd, 
              vst.visit_id, 
              NULL :: character varying AS task_group, 
              NULL :: integer AS task_id, 
              NULL :: character varying AS task_name, 
              NULL :: character varying AS audit_form_id, 
              NULL :: character varying AS audit_form, 
              NULL :: character varying AS section_id, 
              NULL :: character varying AS section, 
              NULL :: character varying AS subsection_id, 
              NULL :: character varying AS subsection, 
              vst.field_id, 
              vst.field_code, 
              vst.field_label AS questiontext, 
              vst.field_type, 
              vst.dependent_on_field_id, 
              NULL :: character varying AS sku_id, 
              NULL :: character varying AS sku, 
              vst.response, 
              vst.visit_date, 
              vst.check_in_datetime, 
              vst.check_out_datetime, 
              vst.popdb_id, 
              vst.pop_code, 
              vst.pop_name, 
              vst.address, 
              vst.check_in_longitude, 
              vst.check_in_latitude, 
              vst.check_out_longitude, 
              vst.check_out_latitude, 
              vst.check_in_photo, 
              vst.check_out_photo, 
              vst.username, 
              vst.user_full_name, 
              vst.superior_username, 
              vst.superior_name, 
              vst.planned_visit, 
              vst.cancelled_visit, 
              vst.cancellation_reason, 
              vst.cancellation_note, 
              NULL :: character varying AS promotion_plan_id, 
              NULL :: character varying AS promotion_code, 
              NULL :: character varying AS promotion_name, 
              NULL :: character varying AS promotion_mechanics, 
              NULL :: character varying AS promotion_type, 
              (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
              NULL :: character varying AS promotion_compliance, 
              (NULL :: numeric):: numeric(18, 0) AS actual_price, 
              NULL :: character varying AS non_compliance_reason, 
              NULL :: character varying AS photo, 
              vst.product_attribute_id, 
              vst.product_attribute, 
              vst.product_attribute_value_id, 
              vst.product_attribute_value, 
              pop.status AS pop_status, 
              pop.longitude AS pop_longitude, 
              pop.latitude AS pop_latitude, 
              pop.country, 
              pop.channel, 
              pop.retail_environment_ps, 
              pop.customer, 
              pop.sales_group_code, 
              pop.sales_group_name, 
              pop.customer_grade, 
              pop.external_pop_code, 
              pop.business_unit_name, 
              pop.territory_or_region, 
              NULL :: integer AS prod_status, 
              NULL :: character varying AS productdb_id, 
              NULL :: character varying AS barcode, 
              (NULL :: numeric):: numeric(18, 0) AS unit_price, 
              NULL :: integer AS display_order, 
              NULL :: character varying AS launch_date, 
              NULL :: integer AS largest_uom_quantity, 
              NULL :: integer AS middle_uom_quantity, 
              NULL :: integer AS smallest_uom_quantity, 
              NULL :: character varying AS company, 
              NULL :: character varying AS sku_english, 
              NULL :: character varying AS sku_code, 
              prd.ps_category, 
              prd.ps_segment, 
              prd.ps_category_segment, 
              NULL :: character varying AS country_l1, 
              NULL :: character varying AS regional_franchise_l2, 
              NULL :: character varying AS franchise_l3, 
              prd.brand_l4, 
              NULL :: character varying AS sub_category_l5, 
              NULL :: character varying AS platform_l6, 
              NULL :: character varying AS variance_l7, 
              NULL :: character varying AS pack_size_l8, 
              NULL :: character varying AS sap_matl_num, 
              NULL :: character varying AS msl_rank, 
              usr.status AS user_status, 
              usr.userdb_id, 
              usr.first_name, 
              usr.last_name, 
              usr.team, 
              usr.authorisation_group, 
              usr.email_address, 
              usr.longitude AS user_longitude, 
              usr.latitude AS user_latitude, 
              vst.display_plan_id, 
              vst.display_type, 
              vst.display_code, 
              vst.display_name, 
              vst.start_date AS display_start_date, 
              vst.end_date AS display_end_date, 
              vst.checklist_method, 
              vst.display_number, 
              vst.comments AS display_comments 
            FROM 
              (
                (
                  (
                    (
                      SELECT 
                        edw_vw_pop6_visits_display.cntry_cd, 
                        edw_vw_pop6_visits_display.visit_id, 
                        edw_vw_pop6_visits_display.display_plan_id, 
                        edw_vw_pop6_visits_display.display_type, 
                        edw_vw_pop6_visits_display.display_code, 
                        edw_vw_pop6_visits_display.display_name, 
                        edw_vw_pop6_visits_display.start_date, 
                        edw_vw_pop6_visits_display.end_date, 
                        edw_vw_pop6_visits_display.checklist_method, 
                        edw_vw_pop6_visits_display.display_number, 
                        edw_vw_pop6_visits_display.product_attribute_id, 
                        edw_vw_pop6_visits_display.product_attribute, 
                        edw_vw_pop6_visits_display.product_attribute_value_id, 
                        edw_vw_pop6_visits_display.product_attribute_value, 
                        edw_vw_pop6_visits_display.comments, 
                        edw_vw_pop6_visits_display.field_id, 
                        edw_vw_pop6_visits_display.field_code, 
                        edw_vw_pop6_visits_display.field_label, 
                        edw_vw_pop6_visits_display.field_type, 
                        edw_vw_pop6_visits_display.dependent_on_field_id, 
                        edw_vw_pop6_visits_display.response, 
                        edw_vw_pop6_visits_display.visit_date, 
                        edw_vw_pop6_visits_display.check_in_datetime, 
                        edw_vw_pop6_visits_display.check_out_datetime, 
                        edw_vw_pop6_visits_display.popdb_id, 
                        edw_vw_pop6_visits_display.pop_code, 
                        edw_vw_pop6_visits_display.pop_name, 
                        edw_vw_pop6_visits_display.address, 
                        edw_vw_pop6_visits_display.check_in_longitude, 
                        edw_vw_pop6_visits_display.check_in_latitude, 
                        edw_vw_pop6_visits_display.check_out_longitude, 
                        edw_vw_pop6_visits_display.check_out_latitude, 
                        edw_vw_pop6_visits_display.check_in_photo, 
                        edw_vw_pop6_visits_display.check_out_photo, 
                        edw_vw_pop6_visits_display.username, 
                        edw_vw_pop6_visits_display.user_full_name, 
                        edw_vw_pop6_visits_display.superior_username, 
                        edw_vw_pop6_visits_display.superior_name, 
                        edw_vw_pop6_visits_display.planned_visit, 
                        edw_vw_pop6_visits_display.cancelled_visit, 
                        edw_vw_pop6_visits_display.cancellation_reason, 
                        edw_vw_pop6_visits_display.cancellation_note 
                      FROM 
                        edw_vw_pop6_visits_display 
                      WHERE 
                        (
                          (
                            edw_vw_pop6_visits_display.cntry_cd
                          ):: text = ('SG' :: character varying):: text
                        )
                    ) vst 
                    LEFT JOIN edw_vw_pop6_store pop ON (
                      (
                        (vst.popdb_id):: text = (pop.popdb_id):: text
                      )
                    )
                  ) 
                  LEFT JOIN edw_vw_pop6_salesperson usr ON (
                    (
                      (vst.username):: text = (usr.username):: text
                    )
                  )
                ) 
                LEFT JOIN (
                  SELECT 
                    DISTINCT NULL :: character varying AS brand_l4, 
                    (
                      upper(
                        (
                          edw_vw_pop6_products.ps_category
                        ):: text
                      )
                    ):: character varying AS ps_category, 
                    edw_vw_pop6_products.ps_segment, 
                    edw_vw_pop6_products.ps_category_segment, 
                    (
                      (
                        (
                          upper(
                            (
                              edw_vw_pop6_products.ps_category
                            ):: text
                          ) || ('_' :: character varying):: text
                        ) || (
                          edw_vw_pop6_products.ps_segment
                        ):: text
                      )
                    ):: character varying AS ps_categorysegement 
                  FROM 
                    edw_vw_pop6_products 
                  UNION 
                  SELECT 
                    DISTINCT edw_vw_pop6_products.brand_l4, 
                    NULL :: character varying AS ps_category, 
                    NULL :: character varying AS ps_segment, 
                    NULL :: character varying AS ps_category_segment, 
                    NULL :: character varying AS ps_categorysegement 
                  FROM 
                    edw_vw_pop6_products 
                  WHERE 
                    (
                      (edw_vw_pop6_products.cntry_cd):: text = ('SG' :: character varying):: text
                    )
                ) prd ON (
                  CASE WHEN (
                    (vst.product_attribute):: text = (
                      'PS Category Segment' :: character varying
                    ):: text
                  ) THEN (
                    (vst.product_attribute_value):: text = (prd.ps_categorysegement):: text
                  ) ELSE (
                    (vst.product_attribute_value):: text = (prd.brand_l4):: text
                  ) END
                )
              )
          ) srv
      ) ,
product_attribute_audit_4 as (
     
      SELECT 
        srv.data_type, 
        srv.cntry_cd, 
        srv.visit_id, 
        srv.task_group, 
        srv.task_id, 
        srv.task_name, 
        srv.audit_form_id, 
        srv.audit_form, 
        srv.section_id, 
        srv.section, 
        srv.subsection_id, 
        srv.subsection, 
        srv.field_id, 
        srv.field_code, 
        srv.field_label, 
        srv.field_type, 
        srv.dependent_on_field_id, 
        srv.sku_id, 
        srv.sku, 
        srv.response, 
        srv.visit_date, 
        srv.check_in_datetime, 
        srv.check_out_datetime, 
        srv.popdb_id, 
        srv.pop_code, 
        srv.pop_name, 
        srv.address, 
        srv.check_in_longitude, 
        srv.check_in_latitude, 
        srv.check_out_longitude, 
        srv.check_out_latitude, 
        srv.check_in_photo, 
        srv.check_out_photo, 
        srv.username, 
        srv.user_full_name, 
        srv.superior_username, 
        srv.superior_name, 
        srv.planned_visit, 
        srv.cancelled_visit, 
        srv.cancellation_reason, 
        srv.cancellation_note, 
        srv.promotion_plan_id, 
        srv.promotion_code, 
        srv.promotion_name, 
        srv.promotion_mechanics, 
        srv.promotion_type, 
        srv.promotion_price, 
        srv.promotion_compliance, 
        srv.actual_price, 
        srv.non_compliance_reason, 
        srv.photo, 
        srv.product_attribute_id, 
        srv.product_attribute, 
        srv.product_attribute_value_id, 
        srv.product_attribute_value, 
        srv.pop_status, 
        srv.pop_longitude, 
        srv.pop_latitude, 
        srv.country, 
        srv.channel, 
        srv.retail_environment_ps, 
        srv.customer, 
        srv.sales_group_code, 
        srv.sales_group_name, 
        srv.customer_grade, 
        srv.external_pop_code, 
        srv.business_unit_name, 
        srv.territory_or_region, 
        srv.prod_status, 
        srv.productdb_id, 
        srv.barcode, 
        srv.unit_price, 
        srv.display_order, 
        srv.launch_date, 
        srv.largest_uom_quantity, 
        srv.middle_uom_quantity, 
        srv.smallest_uom_quantity, 
        srv.company, 
        srv.sku_english, 
        srv.sku_code, 
        srv.ps_category, 
        srv.ps_segment, 
        srv.ps_category_segment, 
        srv.country_l1, 
        srv.regional_franchise_l2, 
        srv.franchise_l3, 
        srv.brand_l4, 
        srv.sub_category_l5, 
        srv.platform_l6, 
        srv.variance_l7, 
        srv.pack_size_l8, 
        srv.sap_matl_num, 
        srv.msl_rank, 
        srv.user_status, 
        srv.userdb_id, 
        srv.first_name, 
        srv.last_name, 
        srv.team, 
        srv.authorisation_group, 
        srv.email_address, 
        srv.user_longitude, 
        srv.user_latitude, 
        srv.display_plan_id, 
        srv.display_type, 
        srv.display_code, 
        srv.display_name, 
        srv.display_start_date, 
        srv.display_end_date, 
        srv.checklist_method, 
        srv.display_number, 
        srv.display_comments, 
        CASE WHEN (
          upper(
            (srv.response):: text
          ) = ('YES' :: character varying):: text
        ) THEN 'YES' :: character varying WHEN (
          upper(
            (srv.response):: text
          ) = ('NO' :: character varying):: text
        ) THEN 'NO' :: character varying ELSE srv.response END AS "y/n_flag", 
        mkt_shr."target" AS mkt_share, 
        NULL AS planned_visit_date, 
        NULL AS visited_flag, 
        NULL AS facing, 
        NULL AS is_eyelevel 
      FROM 
        (
          (
            SELECT 
              'PRODUCT_ATTIBUTE_AUDIT' :: character varying AS data_type, 
              vst.cntry_cd, 
              vst.visit_id, 
              NULL :: character varying AS task_group, 
              NULL :: integer AS task_id, 
              NULL :: character varying AS task_name, 
              vst.audit_form_id, 
              vst.audit_form, 
              vst.section_id, 
              vst.section, 
              NULL :: character varying AS subsection_id, 
              NULL :: character varying AS subsection, 
              vst.field_id, 
              vst.field_code, 
              vst.field_label, 
              vst.field_type, 
              vst.dependent_on_field_id, 
              NULL :: character varying AS sku_id, 
              NULL :: character varying AS sku, 
              vst.response, 
              vst.visit_date, 
              vst.check_in_datetime, 
              vst.check_out_datetime, 
              vst.popdb_id, 
              vst.pop_code, 
              vst.pop_name, 
              vst.address, 
              vst.check_in_longitude, 
              vst.check_in_latitude, 
              vst.check_out_longitude, 
              vst.check_out_latitude, 
              vst.check_in_photo, 
              vst.check_out_photo, 
              vst.username, 
              vst.user_full_name, 
              vst.superior_username, 
              vst.superior_name, 
              vst.planned_visit, 
              vst.cancelled_visit, 
              vst.cancellation_reason, 
              vst.cancellation_note, 
              NULL :: character varying AS promotion_plan_id, 
              NULL :: character varying AS promotion_code, 
              NULL :: character varying AS promotion_name, 
              NULL :: character varying AS promotion_mechanics, 
              NULL :: character varying AS promotion_type, 
              (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
              NULL :: character varying AS promotion_compliance, 
              (NULL :: numeric):: numeric(18, 0) AS actual_price, 
              NULL :: character varying AS non_compliance_reason, 
              NULL :: character varying AS photo, 
              vst.product_attribute_id, 
              vst.product_attribute, 
              vst.product_attribute_value_id, 
              vst.product_attribute_value, 
              pop.status AS pop_status, 
              pop.longitude AS pop_longitude, 
              pop.latitude AS pop_latitude, 
              pop.country, 
              pop.channel, 
              pop.retail_environment_ps, 
              pop.customer, 
              pop.sales_group_code, 
              pop.sales_group_name, 
              pop.customer_grade, 
              pop.external_pop_code, 
              pop.business_unit_name, 
              pop.territory_or_region, 
              NULL :: integer AS prod_status, 
              NULL :: character varying AS productdb_id, 
              NULL :: character varying AS barcode, 
              (NULL :: numeric):: numeric(18, 0) AS unit_price, 
              NULL :: integer AS display_order, 
              NULL :: character varying AS launch_date, 
              NULL :: integer AS largest_uom_quantity, 
              NULL :: integer AS middle_uom_quantity, 
              NULL :: integer AS smallest_uom_quantity, 
              NULL :: character varying AS company, 
              NULL :: character varying AS sku_english, 
              NULL :: character varying AS sku_code, 
              prd.ps_category, 
              prd.ps_segment, 
              prd.ps_category_segment, 
              NULL :: character varying AS country_l1, 
              NULL :: character varying AS regional_franchise_l2, 
              NULL :: character varying AS franchise_l3, 
              prd.brand_l4, 
              NULL :: character varying AS sub_category_l5, 
              NULL :: character varying AS platform_l6, 
              NULL :: character varying AS variance_l7, 
              NULL :: character varying AS pack_size_l8, 
              NULL :: character varying AS sap_matl_num, 
              NULL :: character varying AS msl_rank, 
              usr.status AS user_status, 
              usr.userdb_id, 
              usr.first_name, 
              usr.last_name, 
              usr.team, 
              usr.authorisation_group, 
              usr.email_address, 
              usr.longitude AS user_longitude, 
              usr.latitude AS user_latitude, 
              NULL :: character varying AS display_plan_id, 
              NULL :: character varying AS display_type, 
              NULL :: character varying AS display_code, 
              NULL :: character varying AS display_name, 
              NULL :: date AS display_start_date, 
              NULL :: date AS display_end_date, 
              NULL :: character varying AS checklist_method, 
              NULL :: integer AS display_number, 
              NULL :: character varying AS display_comments 
            FROM 
              (
                (
                  (
                    (
                      SELECT 
                        edw_vw_pop6_visits_prod_attribute_audits.cntry_cd, 
                        edw_vw_pop6_visits_prod_attribute_audits.visit_id, 
                        edw_vw_pop6_visits_prod_attribute_audits.audit_form_id, 
                        edw_vw_pop6_visits_prod_attribute_audits.audit_form, 
                        edw_vw_pop6_visits_prod_attribute_audits.section_id, 
                        edw_vw_pop6_visits_prod_attribute_audits.section, 
                        edw_vw_pop6_visits_prod_attribute_audits.field_id, 
                        edw_vw_pop6_visits_prod_attribute_audits.field_code, 
                        edw_vw_pop6_visits_prod_attribute_audits.field_label, 
                        edw_vw_pop6_visits_prod_attribute_audits.field_type, 
                        edw_vw_pop6_visits_prod_attribute_audits.dependent_on_field_id, 
                        edw_vw_pop6_visits_prod_attribute_audits.product_attribute_id, 
                        edw_vw_pop6_visits_prod_attribute_audits.product_attribute, 
                        edw_vw_pop6_visits_prod_attribute_audits.product_attribute_value_id, 
                        edw_vw_pop6_visits_prod_attribute_audits.product_attribute_value, 
                        edw_vw_pop6_visits_prod_attribute_audits.response, 
                        edw_vw_pop6_visits_prod_attribute_audits.visit_date, 
                        edw_vw_pop6_visits_prod_attribute_audits.check_in_datetime, 
                        edw_vw_pop6_visits_prod_attribute_audits.check_out_datetime, 
                        edw_vw_pop6_visits_prod_attribute_audits.popdb_id, 
                        edw_vw_pop6_visits_prod_attribute_audits.pop_code, 
                        edw_vw_pop6_visits_prod_attribute_audits.pop_name, 
                        edw_vw_pop6_visits_prod_attribute_audits.address, 
                        edw_vw_pop6_visits_prod_attribute_audits.check_in_longitude, 
                        edw_vw_pop6_visits_prod_attribute_audits.check_in_latitude, 
                        edw_vw_pop6_visits_prod_attribute_audits.check_out_longitude, 
                        edw_vw_pop6_visits_prod_attribute_audits.check_out_latitude, 
                        edw_vw_pop6_visits_prod_attribute_audits.check_in_photo, 
                        edw_vw_pop6_visits_prod_attribute_audits.check_out_photo, 
                        edw_vw_pop6_visits_prod_attribute_audits.username, 
                        edw_vw_pop6_visits_prod_attribute_audits.user_full_name, 
                        edw_vw_pop6_visits_prod_attribute_audits.superior_username, 
                        edw_vw_pop6_visits_prod_attribute_audits.superior_name, 
                        edw_vw_pop6_visits_prod_attribute_audits.planned_visit, 
                        edw_vw_pop6_visits_prod_attribute_audits.cancelled_visit, 
                        edw_vw_pop6_visits_prod_attribute_audits.cancellation_reason, 
                        edw_vw_pop6_visits_prod_attribute_audits.cancellation_note 
                      FROM 
                        edw_vw_pop6_visits_prod_attribute_audits 
                      WHERE 
                        (
                          (
                            edw_vw_pop6_visits_prod_attribute_audits.cntry_cd
                          ):: text = ('TH' :: character varying):: text
                        )
                    ) vst 
                    LEFT JOIN edw_vw_pop6_store pop ON (
                      (
                        (vst.popdb_id):: text = (pop.popdb_id):: text
                      )
                    )
                  ) 
                  LEFT JOIN edw_vw_pop6_salesperson usr ON (
                    (
                      (vst.username):: text = (usr.username):: text
                    )
                  )
                ) 
                LEFT JOIN (
                  SELECT 
                    DISTINCT NULL :: character varying AS brand_l4, 
                    (
                      upper(
                        (
                          edw_vw_pop6_products.ps_category
                        ):: text
                      )
                    ):: character varying AS ps_category, 
                    edw_vw_pop6_products.ps_segment, 
                    edw_vw_pop6_products.ps_category_segment, 
                    (
                      (
                        (
                          upper(
                            (
                              edw_vw_pop6_products.ps_category
                            ):: text
                          ) || ('_' :: character varying):: text
                        ) || (
                          edw_vw_pop6_products.ps_segment
                        ):: text
                      )
                    ):: character varying AS ps_categorysegement 
                  FROM 
                    edw_vw_pop6_products 
                  UNION 
                  SELECT 
                    DISTINCT edw_vw_pop6_products.brand_l4, 
                    NULL :: character varying AS ps_category, 
                    NULL :: character varying AS ps_segment, 
                    NULL :: character varying AS ps_category_segment, 
                    NULL :: character varying AS ps_categorysegement 
                  FROM 
                    edw_vw_pop6_products 
                  WHERE 
                    (
                      (edw_vw_pop6_products.cntry_cd):: text = ('TH' :: character varying):: text
                    )
                ) prd ON (
                  CASE WHEN (
                    (vst.product_attribute):: text = (
                      'PS Category Segment' :: character varying
                    ):: text
                  ) THEN (
                    (vst.product_attribute_value):: text = (prd.ps_categorysegement):: text
                  ) ELSE (
                    (vst.product_attribute_value):: text = (prd.brand_l4):: text
                  ) END
                )
              )
          ) srv 
          LEFT JOIN (
            SELECT 
              edw_vw_ps_targets.kpi, 
              edw_vw_ps_targets.attribute_1 AS category, 
              edw_vw_ps_targets.attribute_2 AS segment, 
              edw_vw_ps_targets.value AS "target" 
            FROM 
              edw_vw_ps_targets 
            WHERE 
              (
                (
                  upper(
                    (edw_vw_ps_targets.market):: text
                  ) = ('THAILAND' :: character varying):: text
                ) 
                AND (
                  upper(
                    (edw_vw_ps_targets.channel):: text
                  ) = (
                    'MODERN TRADE' :: character varying
                  ):: text
                )
              )
          ) mkt_shr ON (
            (
              (
                (
                  upper(
                    (mkt_shr.category):: text
                  ) = upper(
                    (srv.ps_category):: text
                  )
                ) 
                AND (
                  upper(
                    (mkt_shr.segment):: text
                  ) = upper(
                    (srv.ps_segment):: text
                  )
                )
              ) 
              AND (
                upper(
                  (mkt_shr.kpi):: text
                ) = (
                  CASE WHEN (
                    "substring"(
                      upper(
                        (srv.field_code):: text
                      ), 
                      0, 
                      7
                    ) = ('PS_SOS' :: character varying):: text
                  ) THEN 'SOS COMPLIANCE' :: character varying WHEN (
                    "substring"(
                      upper(
                        (srv.field_code):: text
                      ), 
                      0, 
                      6
                    ) = ('PS_SOA' :: character varying):: text
                  ) THEN 'SOA COMPLIANCE' :: character varying ELSE srv.field_code END
                ):: text
              )
            )
          )
        )
    ) ,
display_4 as (
    SELECT 
      srv.data_type, 
      srv.cntry_cd, 
      srv.visit_id, 
      srv.task_group, 
      srv.task_id, 
      srv.task_name, 
      srv.audit_form_id, 
      srv.audit_form, 
      srv.section_id, 
      srv.section, 
      srv.subsection_id, 
      srv.subsection, 
      srv.field_id, 
      srv.field_code, 
      srv.questiontext AS field_label, 
      srv.field_type, 
      srv.dependent_on_field_id, 
      srv.sku_id, 
      srv.sku, 
      srv.response, 
      srv.visit_date, 
      srv.check_in_datetime, 
      srv.check_out_datetime, 
      srv.popdb_id, 
      srv.pop_code, 
      srv.pop_name, 
      srv.address, 
      srv.check_in_longitude, 
      srv.check_in_latitude, 
      srv.check_out_longitude, 
      srv.check_out_latitude, 
      srv.check_in_photo, 
      srv.check_out_photo, 
      srv.username, 
      srv.user_full_name, 
      srv.superior_username, 
      srv.superior_name, 
      srv.planned_visit, 
      srv.cancelled_visit, 
      srv.cancellation_reason, 
      srv.cancellation_note, 
      srv.promotion_plan_id, 
      srv.promotion_code, 
      srv.promotion_name, 
      srv.promotion_mechanics, 
      srv.promotion_type, 
      srv.promotion_price, 
      srv.promotion_compliance, 
      srv.actual_price, 
      srv.non_compliance_reason, 
      srv.photo, 
      srv.product_attribute_id, 
      srv.product_attribute, 
      srv.product_attribute_value_id, 
      srv.product_attribute_value, 
      srv.pop_status, 
      srv.pop_longitude, 
      srv.pop_latitude, 
      srv.country, 
      srv.channel, 
      srv.retail_environment_ps, 
      srv.customer, 
      srv.sales_group_code, 
      srv.sales_group_name, 
      srv.customer_grade, 
      srv.external_pop_code, 
      srv.business_unit_name, 
      srv.territory_or_region, 
      srv.prod_status, 
      srv.productdb_id, 
      srv.barcode, 
      srv.unit_price, 
      srv.display_order, 
      srv.launch_date, 
      srv.largest_uom_quantity, 
      srv.middle_uom_quantity, 
      srv.smallest_uom_quantity, 
      srv.company, 
      srv.sku_english, 
      srv.sku_code, 
      srv.ps_category, 
      srv.ps_segment, 
      srv.ps_category_segment, 
      srv.country_l1, 
      srv.regional_franchise_l2, 
      srv.franchise_l3, 
      srv.brand_l4, 
      srv.sub_category_l5, 
      srv.platform_l6, 
      srv.variance_l7, 
      srv.pack_size_l8, 
      srv.sap_matl_num, 
      srv.msl_rank, 
      srv.user_status, 
      srv.userdb_id, 
      srv.first_name, 
      srv.last_name, 
      srv.team, 
      srv.authorisation_group, 
      srv.email_address, 
      srv.user_longitude, 
      srv.user_latitude, 
      srv.display_plan_id, 
      srv.display_type, 
      srv.display_code, 
      srv.display_name, 
      srv.display_start_date, 
      srv.display_end_date, 
      srv.checklist_method, 
      srv.display_number, 
      srv.display_comments, 
      CASE WHEN (
        upper(
          (srv.response):: text
        ) = ('YES' :: character varying):: text
      ) THEN 'YES' :: character varying WHEN (
        upper(
          (srv.response):: text
        ) = ('NO' :: character varying):: text
      ) THEN 'NO' :: character varying ELSE srv.response END AS "y/n_flag", 
      NULL AS mkt_share, 
      NULL AS planned_visit_date, 
      NULL AS visited_flag, 
      NULL AS facing, 
      NULL AS is_eyelevel 
    FROM 
      (
        SELECT 
          'DISPLAY' :: character varying AS data_type, 
          vst.cntry_cd, 
          vst.visit_id, 
          NULL :: character varying AS task_group, 
          NULL :: integer AS task_id, 
          NULL :: character varying AS task_name, 
          NULL :: character varying AS audit_form_id, 
          NULL :: character varying AS audit_form, 
          NULL :: character varying AS section_id, 
          NULL :: character varying AS section, 
          NULL :: character varying AS subsection_id, 
          NULL :: character varying AS subsection, 
          vst.field_id, 
          vst.field_code, 
          vst.field_label AS questiontext, 
          vst.field_type, 
          vst.dependent_on_field_id, 
          NULL :: character varying AS sku_id, 
          NULL :: character varying AS sku, 
          vst.response, 
          vst.visit_date, 
          vst.check_in_datetime, 
          vst.check_out_datetime, 
          vst.popdb_id, 
          vst.pop_code, 
          vst.pop_name, 
          vst.address, 
          vst.check_in_longitude, 
          vst.check_in_latitude, 
          vst.check_out_longitude, 
          vst.check_out_latitude, 
          vst.check_in_photo, 
          vst.check_out_photo, 
          vst.username, 
          vst.user_full_name, 
          vst.superior_username, 
          vst.superior_name, 
          vst.planned_visit, 
          vst.cancelled_visit, 
          vst.cancellation_reason, 
          vst.cancellation_note, 
          NULL :: character varying AS promotion_plan_id, 
          NULL :: character varying AS promotion_code, 
          NULL :: character varying AS promotion_name, 
          NULL :: character varying AS promotion_mechanics, 
          NULL :: character varying AS promotion_type, 
          (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
          NULL :: character varying AS promotion_compliance, 
          (NULL :: numeric):: numeric(18, 0) AS actual_price, 
          NULL :: character varying AS non_compliance_reason, 
          NULL :: character varying AS photo, 
          vst.product_attribute_id, 
          vst.product_attribute, 
          vst.product_attribute_value_id, 
          vst.product_attribute_value, 
          pop.status AS pop_status, 
          pop.longitude AS pop_longitude, 
          pop.latitude AS pop_latitude, 
          pop.country, 
          pop.channel, 
          pop.retail_environment_ps, 
          pop.customer, 
          pop.sales_group_code, 
          pop.sales_group_name, 
          pop.customer_grade, 
          pop.external_pop_code, 
          pop.business_unit_name, 
          pop.territory_or_region, 
          NULL :: integer AS prod_status, 
          NULL :: character varying AS productdb_id, 
          NULL :: character varying AS barcode, 
          (NULL :: numeric):: numeric(18, 0) AS unit_price, 
          NULL :: integer AS display_order, 
          NULL :: character varying AS launch_date, 
          NULL :: integer AS largest_uom_quantity, 
          NULL :: integer AS middle_uom_quantity, 
          NULL :: integer AS smallest_uom_quantity, 
          NULL :: character varying AS company, 
          NULL :: character varying AS sku_english, 
          NULL :: character varying AS sku_code, 
          prd.ps_category, 
          prd.ps_segment, 
          prd.ps_category_segment, 
          NULL :: character varying AS country_l1, 
          NULL :: character varying AS regional_franchise_l2, 
          NULL :: character varying AS franchise_l3, 
          prd.brand_l4, 
          NULL :: character varying AS sub_category_l5, 
          NULL :: character varying AS platform_l6, 
          NULL :: character varying AS variance_l7, 
          NULL :: character varying AS pack_size_l8, 
          NULL :: character varying AS sap_matl_num, 
          NULL :: character varying AS msl_rank, 
          usr.status AS user_status, 
          usr.userdb_id, 
          usr.first_name, 
          usr.last_name, 
          usr.team, 
          usr.authorisation_group, 
          usr.email_address, 
          usr.longitude AS user_longitude, 
          usr.latitude AS user_latitude, 
          vst.display_plan_id, 
          vst.display_type, 
          vst.display_code, 
          vst.display_name, 
          vst.start_date AS display_start_date, 
          vst.end_date AS display_end_date, 
          vst.checklist_method, 
          vst.display_number, 
          vst.comments AS display_comments 
        FROM 
          (
            (
              (
                (
                  SELECT 
                    edw_vw_pop6_visits_display.cntry_cd, 
                    edw_vw_pop6_visits_display.visit_id, 
                    edw_vw_pop6_visits_display.display_plan_id, 
                    edw_vw_pop6_visits_display.display_type, 
                    edw_vw_pop6_visits_display.display_code, 
                    edw_vw_pop6_visits_display.display_name, 
                    edw_vw_pop6_visits_display.start_date, 
                    edw_vw_pop6_visits_display.end_date, 
                    edw_vw_pop6_visits_display.checklist_method, 
                    edw_vw_pop6_visits_display.display_number, 
                    edw_vw_pop6_visits_display.product_attribute_id, 
                    edw_vw_pop6_visits_display.product_attribute, 
                    edw_vw_pop6_visits_display.product_attribute_value_id, 
                    edw_vw_pop6_visits_display.product_attribute_value, 
                    edw_vw_pop6_visits_display.comments, 
                    edw_vw_pop6_visits_display.field_id, 
                    edw_vw_pop6_visits_display.field_code, 
                    edw_vw_pop6_visits_display.field_label, 
                    edw_vw_pop6_visits_display.field_type, 
                    edw_vw_pop6_visits_display.dependent_on_field_id, 
                    edw_vw_pop6_visits_display.response, 
                    edw_vw_pop6_visits_display.visit_date, 
                    edw_vw_pop6_visits_display.check_in_datetime, 
                    edw_vw_pop6_visits_display.check_out_datetime, 
                    edw_vw_pop6_visits_display.popdb_id, 
                    edw_vw_pop6_visits_display.pop_code, 
                    edw_vw_pop6_visits_display.pop_name, 
                    edw_vw_pop6_visits_display.address, 
                    edw_vw_pop6_visits_display.check_in_longitude, 
                    edw_vw_pop6_visits_display.check_in_latitude, 
                    edw_vw_pop6_visits_display.check_out_longitude, 
                    edw_vw_pop6_visits_display.check_out_latitude, 
                    edw_vw_pop6_visits_display.check_in_photo, 
                    edw_vw_pop6_visits_display.check_out_photo, 
                    edw_vw_pop6_visits_display.username, 
                    edw_vw_pop6_visits_display.user_full_name, 
                    edw_vw_pop6_visits_display.superior_username, 
                    edw_vw_pop6_visits_display.superior_name, 
                    edw_vw_pop6_visits_display.planned_visit, 
                    edw_vw_pop6_visits_display.cancelled_visit, 
                    edw_vw_pop6_visits_display.cancellation_reason, 
                    edw_vw_pop6_visits_display.cancellation_note 
                  FROM 
                    edw_vw_pop6_visits_display 
                  WHERE 
                    (
                      (
                        edw_vw_pop6_visits_display.cntry_cd
                      ):: text = ('TH' :: character varying):: text
                    )
                ) vst 
                LEFT JOIN edw_vw_pop6_store pop ON (
                  (
                    (vst.popdb_id):: text = (pop.popdb_id):: text
                  )
                )
              ) 
              LEFT JOIN edw_vw_pop6_salesperson usr ON (
                (
                  (vst.username):: text = (usr.username):: text
                )
              )
            ) 
            LEFT JOIN (
              SELECT 
                DISTINCT NULL :: character varying AS brand_l4, 
                (
                  upper(
                    (
                      edw_vw_pop6_products.ps_category
                    ):: text
                  )
                ):: character varying AS ps_category, 
                edw_vw_pop6_products.ps_segment, 
                edw_vw_pop6_products.ps_category_segment, 
                (
                  (
                    (
                      upper(
                        (
                          edw_vw_pop6_products.ps_category
                        ):: text
                      ) || ('_' :: character varying):: text
                    ) || (
                      edw_vw_pop6_products.ps_segment
                    ):: text
                  )
                ):: character varying AS ps_categorysegement 
              FROM 
                edw_vw_pop6_products 
              UNION 
              SELECT 
                DISTINCT edw_vw_pop6_products.brand_l4, 
                NULL :: character varying AS ps_category, 
                NULL :: character varying AS ps_segment, 
                NULL :: character varying AS ps_category_segment, 
                NULL :: character varying AS ps_categorysegement 
              FROM 
                edw_vw_pop6_products 
              WHERE 
                (
                  (edw_vw_pop6_products.cntry_cd):: text = ('TH' :: character varying):: text
                )
            ) prd ON (
              CASE WHEN (
                (vst.product_attribute):: text = (
                  'PS Category Segment' :: character varying
                ):: text
              ) THEN (
                (vst.product_attribute_value):: text = (prd.ps_categorysegement):: text
              ) ELSE (
                (vst.product_attribute_value):: text = (prd.brand_l4):: text
              ) END
            )
          )
      ) srv
  ) ,
rir as (
  
  SELECT 
    srv.data_type, 
    srv.cntry_cd, 
    srv.visit_id, 
    srv.task_group, 
    srv.task_id, 
    srv.task_name, 
    srv.audit_form_id, 
    srv.audit_form, 
    srv.section_id, 
    srv.section, 
    srv.subsection_id, 
    srv.subsection, 
    srv.field_id, 
    srv.field_code, 
    srv.questiontext AS field_label, 
    srv.field_type, 
    srv.dependent_on_field_id, 
    srv.sku_id, 
    srv.sku, 
    srv.response, 
    srv.visit_date, 
    srv.check_in_datetime, 
    srv.check_out_datetime, 
    srv.popdb_id, 
    srv.pop_code, 
    srv.pop_name, 
    srv.address, 
    srv.check_in_longitude, 
    srv.check_in_latitude, 
    srv.check_out_longitude, 
    srv.check_out_latitude, 
    srv.check_in_photo, 
    srv.check_out_photo, 
    srv.username, 
    srv.user_full_name, 
    srv.superior_username, 
    srv.superior_name, 
    srv.planned_visit, 
    srv.cancelled_visit, 
    srv.cancellation_reason, 
    srv.cancellation_note, 
    srv.promotion_plan_id, 
    srv.promotion_code, 
    srv.promotion_name, 
    srv.promotion_mechanics, 
    srv.promotion_type, 
    srv.promotion_price, 
    srv.promotion_compliance, 
    srv.actual_price, 
    srv.non_compliance_reason, 
    srv.photo, 
    srv.product_attribute_id, 
    srv.product_attribute, 
    srv.product_attribute_value_id, 
    srv.product_attribute_value, 
    srv.pop_status, 
    srv.pop_longitude, 
    srv.pop_latitude, 
    srv.country, 
    srv.channel, 
    srv.retail_environment_ps, 
    srv.customer, 
    srv.sales_group_code, 
    srv.sales_group_name, 
    srv.customer_grade, 
    srv.external_pop_code, 
    srv.business_unit_name, 
    srv.territory_or_region, 
    srv.prod_status, 
    srv.productdb_id, 
    srv.barcode, 
    srv.unit_price, 
    srv.display_order, 
    srv.launch_date, 
    srv.largest_uom_quantity, 
    srv.middle_uom_quantity, 
    srv.smallest_uom_quantity, 
    srv.company, 
    srv.sku_english, 
    srv.sku_code, 
    srv.ps_category, 
    srv.ps_segment, 
    srv.ps_category_segment, 
    srv.country_l1, 
    srv.regional_franchise_l2, 
    srv.franchise_l3, 
    srv.brand_l4, 
    srv.sub_category_l5, 
    srv.platform_l6, 
    srv.variance_l7, 
    srv.pack_size_l8, 
    srv.sap_matl_num, 
    srv.msl_rank, 
    srv.user_status, 
    srv.userdb_id, 
    srv.first_name, 
    srv.last_name, 
    srv.team, 
    srv.authorisation_group, 
    srv.email_address, 
    srv.user_longitude, 
    srv.user_latitude, 
    srv.display_plan_id, 
    srv.display_type, 
    srv.display_code, 
    srv.display_name, 
    srv.display_start_date, 
    srv.display_end_date, 
    srv.checklist_method, 
    srv.display_number, 
    srv.display_comments, 
    CASE WHEN (
      upper(
        (srv.response):: text
      ) = ('YES' :: character varying):: text
    ) THEN 'YES' :: character varying WHEN (
      upper(
        (srv.response):: text
      ) = ('NO' :: character varying):: text
    ) THEN 'NO' :: character varying ELSE srv.response END AS "y/n_flag", 
    NULL AS mkt_share, 
    NULL AS planned_visit_date, 
    NULL AS visited_flag, 
    srv.facing, 
    srv.is_eyelevel 
  FROM 
    (
      SELECT 
        'RIR' :: character varying AS data_type, 
        vst.cntry_cd, 
        vst.visit_id, 
        NULL :: character varying AS task_group, 
        NULL :: integer AS task_id, 
        NULL :: character varying AS task_name, 
        NULL :: character varying AS audit_form_id, 
        NULL :: character varying AS audit_form, 
        NULL :: character varying AS section_id, 
        NULL :: character varying AS section, 
        NULL :: character varying AS subsection_id, 
        NULL :: character varying AS subsection, 
        vst.field_id, 
        vst.field_code, 
        vst.field_label AS questiontext, 
        vst.field_type, 
        vst.dependent_on_field_id, 
        rir.sku_id, 
        vst.sku, 
        vst.response, 
        vst.visit_date, 
        vst.check_in_datetime, 
        vst.check_out_datetime, 
        vst.popdb_id, 
        vst.pop_code, 
        vst.pop_name, 
        vst.address, 
        vst.check_in_longitude, 
        vst.check_in_latitude, 
        vst.check_out_longitude, 
        vst.check_out_latitude, 
        vst.check_in_photo, 
        vst.check_out_photo, 
        vst.username, 
        vst.user_full_name, 
        vst.superior_username, 
        vst.superior_name, 
        vst.planned_visit, 
        vst.cancelled_visit, 
        vst.cancellation_reason, 
        vst.cancellation_note, 
        NULL :: character varying AS promotion_plan_id, 
        NULL :: character varying AS promotion_code, 
        NULL :: character varying AS promotion_name, 
        NULL :: character varying AS promotion_mechanics, 
        NULL :: character varying AS promotion_type, 
        (NULL :: numeric):: numeric(18, 0) AS promotion_price, 
        NULL :: character varying AS promotion_compliance, 
        (NULL :: numeric):: numeric(18, 0) AS actual_price, 
        NULL :: character varying AS non_compliance_reason, 
        NULL :: character varying AS photo, 
        vst.product_attribute_id, 
        vst.product_attribute, 
        vst.product_attribute_value_id, 
        vst.product_attribute_value, 
        pop.status AS pop_status, 
        pop.longitude AS pop_longitude, 
        pop.latitude AS pop_latitude, 
        pop.country, 
        pop.channel, 
        pop.retail_environment_ps, 
        pop.customer, 
        pop.sales_group_code, 
        pop.sales_group_name, 
        pop.customer_grade, 
        pop.external_pop_code, 
        pop.business_unit_name, 
        pop.territory_or_region, 
        NULL :: integer AS prod_status, 
        vst.productdb_id, 
        prd.barcode, 
        (NULL :: numeric):: numeric(18, 0) AS unit_price, 
        NULL :: integer AS display_order, 
        NULL :: character varying AS launch_date, 
        NULL :: integer AS largest_uom_quantity, 
        NULL :: integer AS middle_uom_quantity, 
        NULL :: integer AS smallest_uom_quantity, 
        NULL :: character varying AS company, 
        NULL :: character varying AS sku_english, 
        NULL :: character varying AS sku_code, 
        prd.ps_category, 
        prd.ps_segment, 
        NULL :: character varying AS ps_category_segment, 
        prd.country_l1, 
        prd.regional_franchise_l2, 
        prd.franchise_l3, 
        prd.brand_l4, 
        prd.sub_category_l5, 
        prd.platform_l6, 
        prd.variance_l7, 
        prd.pack_size_l8, 
        NULL :: character varying AS sap_matl_num, 
        NULL :: character varying AS msl_rank, 
        usr.status AS user_status, 
        usr.userdb_id, 
        usr.first_name, 
        usr.last_name, 
        usr.team, 
        usr.authorisation_group, 
        usr.email_address, 
        usr.longitude AS user_longitude, 
        usr.latitude AS user_latitude, 
        NULL :: character varying AS display_plan_id, 
        NULL :: character varying AS display_type, 
        NULL :: character varying AS display_code, 
        NULL :: character varying AS display_name, 
        NULL :: date AS display_start_date, 
        NULL :: date AS display_end_date, 
        NULL :: character varying AS checklist_method, 
        NULL :: integer AS display_number, 
        NULL :: character varying AS display_comments, 
        rir.facing, 
        rir.is_eyelevel 
      FROM 
        (
          (
            (
              (
                (
                  SELECT 
                    edw_vw_pop6_visits_rir_data.cntry_cd, 
                    edw_vw_pop6_visits_rir_data.visit_id, 
                    NULL :: character varying AS display_plan_id, 
                    NULL :: character varying AS display_type, 
                    NULL :: character varying AS display_code, 
                    NULL :: character varying AS display_name, 
                    NULL :: date AS start_date, 
                    NULL :: date AS end_date, 
                    NULL :: character varying AS checklist_method, 
                    NULL :: character varying AS display_number, 
                    NULL :: character varying AS product_attribute_id, 
                    NULL :: character varying AS product_attribute, 
                    NULL :: character varying AS product_attribute_value_id, 
                    NULL :: character varying AS product_attribute_value, 
                    NULL :: character varying AS comments, 
                    NULL :: character varying AS field_id, 
                    NULL :: character varying AS field_code, 
                    NULL :: character varying AS field_label, 
                    NULL :: character varying AS field_type, 
                    NULL :: character varying AS dependent_on_field_id, 
                    NULL :: character varying AS response, 
                    edw_vw_pop6_visits_rir_data.visit_date, 
                    edw_vw_pop6_visits_rir_data.check_in_datetime, 
                    edw_vw_pop6_visits_rir_data.check_out_datetime, 
                    edw_vw_pop6_visits_rir_data.popdb_id, 
                    edw_vw_pop6_visits_rir_data.pop_code, 
                    edw_vw_pop6_visits_rir_data.pop_name, 
                    edw_vw_pop6_visits_rir_data.address, 
                    edw_vw_pop6_visits_rir_data.check_in_longitude, 
                    edw_vw_pop6_visits_rir_data.check_in_latitude, 
                    edw_vw_pop6_visits_rir_data.check_out_longitude, 
                    edw_vw_pop6_visits_rir_data.check_out_latitude, 
                    edw_vw_pop6_visits_rir_data.check_in_photo, 
                    edw_vw_pop6_visits_rir_data.check_out_photo, 
                    edw_vw_pop6_visits_rir_data.username, 
                    edw_vw_pop6_visits_rir_data.user_full_name, 
                    edw_vw_pop6_visits_rir_data.superior_username, 
                    edw_vw_pop6_visits_rir_data.superior_name, 
                    edw_vw_pop6_visits_rir_data.planned_visit, 
                    edw_vw_pop6_visits_rir_data.cancelled_visit, 
                    edw_vw_pop6_visits_rir_data.cancellation_reason, 
                    edw_vw_pop6_visits_rir_data.cancellation_note, 
                    edw_vw_pop6_visits_rir_data.productdb_id, 
                    edw_vw_pop6_visits_rir_data.sku 
                  FROM 
                    edw_vw_pop6_visits_rir_data 
                  WHERE 
                    (
                      (
                        edw_vw_pop6_visits_rir_data.cntry_cd
                      ):: text = ('TH' :: character varying):: text
                    )
                ) vst 
                LEFT JOIN itg_pop6_rir_data rir ON (
                  (
                    (
                      (rir.visit_id):: text = (vst.visit_id):: text
                    ) 
                    AND (
                      (rir.sku_id):: text = (vst.productdb_id):: text
                    )
                  )
                )
              ) 
              LEFT JOIN edw_vw_pop6_store pop ON (
                (
                  (vst.popdb_id):: text = (pop.popdb_id):: text
                )
              )
            ) 
            LEFT JOIN edw_vw_pop6_salesperson usr ON (
              (
                (vst.username):: text = (usr.username):: text
              )
            )
          ) 
          LEFT JOIN (
            SELECT 
              edw_vw_pop6_products.cntry_cd, 
              edw_vw_pop6_products.src_file_date, 
              edw_vw_pop6_products.status, 
              edw_vw_pop6_products.productdb_id, 
              edw_vw_pop6_products.barcode, 
              edw_vw_pop6_products.sku, 
              edw_vw_pop6_products.unit_price, 
              edw_vw_pop6_products.display_order, 
              edw_vw_pop6_products.launch_date, 
              edw_vw_pop6_products.largest_uom_quantity, 
              edw_vw_pop6_products.middle_uom_quantity, 
              edw_vw_pop6_products.smallest_uom_quantity, 
              edw_vw_pop6_products.company, 
              edw_vw_pop6_products.sku_english, 
              edw_vw_pop6_products.sku_code, 
              edw_vw_pop6_products.ps_category, 
              edw_vw_pop6_products.ps_segment, 
              edw_vw_pop6_products.ps_category_segment, 
              edw_vw_pop6_products.country_l1, 
              edw_vw_pop6_products.regional_franchise_l2, 
              edw_vw_pop6_products.franchise_l3, 
              edw_vw_pop6_products.brand_l4, 
              edw_vw_pop6_products.sub_category_l5, 
              edw_vw_pop6_products.platform_l6, 
              edw_vw_pop6_products.variance_l7, 
              edw_vw_pop6_products.pack_size_l8 
            FROM 
              edw_vw_pop6_products 
            WHERE 
              (
                (edw_vw_pop6_products.cntry_cd):: text = ('TH' :: character varying):: text
              )
          ) prd ON (
            (
              (vst.productdb_id):: text = (prd.productdb_id):: text
            )
          )
        )
    ) srv
) ,
planned_visits as (SELECT 
  'PLANNED VISITS' AS data_type, 
  plv.cntry_cd, 
  NULL AS visit_id, 
  NULL AS task_group, 
  NULL AS task_id, 
  NULL AS task_name, 
  NULL AS audit_form_id, 
  NULL AS audit_form, 
  NULL AS section_id, 
  NULL AS section, 
  NULL AS subsection_id, 
  NULL AS subsection, 
  NULL AS field_id, 
  NULL AS field_code, 
  NULL AS field_label, 
  NULL AS field_type, 
  NULL AS dependent_on_field_id, 
  NULL AS sku_id, 
  NULL AS sku, 
  NULL AS response, 
  exv.visit_date, 
  NULL AS check_in_datetime, 
  NULL AS check_out_datetime, 
  plv.popdb_id, 
  plv.pop_code, 
  plv.pop_name, 
  plv.address, 
  NULL AS check_in_longitude, 
  NULL AS check_in_latitude, 
  NULL AS check_out_longitude, 
  NULL AS check_out_latitude, 
  NULL AS check_in_photo, 
  NULL AS check_out_photo, 
  plv.username, 
  plv.user_full_name, 
  NULL AS superior_username, 
  NULL AS superior_name, 
  exv.planned_visit, 
  NULL AS cancelled_visit, 
  NULL AS cancellation_reason, 
  NULL AS cancellation_note, 
  NULL AS promotion_plan_id, 
  NULL AS promotion_code, 
  NULL AS promotion_name, 
  NULL AS promotion_mechanics, 
  NULL AS promotion_type, 
  NULL AS promotion_price, 
  NULL AS promotion_compliance, 
  NULL AS actual_price, 
  NULL AS non_compliance_reason, 
  NULL AS photo, 
  NULL AS product_attribute_id, 
  NULL AS product_attribute, 
  NULL AS product_attribute_value_id, 
  NULL AS product_attribute_value, 
  st.status AS pop_status, 
  st.longitude AS pop_longitude, 
  st.latitude AS pop_latitude, 
  st.country, 
  st.channel, 
  st.retail_environment_ps, 
  st.customer, 
  st.sales_group_code, 
  st.sales_group_name, 
  st.customer_grade, 
  st.external_pop_code, 
  st.business_unit_name, 
  st.territory_or_region, 
  NULL AS prod_status, 
  NULL AS productdb_id, 
  NULL AS barcode, 
  NULL AS unit_price, 
  NULL AS display_order, 
  NULL AS launch_date, 
  NULL AS largest_uom_quantity, 
  NULL AS middle_uom_quantity, 
  NULL AS smallest_uom_quantity, 
  NULL AS company, 
  NULL AS sku_english, 
  NULL AS sku_code, 
  NULL AS ps_category, 
  NULL AS ps_segment, 
  NULL AS ps_category_segment, 
  NULL AS country_l1, 
  NULL AS regional_franchise_l2, 
  NULL AS franchise_l3, 
  NULL AS brand_l4, 
  NULL AS sub_category_l5, 
  NULL AS platform_l6, 
  NULL AS variance_l7, 
  NULL AS pack_size_l8, 
  NULL AS sap_matl_num, 
  NULL AS msl_rank, 
  sp.status AS user_status, 
  sp.userdb_id, 
  sp.first_name, 
  sp.last_name, 
  sp.team, 
  sp.authorisation_group, 
  sp.email_address, 
  sp.longitude AS user_longitude, 
  sp.latitude AS user_latitude, 
  NULL AS display_plan_id, 
  NULL AS display_type, 
  NULL AS display_code, 
  NULL AS display_name, 
  NULL AS display_start_date, 
  NULL AS display_end_date, 
  NULL AS checklist_method, 
  NULL AS display_number, 
  NULL AS display_comments, 
  NULL AS "y/n_flag", 
  NULL AS mkt_share, 
  plv.planned_visit_date, 
  CASE WHEN (
    (
      (
        (exv.visit_date):: character varying
      ):: text <> (NULL :: character varying):: text
    ) 
    OR (
      (
        (exv.visit_date):: character varying
      ):: text <> ('' :: character varying):: text
    )
  ) THEN 'Y' :: character varying ELSE 'N' :: character varying END AS visited_flag, 
  NULL AS facing, 
  NULL AS is_eyelevel 
FROM 
  (
    (
      (
        edw_vw_pop6_planned_visits plv 
        LEFT JOIN edw_vw_pop6_salesperson sp ON (
          (
            (sp.username):: text = (plv.username):: text
          )
        )
      ) 
      LEFT JOIN edw_vw_pop6_store st ON (
        (
          (st.popdb_id):: text = (plv.popdb_id):: text
        )
      )
    ) 
    LEFT JOIN (
      SELECT 
        itg_pop6_executed_visits.cntry_cd, 
        itg_pop6_executed_visits.src_file_date, 
        itg_pop6_executed_visits.visit_id, 
        itg_pop6_executed_visits.visit_date, 
        itg_pop6_executed_visits.check_in_datetime, 
        itg_pop6_executed_visits.check_out_datetime, 
        itg_pop6_executed_visits.popdb_id, 
        itg_pop6_executed_visits.pop_code, 
        itg_pop6_executed_visits.pop_name, 
        itg_pop6_executed_visits.address, 
        itg_pop6_executed_visits.check_in_longitude, 
        itg_pop6_executed_visits.check_in_latitude, 
        itg_pop6_executed_visits.check_out_longitude, 
        itg_pop6_executed_visits.check_out_latitude, 
        itg_pop6_executed_visits.check_in_photo, 
        itg_pop6_executed_visits.check_out_photo, 
        itg_pop6_executed_visits.username, 
        itg_pop6_executed_visits.user_full_name, 
        itg_pop6_executed_visits.superior_username, 
        itg_pop6_executed_visits.superior_name, 
        itg_pop6_executed_visits.planned_visit, 
        itg_pop6_executed_visits.cancelled_visit, 
        itg_pop6_executed_visits.cancellation_reason, 
        itg_pop6_executed_visits.cancellation_note, 
        itg_pop6_executed_visits.file_name, 
        itg_pop6_executed_visits.run_id, 
        itg_pop6_executed_visits.crtd_dttm, 
        itg_pop6_executed_visits.updt_dttm 
      FROM 
        itg_pop6_executed_visits 
      WHERE 
        (
          itg_pop6_executed_visits.planned_visit = 1
        )
    ) exv ON (
      (
        (
          (
            exv.visit_date = plv.planned_visit_date
          ) 
          AND (
            (exv.popdb_id):: text = (plv.popdb_id):: text
          )
        ) 
        AND (
          (exv.username):: text = (plv.username):: text
        )
      )
    )
  )
),
final as (
select * from sku_audit
union 
select * from tasks
union
select * from promotions
union
select * from product_attribute_audit
union
select * from display
union
select * from product_attribute_audit_2
union
select * from display_2
union
select * from product_attribute_audit_3
union
select * from display_3
union
select * from product_attribute_audit_4
union
select * from display_4
union 
select * from rir
union all
select * from planned_visits
)
select * from final