with itg_pop6_promotions AS
(
       select * from {{ ref('aspitg_integration__itg_pop6_promotions') }}
),

itg_pop6_executed_visits AS
(
       select * from {{ ref('aspitg_integration__itg_pop6_executed_visits') }}
),

itg_pop6_exclusion AS
(
       select * from {{ ref('aspitg_integration__itg_pop6_exclusion') }}
),

visit AS
(
       SELECT 
              visit.cntry_cd,
              visit.src_file_date,
              visit.visit_id,
              visit.visit_date,
              visit.check_in_datetime,
              visit.check_out_datetime,
              visit.popdb_id,
              visit.pop_code,
              visit.pop_name,
              visit.address,
              visit.check_in_longitude,
              visit.check_in_latitude,
              visit.check_out_longitude,
              visit.check_out_latitude,
              visit.check_in_photo,
              visit.check_out_photo,
              visit.username,
              visit.user_full_name,
              visit.superior_username,
              visit.superior_name,
              visit.planned_visit,
              visit.cancelled_visit,
              visit.cancellation_reason,
              visit.cancellation_note,
              visit.rn
       FROM (
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
                     row_number() OVER (
                            PARTITION BY itg_pop6_executed_visits.visit_id,
                            itg_pop6_executed_visits.popdb_id ORDER BY itg_pop6_executed_visits.run_id DESC
                            ) AS rn
              FROM itg_pop6_executed_visits
              WHERE NOT (
                            (
                                   itg_pop6_executed_visits.cntry_cd,
                                   itg_pop6_executed_visits.visit_date,
                                   itg_pop6_executed_visits.pop_code,
                                   itg_pop6_executed_visits.username
                                   ) IN (
                                   SELECT itg_pop6_exclusion.country,
                                          itg_pop6_exclusion.visit_date,
                                          itg_pop6_exclusion.pop_code,
                                          itg_pop6_exclusion.merchandiser_userid
                                   FROM itg_pop6_exclusion
                                   WHERE itg_pop6_exclusion.audit_form_name IS NULL
                                          AND itg_pop6_exclusion.section_name IS NULL
                                   )
                            )
              ) visit
       WHERE visit.rn = 1
),

promotions AS
(
       SELECT 
              promotions.cntry_cd,
              promotions.src_file_date,
              promotions.visit_id,
              promotions.promotion_plan_id,
              promotions.promotion_code,
              promotions.promotion_name,
              promotions.promotion_mechanics,
              promotions.promotion_type,
              promotions.start_date,
              promotions.end_date,
              promotions.product_attribute_id,
              promotions.product_attribute,
              promotions.product_attribute_value_id,
              promotions.product_attribute_value,
              promotions.promotion_price,
              promotions.promotion_compliance,
              promotions.actual_price,
              promotions.non_compliance_reason,
              promotions.photo,
              promotions.file_name,
              promotions.run_id,
              promotions.crtd_dttm,
              promotions.updt_dttm,
              promotions.rn
       FROM (
              SELECT 
                     itg_pop6_promotions.cntry_cd,
                     itg_pop6_promotions.src_file_date,
                     itg_pop6_promotions.visit_id,
                     itg_pop6_promotions.promotion_plan_id,
                     itg_pop6_promotions.promotion_code,
                     itg_pop6_promotions.promotion_name,
                     itg_pop6_promotions.promotion_mechanics,
                     itg_pop6_promotions.promotion_type,
                     itg_pop6_promotions.start_date,
                     itg_pop6_promotions.end_date,
                     itg_pop6_promotions.product_attribute_id,
                     itg_pop6_promotions.product_attribute,
                     itg_pop6_promotions.product_attribute_value_id,
                     itg_pop6_promotions.product_attribute_value,
                     itg_pop6_promotions.promotion_price,
                     itg_pop6_promotions.promotion_compliance,
                     itg_pop6_promotions.actual_price,
                     itg_pop6_promotions.non_compliance_reason,
                     itg_pop6_promotions.photo,
                     itg_pop6_promotions.file_name,
                     itg_pop6_promotions.run_id,
                     itg_pop6_promotions.crtd_dttm,
                     itg_pop6_promotions.updt_dttm,
                     row_number() OVER (
                            PARTITION BY itg_pop6_promotions.visit_id,
                            itg_pop6_promotions.promotion_plan_id,
                            itg_pop6_promotions.start_date,
                            itg_pop6_promotions.end_date,
                            itg_pop6_promotions.product_attribute_value_id ORDER BY itg_pop6_promotions.run_id DESC
                            ) AS rn
              FROM itg_pop6_promotions
              ) promotions
       WHERE promotions.rn = 1
),



final AS
(
       SELECT 
              promotions.cntry_cd,
              promotions.visit_id,
              promotions.promotion_plan_id,
              promotions.promotion_code,
              promotions.promotion_name,
              promotions.promotion_mechanics,
              promotions.promotion_type,
              promotions.start_date,
              promotions.end_date,
              promotions.product_attribute_id,
              promotions.product_attribute,
              promotions.product_attribute_value_id,
              promotions.product_attribute_value,
              promotions.promotion_price,
              promotions.promotion_compliance,
              promotions.actual_price,
              promotions.non_compliance_reason,
              promotions.photo,
              visit.visit_date,
              visit.check_in_datetime,
              visit.check_out_datetime,
              visit.popdb_id,
              visit.pop_code,
              visit.pop_name,
              visit.address,
              visit.check_in_longitude,
              visit.check_in_latitude,
              visit.check_out_longitude,
              visit.check_out_latitude,
              visit.check_in_photo,
              visit.check_out_photo,
              visit.username,
              visit.user_full_name,
              visit.superior_username,
              visit.superior_name,
              visit.planned_visit,
              visit.cancelled_visit,
              visit.cancellation_reason,
              visit.cancellation_note
       FROM promotions
       LEFT JOIN visit ON promotions.visit_id::TEXT = visit.visit_id::TEXT
)

select * from final