with itg_pop6_executed_visits as (
  select 
    * 
  from 
    {{ ref('aspitg_integration__itg_pop6_executed_visits') }}
), 

itg_pop6_exclusion as (
  select 
    * 
  from 
    {{ ref('aspitg_integration__itg_pop6_exclusion') }}
), 

itg_pop6_displays as (
  select 
    * 
  from 
    {{ ref('aspitg_integration__itg_pop6_displays') }}
), 

disp as (
  SELECT 
    disp.cntry_cd, 
    disp.src_file_date, 
    disp.visit_id, 
    disp.display_plan_id, 
    disp.display_type, 
    disp.display_code, 
    disp.display_name, 
    disp.start_date, 
    disp.end_date, 
    disp.checklist_method, 
    disp.display_number, 
    disp.product_attribute_id, 
    disp.product_attribute, 
    disp.product_attribute_value_id, 
    disp.product_attribute_value, 
    disp.comments, 
    disp.field_id, 
    disp.field_code, 
    disp.field_label, 
    disp.field_type, 
    disp.dependent_on_field_id, 
    disp.response, 
    disp.rn 
  FROM 
    (
      SELECT 
        itg_pop6_displays.cntry_cd, 
        itg_pop6_displays.src_file_date, 
        itg_pop6_displays.visit_id, 
        itg_pop6_displays.display_plan_id, 
        itg_pop6_displays.display_type, 
        itg_pop6_displays.display_code, 
        itg_pop6_displays.display_name, 
        itg_pop6_displays.start_date, 
        itg_pop6_displays.end_date, 
        itg_pop6_displays.checklist_method, 
        itg_pop6_displays.display_number, 
        itg_pop6_displays.product_attribute_id, 
        itg_pop6_displays.product_attribute, 
        itg_pop6_displays.product_attribute_value_id, 
        itg_pop6_displays.product_attribute_value, 
        itg_pop6_displays.comments, 
        itg_pop6_displays.field_id, 
        itg_pop6_displays.field_code, 
        itg_pop6_displays.field_label, 
        itg_pop6_displays.field_type, 
        itg_pop6_displays.dependent_on_field_id, 
        itg_pop6_displays.response, 
        row_number() OVER (
          PARTITION BY itg_pop6_displays.visit_id, 
          itg_pop6_displays.display_plan_id, 
          itg_pop6_displays.field_id, 
          itg_pop6_displays.dependent_on_field_id 
          ORDER BY 
            itg_pop6_displays.run_id DESC
        ) AS rn 
      FROM 
        itg_pop6_displays
    ) disp 
  WHERE 
    disp.rn = 1
), 

visit AS (
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
  FROM 
    (
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
          itg_pop6_executed_visits.popdb_id 
          ORDER BY 
            itg_pop6_executed_visits.run_id DESC
        ) AS rn 
      FROM 
        itg_pop6_executed_visits 
      WHERE 
        NOT (
          (
            itg_pop6_executed_visits.cntry_cd, 
            itg_pop6_executed_visits.visit_date, 
            itg_pop6_executed_visits.pop_code, 
            itg_pop6_executed_visits.username
          ) IN (
            SELECT 
              itg_pop6_exclusion.country, 
              itg_pop6_exclusion.visit_date, 
              itg_pop6_exclusion.pop_code, 
              itg_pop6_exclusion.merchandiser_userid 
            FROM 
              itg_pop6_exclusion 
            WHERE 
              itg_pop6_exclusion.audit_form_name IS NULL 
              AND itg_pop6_exclusion.section_name IS NULL
          )
        )
    ) visit 
  WHERE 
    visit.rn = 1
), 

final as (
  SELECT 
    visit.cntry_cd, 
    visit.visit_id, 
    disp.display_plan_id, 
    disp.display_type, 
    disp.display_code, 
    disp.display_name, 
    disp.start_date, 
    disp.end_date, 
    disp.checklist_method, 
    disp.display_number, 
    disp.product_attribute_id, 
    disp.product_attribute, 
    disp.product_attribute_value_id, 
    disp.product_attribute_value, 
    disp.comments, 
    disp.field_id, 
    disp.field_code, 
    disp.field_label, 
    disp.field_type, 
    disp.dependent_on_field_id, 
    disp.response, 
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
  FROM 
    visit 
    LEFT JOIN disp ON disp.visit_id :: TEXT = visit.visit_id :: TEXT
) 

select 
  * 
from 
  final
