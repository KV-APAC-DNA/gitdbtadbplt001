with itg_pop6_executed_visits as (
    select * from {{ ref('aspitg_integration__itg_pop6_executed_visits') }}
),
itg_pop6_product_attribute_audits as (
    select * from {{ ref('aspitg_integration__itg_pop6_product_attribute_audits') }}
),
audits as (
    SELECT itg_pop6_product_attribute_audits.cntry_cd,
        itg_pop6_product_attribute_audits.src_file_date,
        itg_pop6_product_attribute_audits.visit_id,
        itg_pop6_product_attribute_audits.audit_form_id,
        itg_pop6_product_attribute_audits.audit_form,
        itg_pop6_product_attribute_audits.section_id,
        itg_pop6_product_attribute_audits.section,
        itg_pop6_product_attribute_audits.field_id,
        itg_pop6_product_attribute_audits.field_code,
        itg_pop6_product_attribute_audits.field_label,
        itg_pop6_product_attribute_audits.field_type,
        itg_pop6_product_attribute_audits.dependent_on_field_id,
        itg_pop6_product_attribute_audits.product_attribute_id,
        itg_pop6_product_attribute_audits.product_attribute,
        itg_pop6_product_attribute_audits.product_attribute_value_id,
        itg_pop6_product_attribute_audits.product_attribute_value,
        itg_pop6_product_attribute_audits.response
    FROM itg_pop6_product_attribute_audits
    WHERE (
            (itg_pop6_product_attribute_audits.cntry_cd)::text = 'TH'::text
        )
),
visit as (
    SELECT visit.cntry_cd,
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
            SELECT itg_pop6_executed_visits.cntry_cd,
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
                pg_catalog.row_number() OVER(
                    PARTITION BY itg_pop6_executed_visits.visit_id,
                    itg_pop6_executed_visits.popdb_id
                    ORDER BY itg_pop6_executed_visits.run_id DESC
                ) AS rn
            FROM itg_pop6_executed_visits
            WHERE (
                    (itg_pop6_executed_visits.cntry_cd)::text = 'TH'::text
                )
        ) visit
    WHERE (visit.rn = 1)
),
final as (
    SELECT visit.cntry_cd,
        visit.visit_id,
        audits.audit_form_id,
        audits.audit_form,
        audits.section_id,
        audits.section,
        audits.field_id,
        audits.field_code,
        audits.field_label,
        audits.field_type,
        audits.dependent_on_field_id,
        audits.product_attribute_id,
        audits.product_attribute,
        audits.product_attribute_value_id,
        audits.product_attribute_value,
        audits.response,
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
    FROM visit
        LEFT JOIN audits ON ((audits.visit_id)::text = (visit.visit_id)::text)
)
select * from final