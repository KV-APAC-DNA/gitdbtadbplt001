with itg_pop6_general_audits as (
    select * from {{ ref('ntaitg_integration__itg_pop6_general_audits') }}
),
itg_pop6_executed_visits as (
    select * from {{ ref('ntaitg_integration__itg_pop6_executed_visits') }}
),
itg_pop6_exclusion as (
    select * from {{ ref('ntaitg_integration__itg_pop6_exclusion') }}
),
gen as (
    SELECT gen.cntry_cd,
        gen.src_file_date,
        gen.visit_id,
        gen.audit_form_id,
        gen.audit_form,
        gen.section_id,
        gen.section,
        gen.subsection_id,
        gen.subsection,
        gen.field_id,
        gen.field_code,
        gen.field_label,
        gen.field_type,
        gen.dependent_on_field_id,
        gen.response,
        gen.rn
    FROM (
            SELECT itg_pop6_general_audits.cntry_cd,
                itg_pop6_general_audits.src_file_date,
                itg_pop6_general_audits.visit_id,
                itg_pop6_general_audits.audit_form_id,
                itg_pop6_general_audits.audit_form,
                itg_pop6_general_audits.section_id,
                itg_pop6_general_audits.section,
                itg_pop6_general_audits.subsection_id,
                itg_pop6_general_audits.subsection,
                itg_pop6_general_audits.field_id,
                itg_pop6_general_audits.field_code,
                itg_pop6_general_audits.field_label,
                itg_pop6_general_audits.field_type,
                itg_pop6_general_audits.dependent_on_field_id,
                itg_pop6_general_audits.response,
                row_number() OVER(
                    PARTITION BY itg_pop6_general_audits.visit_id,
                    itg_pop6_general_audits.audit_form_id,
                    itg_pop6_general_audits.section_id,
                    itg_pop6_general_audits.subsection_id,
                    itg_pop6_general_audits.field_id,
                    itg_pop6_general_audits.dependent_on_field_id
                    ORDER BY itg_pop6_general_audits.run_id DESC
                ) AS rn
            FROM itg_pop6_general_audits
        ) gen
    WHERE (gen.rn = 1)
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
                row_number() OVER(
                    PARTITION BY itg_pop6_executed_visits.visit_id,
                    itg_pop6_executed_visits.popdb_id
                    ORDER BY itg_pop6_executed_visits.run_id DESC
                ) AS rn
            FROM itg_pop6_executed_visits
            WHERE (
                    NOT (
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
                            WHERE (
                                    (itg_pop6_exclusion.audit_form_name IS NULL)
                                    AND (itg_pop6_exclusion.section_name IS NULL)
                                )
                        )
                    )
                )
        ) visit
    WHERE (visit.rn = 1)
),
final as (
    SELECT visit.cntry_cd,
        visit.visit_id,
        gen.audit_form_id,
        gen.audit_form,
        gen.section_id,
        gen.section,
        gen.subsection_id,
        gen.subsection,
        gen.field_id,
        gen.field_code,
        gen.field_label,
        gen.field_type,
        gen.dependent_on_field_id,
        gen.response,
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
    FROM gen
        LEFT JOIN visit ON (((gen.visit_id)::text = (visit.visit_id)::text))
)
select * from final