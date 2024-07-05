{{
    config
        (
            materialized="incremental",
            incremental_strategy= "append"
        )
}}
with sdl_pop6_kr_general_audits as
(
    select * from {{ref('aspwks_integration__wks_pop6_kr_general_audits')}}
),
sdl_pop6_tw_general_audits as
(
    select * from {{ref('aspwks_integration__wks_pop6_tw_general_audits')}}
),
sdl_pop6_hk_general_audits as
(
    select * from {{ref('aspwks_integration__wks_pop6_hk_general_audits')}}
),
sdl_pop6_jp_general_audits as
(
    select * from {{ref('aspwks_integration__wks_pop6_jp_general_audits')}}
),
sdl_pop6_sg_general_audits as
(
    select * from {{ref('aspwks_integration__wks_pop6_sg_general_audits')}}
),
sdl_pop6_th_general_audits as
(
    select * from {{ref('aspwks_integration__wks_pop6_th_general_audits')}}
),
transformed as 
(    
    SELECT 
        'KR' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        subsection_id as subsection_id,
        subsection as subsection,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm
    FROM sdl_pop6_kr_general_audits
    union
    SELECT 
        'TW' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        subsection_id as subsection_id,
        subsection as subsection,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm
    FROM sdl_pop6_tw_general_audits
    union
    SELECT
        'HK' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        subsection_id as subsection_id,
        subsection as subsection,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm
    FROM sdl_pop6_hk_general_audits
    union
    SELECT 
        'JP' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        subsection_id as subsection_id,
        subsection as subsection,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm
    FROM sdl_pop6_jp_general_audits
    union
    SELECT 
        'SG' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        subsection_id as subsection_id,
        subsection as subsection,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm
    FROM sdl_pop6_sg_general_audits
    union
    SELECT 
        'TH' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        subsection_id as subsection_id,
        subsection as subsection,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm,
    FROM sdl_pop6_th_general_audits
),
final as 
(
    select 
        cntry_cd::varchar(10) as cntry_cd,
        src_file_date::varchar(10) as src_file_date,
        visit_id::varchar(255) as visit_id,
        audit_form_id::varchar(255) as audit_form_id,
        audit_form::varchar(255) as audit_form,
        section_id::varchar(255) as section_id,
        section::varchar(255) as section,
        subsection_id::varchar(255) as subsection_id,
        subsection::varchar(255) as subsection,
        field_id::varchar(255) as field_id,
        field_code::varchar(255) as field_code,
        field_label::varchar(255) as field_label,
        field_type::varchar(50) as field_type,
        dependent_on_field_id::varchar(255) as dependent_on_field_id,
        response::varchar(65535) as response,
        file_name::varchar(100) as file_name,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where file_name not in (select distinct file_name from {{ this }}) 
 {% endif %}
)
select * from final