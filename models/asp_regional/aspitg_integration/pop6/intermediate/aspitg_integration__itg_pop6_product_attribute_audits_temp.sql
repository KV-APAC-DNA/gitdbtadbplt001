with sdl_pop6_kr_product_attribute_audits as (
    select * from --{{ source('ntasdl_raw', 'sdl_pop6_kr_product_attribute_audits') }}
    {{ref('aspwks_integration__wks_pop6_kr_product_attribute_audits')}}
),
sdl_pop6_tw_product_attribute_audits as (
    select * from --{{ source('ntasdl_raw', 'sdl_pop6_tw_product_attribute_audits') }}
    {{ref('aspwks_integration__wks_pop6_tw_product_attribute_audits')}}
),
sdl_pop6_hk_product_attribute_audits as (
    select * from --{{ source('ntasdl_raw', 'sdl_pop6_hk_product_attribute_audits') }}
    {{ref('aspwks_integration__wks_pop6_hk_product_attribute_audits')}}
),
sdl_pop6_jp_product_attribute_audits as (
    select * from --{{ source('jpnsdl_raw', 'sdl_pop6_jp_product_attribute_audits') }}
    {{ref('aspwks_integration__wks_pop6_jp_product_attribute_audits')}}
),
sdl_pop6_sg_product_attribute_audits as (
    select * from --{{ source('sgpsdl_raw', 'sdl_pop6_sg_product_attribute_audits') }}
    {{ref('aspwks_integration__wks_pop6_sg_product_attribute_audits')}}
),
sdl_pop6_th_product_attribute_audits as (
    select * from --{{ source('thasdl_raw', 'sdl_pop6_th_product_attribute_audits') }}
    {{ref('aspwks_integration__wks_pop6_th_product_attribute_audits')}}
),
transformed as (
    SELECT 
        'KR' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_kr_product_attribute_audits
    union
    SELECT 
        'TW' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_tw_product_attribute_audits
    union
    SELECT 
        'HK' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_hk_product_attribute_audits
    union
    SELECT 
        'JP' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_jp_product_attribute_audits
    union
    SELECT 
        'SG' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_sg_product_attribute_audits
    union
    SELECT 	'TH' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        audit_form_id as audit_form_id,
        audit_form as audit_form,
        section_id as section_id,
        section as section,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_th_product_attribute_audits
),
final as (
    select
        cntry_cd::varchar(10) as cntry_cd,
        src_file_date::varchar(10) as src_file_date,
        visit_id::varchar(255) as visit_id,
        audit_form_id::varchar(255) as audit_form_id,
        audit_form::varchar(255) as audit_form,
        section_id::varchar(255) as section_id,
        section::varchar(255) as section,
        field_id::varchar(255) as field_id,
        field_code::varchar(255) as field_code,
        field_label::varchar(255) as field_label,
        field_type::varchar(50) as field_type,
        dependent_on_field_id::varchar(255) as dependent_on_field_id,
        product_attribute_id::varchar(255) as product_attribute_id,
        product_attribute::varchar(200) as product_attribute,
        product_attribute_value_id::varchar(255) as product_attribute_value_id,
        product_attribute_value::varchar(200) as product_attribute_value,
        response::varchar(65535) as response,
        file_name::varchar(100) as file_name,
        run_id::numeric(14) as run_id,
        crtd_dttm::timestamp as crtd_dttm,
        updt_dttm::timestamp as updt_dttm
    from transformed
)
select * from final