{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where (visit_id, audit_form_id, section_id, coalesce(field_code,'#'), field_type, product_attribute_value_id) in (select distinct visit_id, audit_form_id, section_id, coalesce(field_code,'#'), field_type, product_attribute_value_id from {{ ref('aspitg_integration__itg_pop6_product_attribute_audits_temp') }}) or response in ('no', 'NO', 'null','NULL');
        {% endif %}"
    )
}}
with source as (
    select * from {{ ref('aspitg_integration__itg_pop6_product_attribute_audits_temp') }}
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
    from source
)
select * from final