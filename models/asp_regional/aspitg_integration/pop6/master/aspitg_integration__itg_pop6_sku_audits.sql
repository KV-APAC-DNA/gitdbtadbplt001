
{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}}
        where (visit_id, audit_form_id, section_id, field_id, sku_id)
        in (select distinct  visit_id, audit_form_id, section_id, field_id, sku_id from {{ ref('aspwks_integration__wks_pop6_sku_audits') }}) or response in ('no','NO','NULL','null');
            {% endif %}"
)}}
with itg_pop6_sku_audits_temp as (
    select * from {{ ref('aspwks_integration__wks_pop6_sku_audits') }}
        {% if is_incremental() %}
        where (file_name,cntry_cd) not in (select distinct file_name ,cntry_cd from {{this}})
    {% endif %}
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
sku_id::varchar(255) as sku_id,
sku::varchar(255) as sku,
response::varchar(65535) as response,
file_name::varchar(100) as file_name,
run_id::number(14,0) as run_id,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from itg_pop6_sku_audits_temp
)
select * from final
