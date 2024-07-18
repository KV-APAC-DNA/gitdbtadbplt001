with sdl_pop6_kr_sku_audits as (
    select * from {{ ref('aspwks_integration__wks_pop6_kr_sku_audits') }}
),
sdl_pop6_tw_sku_audits as (
    select * from {{ ref('aspwks_integration__wks_pop6_tw_sku_audits') }}
),
sdl_pop6_hk_sku_audits as (
    select * from {{ ref('aspwks_integration__wks_pop6_hk_sku_audits') }}
),
sdl_pop6_jp_sku_audits as (
    select * from {{ ref('aspwks_integration__wks_pop6_jp_sku_audits') }}
),
sdl_pop6_sg_sku_audits as (
    select * from {{ ref('aspwks_integration__wks_pop6_sg_sku_audits') }}
),
sdl_pop6_th_sku_audits as (
    select * from {{ ref('aspwks_integration__wks_pop6_th_sku_audits') }}
),
transformed as (
SELECT 
	'KR' as cntry_cd,
	substring(file_name, 1, 8) as src_file_date, 
	visit_id,
	audit_form_id,
	audit_form,
	section_id,
	section,
	field_id,
	field_code,
	field_label,
	field_type,
	dependent_on_field_id,
	sku_id,
	sku,
	response,
	file_name,
	null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9) as updt_dttm
FROM sdl_pop6_kr_sku_audits
union
SELECT 
	'TW',
	substring(file_name, 1, 8), 
	visit_id,
	audit_form_id,
	audit_form,
	section_id,
	section,
	field_id,
	field_code,
	field_label,
	field_type,
	dependent_on_field_id,
	sku_id,
	sku,
	response,
	file_name,
	null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9)
FROM sdl_pop6_tw_sku_audits
union	
SELECT 
	'HK',
	substring(file_name, 1, 8), 
	visit_id,
	audit_form_id,
	audit_form,
	section_id,
	section,
	field_id,
	field_code,
	field_label,
	field_type,
	dependent_on_field_id,
	sku_id,
	sku,
	response,
	file_name,
 null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9)
FROM sdl_pop6_hk_sku_audits
union

SELECT 
	'JP',
	substring(file_name, 1, 8), 
	visit_id,
	audit_form_id,
	audit_form,
	section_id,
	section,
	field_id,
	field_code,
	field_label,
	field_type,
	dependent_on_field_id,
	sku_id,
	sku,
	response,
	file_name,
	null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9)
FROM sdl_pop6_jp_sku_audits
union

SELECT 
	'SG',
	substring(file_name, 1, 8), 
	visit_id,
	audit_form_id,
	audit_form,
	section_id,
	section,
	field_id,
	field_code,
	field_label,
	field_type,
	dependent_on_field_id,
	sku_id,
	sku,
	response,
	file_name,
  null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9)
FROM sdl_pop6_sg_sku_audits
union
SELECT 
	'TH',
	substring(file_name, 1, 8), 
	visit_id,
	audit_form_id,
	audit_form,
	section_id,
	section,
	field_id,
	field_code,
	field_label,
	field_type,
	dependent_on_field_id,
	sku_id,
	sku,
	response,
	file_name,
  null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9)
FROM sdl_pop6_th_sku_audits
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
from transformed
)
select * from final 