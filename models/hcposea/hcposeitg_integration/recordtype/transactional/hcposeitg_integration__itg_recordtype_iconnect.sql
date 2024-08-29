 with itg_recordtype
 as
 (
 select * from {{ ref('hcposeitg_integration__itg_recordtype') }}
 )

,
transformed as 
(
   select * from itg_recordtype where lower(created_by_id) not like '%_rg'
)
,
final as
(select 
    record_type_source_id::varchar(18) as record_type_source_id,
	record_type_name::varchar(255) as record_type_name,
	developer_name::varchar(80) as developer_name,
	name_space_prefix::varchar(15) as name_space_prefix,
	description::varchar(255) as description,
	business_process_id::varchar(18) as business_process_id,
	sobjecttype::varchar(40) as sobjecttype,
	is_active::varchar(5) as is_active,
	is_person_type::varchar(5) as is_person_type,
	created_by_id::varchar(30) as created_by_id,
	created_date::timestamp_ntz(9) as created_date,
	last_modified_by_id::varchar(18) as last_modified_by_id,
	last_modified_date::timestamp_ntz(9) as last_modified_date,
	country_code::varchar(255) as country_code,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
    from transformed
)

select * from final 
