with dim_profile as (
    select * from dev_dna_core.hcposeedw_integration.dim_profile
),


result as (
    select *
    from dim_profile
    where lower(created_by_id) 
    not like '%_rg'
),

final as (
    select
    profile_key::varchar(32) as profile_key,
	country_code::varchar(2) as country_code,
	profile_source_id::varchar(18) as profile_source_id,
	modified_dt::timestamp_ntz(9) as modified_dt,
	modified_id::varchar(18) as modified_id,
	profile_name::varchar(255) as profile_name,
	function_name::varchar(255) as function_name,
	created_date::timestamp_ntz(9) as created_date,
	created_by_id::varchar(30) as created_by_id,
	type::varchar(43) as type,
	userlicense_source_id::varchar(18) as userlicense_source_id,
	usertype::varchar(43) as usertype,
	description::varchar(255) as description,
	current_timestamp()::timestamp_ntz(9) as inserted_date,
	current_timestamp()::timestamp_ntz(9) as updated_date
    from result
)

select * from final