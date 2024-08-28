with itg_profile
as
(
    select * from {{ ref('hcposeitg_integration__itg_profile') }}
)
,

transformed 
as
(
 select * from itg_profile where lower(created_by_id) not like '%_rg'
)
,
final
as
( select 
    PROFILE_KEY::VARCHAR(32) as PROFILE_KEY,
	PROFILE_SOURCE_ID::VARCHAR(18) as PROFILE_SOURCE_ID,
	PROFILE_NAME::VARCHAR(255) as PROFILE_NAME,
	TYPE::VARCHAR(40) as TYPE,
	USERLICENSE_SOURCE_ID::VARCHAR(18) as USERLICENSE_SOURCE_ID,
	USERTYPE::VARCHAR(40) as USERTYPE,
	CREATED_DATE::TIMESTAMP_NTZ(9) as CREATED_DATE,
	CREATED_BY_ID::VARCHAR(30) as CREATED_BY_ID,
	LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) as LAST_MODIFIED_DATE,
	LAST_MODIFIED_BY_ID::VARCHAR(18) as LAST_MODIFIED_BY_ID,
	DESCRIPTION::VARCHAR(255) as DESCRIPTION,
	COUNTRY_CODE::VARCHAR(255) as COUNTRY_CODE,
	INSERTED_DATE::TIMESTAMP_NTZ(9) as INSERTED_DATE,
	UPDATED_DATE::TIMESTAMP_NTZ(9) as UPDATED_DATE
    from transformed 
)
select * from final 
        