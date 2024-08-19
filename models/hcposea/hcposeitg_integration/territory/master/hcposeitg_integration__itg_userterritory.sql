{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                DELETE FROM {{this}} 
WHERE user_territory_source_id  IN(SELECT user_territory_source_id FROM {{source('hcposesdl_raw', 'sdl_hcp_osea_userterritory')}});
                    {% endif %}"
    )
}}
with sdl_hcp_osea_userterritory
as
(
select 
    USER_TERRITORY_SOURCE_ID,
	USER_TERRITORY_USER_SOURCE_ID,
	TERRITORY_SOURCE_ID,
	IS_ACTIVE,
	ROLE_IN_TERRITORY,
	LAST_MODIFIED_DATE,
	LAST_MODIFIED_BY_ID,
	CRT_DTTM,
	FILENAME,
	RUN_ID
 from {{ source('hcposesdl_raw', 'sdl_hcp_osea_userterritory') }}
)
,

transformed
as
(
select user_territory_source_id,
       user_territory_user_source_id,
       territory_source_id,
       (case when upper(is_active) = 'true' then 1 when upper(is_active) is null then 0 when upper(is_active) = ' ' then 0 else 0 end) as is_active,
	   role_in_territory,
       last_modified_date,
       last_modified_by_id,
       '' as country_code,
       sysdate() as inserted_date,
       sysdate() as updated_date
from sdl_hcp_osea_userterritory
),

final 
as
(select 
    user_territory_source_id::varchar(18) as user_territory_source_id,
	user_territory_user_source_id::varchar(18) as user_territory_user_source_id,
	territory_source_id::varchar(18) as territory_source_id,
	is_active::varchar(5) as is_active,
	role_in_territory::varchar(255) as role_in_territory,
	last_modified_date::timestamp_ntz(9) as last_modified_date,
	last_modified_by_id::varchar(18) as last_modified_by_id,
	country_code::varchar(10) as country_code,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
    from transformed 
)

select * from final 