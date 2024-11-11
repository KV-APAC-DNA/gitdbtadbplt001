
with sdl_hcp_osea_group_member
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_group_member') }}
)
,
transformed as
(
    select
    GROUP_MEMBER_ID,
GROUP_ID,
USER_GROUP_ID,
SYSTEMMODSTAMP,
current_timestamp() as INSERTED_DATE,
       NULL as UPDATED_DATE 
from sdl_hcp_osea_group_member
)
,
final as
(select 
	GROUP_MEMBER_ID::VARCHAR(18) AS GROUP_MEMBER_ID,
GROUP_ID::VARCHAR(18) AS GROUP_ID,
USER_GROUP_ID::VARCHAR(18) AS USER_GROUP_ID,
SYSTEMMODSTAMP::TIMESTAMP_NTZ(9) AS SYSTEMMODSTAMP,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
	from transformed
)

select * from final 
