with sdl_hcp_osea_holiday_list 
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_holiday_list') }}
),

transformed 
as
(
   select 
country
,holiday_key
,current_timestamp() as inserted_date
,current_timestamp() as updated_date 
from sdl_hcp_osea_holiday_list 
)
,
final as
(
select 
    country::varchar(5) as country,
	holiday_key::varchar(20) as holiday_key,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
    from transformed 
)
 
select * from final 