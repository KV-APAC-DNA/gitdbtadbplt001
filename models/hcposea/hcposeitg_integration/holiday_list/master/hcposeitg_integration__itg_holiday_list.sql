with sdl_hcp_osea_holiday_list 
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_holiday_list') }}
where filename not in (
            select distinct file_name from {{ source('hcposewks_integration', 'TRATBL_sdl_hcp_osea_holiday_list__null_test') }}
            union all
            select distinct file_name from {{ source('hcposewks_integration', 'TRATBL_sdl_hcp_osea_holiday_list__duplicate_test') }}
    )
),

transformed 
as
(
   select 
country
,holiday_key
,current_timestamp() as inserted_date
,current_timestamp() as updated_date,
filename
from sdl_hcp_osea_holiday_list 
)
,
final as
(
select 
    country::varchar(5) as country,
	holiday_key::varchar(20) as holiday_key,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date,
    filename::varchar(255) as file_name
    from transformed 
)
 
select * from final 