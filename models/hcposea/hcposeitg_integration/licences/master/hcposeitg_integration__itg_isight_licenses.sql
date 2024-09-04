with sdl_hcp_osea_isight_licenses
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_isight_licenses') }}
)
,
transformed
as
(
 select year ,
 country  ,
 sector ,
 qty,
 licensetype,
 current_timestamp() as inserted_date,
 current_timestamp() as updated_date
 from sdl_hcp_osea_isight_licenses
)
,
final 
as
(select 
    year:: number(18,0) as year,
	country:: varchar(255) as country,
	sector:: varchar(255) as sector,
	qty:: number(18,0) as qty,
	licensetype:: varchar(255) as licensetype,
	inserted_date:: timestamp_ntz(9) as inserted_date,
	updated_date:: timestamp_ntz(9) as updated_date
    from 
    transformed 
)

select * from final 



 


