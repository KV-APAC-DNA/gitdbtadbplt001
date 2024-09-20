with 
sdl_hcp_osea_isight_sector_mapping
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_isight_sector_mapping') }}
where filename not in (
            select distinct file_name from {{ source('hcposewks_integration', 'TRATBL_sdl_hcp_osea_isight_sector_mapping__null_test') }}
            union all
            select distinct file_name from {{ source('hcposewks_integration', 'TRATBL_sdl_hcp_osea_isight_sector_mapping__duplicate_test') }}
    )
)
,
transformed 
as
(
 select country ,
 company ,
 division,
 sector,
 current_timestamp() as inserted_date,
 current_timestamp() as updated_date,
 filename
 from sdl_hcp_osea_isight_sector_mapping
)
,
final as
(select  
    country:: varchar(256) as country,
	company:: varchar(256) as company,
	division:: varchar(256) as division,
	sector:: varchar(256) as sector,
	inserted_date:: timestamp_ntz(9) as inserted_date,
	updated_date:: timestamp_ntz(9) as updated_date,
	filename::varchar(255) as file_name
    from transformed 
)

select * from final 