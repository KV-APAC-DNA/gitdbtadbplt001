with 
sdl_hcp_osea_isight_sector_mapping
as
(
select * from dev_dna_load.hcposesdl_raw.sdl_hcp_osea_isight_sector_mapping
)
,
transformed 
as
(
 select country ,
 company ,
 division,
 sector,
 sysdate() as inserted_date,
 sysdate() as updated_date
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
	updated_date:: timestamp_ntz(9) as updated_date
    from transformed 
)

select * from final 